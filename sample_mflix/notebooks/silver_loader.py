# Databricks notebook source
# MAGIC %md
# MAGIC # silver_loader — Bronze -> Silver
# MAGIC
# MAGIC Quebra a coluna `json` da Bronze em colunas tipadas. Mesma estrutura do
# MAGIC `bronze_loader`: uma coleção por execução, parâmetros vindos de
# MAGIC `config/`, log na mesma tabela de controle.
# MAGIC
# MAGIC A leitura da Bronze é incremental por `_ingestion_timestamp`: só entram
# MAGIC as linhas gravadas depois da última execução bem-sucedida da Silver.
# MAGIC Coleção sem linha nova sai com 0 registros, sem escrever nada.

# COMMAND ----------

dbutils.widgets.text("collection", "movies")
dbutils.widgets.text("table_prefix", "dev_")
dbutils.widgets.text("bundle_root", "")

collection = dbutils.widgets.get("collection")
table_prefix = dbutils.widgets.get("table_prefix")

# COMMAND ----------

import json
import math
import os
import sys
import time
from datetime import datetime

import yaml
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

bundle_root = dbutils.widgets.get("bundle_root") or os.path.dirname(os.getcwd())
sys.path.append(f"{bundle_root}/notebooks")

from control import get_ingestion_id, get_last_watermark, log_execution, reconcile

with open(f"{bundle_root}/config/pipeline_config.yaml") as f:
    CFG = yaml.safe_load(f)

with open(f"{bundle_root}/config/collections.json") as f:
    COLLECTIONS = json.load(f)

BRONZE_SCHEMA = CFG["bronze_schema"]
SILVER_SCHEMA = CFG["silver_schema"]
ROWS_PER_FILE = CFG["write"]["target_rows_per_file"]
LIMIAR = CFG["reconcile"]["limiar_divergencia"]
TENTATIVAS = CFG["retry"]["tentativas"]
BACKOFF = CFG["retry"]["backoff_inicial_seg"]
SAMPLE_SIZE = CFG["extract"]["sample_size"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

# COMMAND ----------

def com_retry(fn):
    for tentativa in range(TENTATIVAS):
        try:
            return fn()
        except Exception:
            if tentativa == TENTATIVAS - 1:
                raise
            time.sleep(BACKOFF * 2 ** tentativa)

# COMMAND ----------
# MAGIC %md ## Tipagem e achatamento
# MAGIC
# MAGIC O schema sai de `schema_of_json_agg` sobre o próprio lote — nenhuma
# MAGIC coluna é declarada à mão, então campo novo na origem aparece sozinho.
# MAGIC Structs aninhados viram colunas com `_` (`imdb.rating` -> `imdb_rating`);
# MAGIC arrays ficam intactos aqui e viram tabelas satélite depois.

# COMMAND ----------

def parse_json(df):
    ddl = df.select(F.expr("schema_of_json_agg(json)")).first()[0]
    if not ddl or not ddl.upper().startswith("STRUCT<"):
        return None
    return df.select("id", "_ingestion_timestamp", F.from_json("json", ddl).alias("doc")) \
             .select("id", "_ingestion_timestamp", "doc.*")

def expandir_objetos(df, sample_size):
    # A landing grava tudo como string, então campo aninhado vira o texto
    # '{"rating":7.3}'. Aqui cada coluna string que carrega um objeto JSON
    # volta a ser struct, para o flatten conseguir achatar em seguida.
    amostra = df.limit(sample_size)
    for campo, tipo in df.dtypes:
        if tipo != "string" or campo in ("id", "_ingestion_timestamp"):
            continue
        try:
            ddl = amostra.select(F.expr(f"schema_of_json_agg(`{campo}`)")).first()[0]
        except Exception:
            continue
        # _corrupt_record no schema = a coluna é texto comum, não objeto JSON
        if ddl and ddl.upper().startswith("STRUCT<") and "_corrupt_record" not in ddl:
            df = df.withColumn(campo, F.from_json(F.col(f"`{campo}`"), ddl))
    return df

def flatten(df):
    while True:
        structs = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        if not structs:
            return df
        cols = []
        for f in df.schema.fields:
            if f.name in structs:
                cols += [F.col(f"`{f.name}`.`{c.name}`").alias(f"{f.name}_{c.name}")
                         for c in f.dataType.fields]
            else:
                cols.append(F.col(f"`{f.name}`"))
        df = df.select(*cols)

def dedup_latest(df):
    ordem = Window.partitionBy("id").orderBy(F.col("_ingestion_timestamp").desc())
    return df.withColumn("_rn", F.row_number().over(ordem)).filter("_rn = 1").drop("_rn")

# COMMAND ----------
# MAGIC %md ## Escrita

# COMMAND ----------

def merge_por_id(df, target_table, qtd, chaves):
    df = df.repartition(max(1, math.ceil(max(qtd, 1) / ROWS_PER_FILE)))
    if not spark.catalog.tableExists(target_table):
        com_retry(lambda: df.write.format("delta").saveAsTable(target_table))
        return
    cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in chaves)
    tbl = DeltaTable.forName(spark, target_table)
    com_retry(lambda: tbl.alias("t").merge(df.alias("s"), cond)
              .whenMatchedUpdateAll()
              .whenNotMatchedInsertAll()
              .withSchemaEvolution()
              .execute())

def como_array(df, campo):
    # A landing grava tudo como string, então array vira o texto '["a","b"]'.
    # Aqui ele volta a ser array antes do explode.
    tipo = dict(df.dtypes)[campo]
    if tipo.startswith("array"):
        return F.col(f"`{campo}`")
    return F.from_json(F.col(f"`{campo}`"), "array<string>")

def explodir(df, collection, campo, table_prefix):
    if campo not in df.columns:
        return
    sat = (
        df.select("id", F.explode_outer(como_array(df, campo)).alias(campo), "_silver_timestamp")
          .filter(F.col(campo).isNotNull())
          .dropDuplicates(["id", campo])
    )
    alvo = f"{SILVER_SCHEMA}.{table_prefix}{collection}_{campo}"
    merge_por_id(sat, alvo, sat.count(), ["id", campo])

# COMMAND ----------
# MAGIC %md ## Função principal

# COMMAND ----------

def load_to_silver(collection, table_prefix):
    cfg = COLLECTIONS[collection]
    explode_fields = cfg.get("explode_fields", [])

    origem = f"{BRONZE_SCHEMA}.{table_prefix}{collection}"
    destino = f"{SILVER_SCHEMA}.{table_prefix}{collection}"
    chave_log = f"silver_{collection}"

    start_time = datetime.now()
    ingestion_id = get_ingestion_id()
    watermark_inicial = get_last_watermark(chave_log)
    status, error_message = "SUCCESS", None
    qtd_lida_origem = qtd_gravada_destino = 0
    watermark_final = watermark_inicial
    pct_nulos = qtd_duplicados = None

    try:
        df = com_retry(lambda: spark.table(origem))
        if watermark_inicial:
            df = df.filter(F.col("_ingestion_timestamp") > F.lit(watermark_inicial).cast("timestamp"))

        qtd_lida_origem = df.count()

        if qtd_lida_origem > 0:
            watermark_final = str(df.agg(F.max("_ingestion_timestamp")).first()[0])

            tipado = parse_json(df)
            if tipado is None:
                raise Exception(f"schema_of_json_agg não devolveu STRUCT para {collection}")

            silver = (
                dedup_latest(flatten(expandir_objetos(tipado, SAMPLE_SIZE)))
                .withColumn("_silver_timestamp", F.current_timestamp())
                .withColumn("_ingestion_id", F.lit(ingestion_id))
            )

            arrays = [c for c in explode_fields if c in silver.columns]
            qtd_gravada_destino = silver.count()
            merge_por_id(silver.drop(*arrays), destino, qtd_gravada_destino, ["id"])

            for campo in arrays:
                explodir(silver, collection, campo, table_prefix)

            status, error_message, pct_nulos, qtd_duplicados = reconcile(
                silver, qtd_lida_origem, qtd_gravada_destino, LIMIAR
            )

    except Exception as e:
        status, error_message = "FAILED", str(e)[:500]

    end_time = datetime.now()
    log_execution(
        collection=chave_log,
        load_type="incremental",
        watermark_inicial=watermark_inicial,
        watermark_final=watermark_final,
        qtd_lida_origem=qtd_lida_origem,
        qtd_gravada_destino=qtd_gravada_destino,
        start_time=start_time,
        end_time=end_time,
        status=status,
        pct_nulos_source_id=pct_nulos,
        qtd_duplicados_source_id=qtd_duplicados,
        error_message=error_message,
        _ingestion_id=ingestion_id,
    )
    print(chave_log, status, "lido:", qtd_lida_origem, "gravado:", qtd_gravada_destino,
          "tabela:", destino, error_message or "")
    return status

# COMMAND ----------

status = load_to_silver(collection, table_prefix)
dbutils.notebook.exit(status)

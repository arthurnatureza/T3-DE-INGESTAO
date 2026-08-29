# Databricks notebook source
# MAGIC %md
# MAGIC # bronze_loader — Landing -> Bronze
# MAGIC
# MAGIC Uma coleção por execução. Todas as tasks do Job chamam este mesmo
# MAGIC notebook, mudando só os parâmetros — não há código duplicado por
# MAGIC coleção (R1). Os parâmetros de cada coleção vêm de
# MAGIC `config/collections.json`; os globais, de `config/pipeline_config.yaml`.
# MAGIC
# MAGIC A Bronze guarda o dado como veio (R6): `id` + `json` com o documento
# MAGIC completo + colunas de controle. Achatamento em colunas de negócio é
# MAGIC responsabilidade da Silver.

# COMMAND ----------

dbutils.widgets.text("collection", "users")
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
from pyspark.sql import functions as F

bundle_root = dbutils.widgets.get("bundle_root") or os.path.dirname(os.getcwd())
sys.path.append(f"{bundle_root}/notebooks")

from control import get_ingestion_id, get_last_watermark, log_execution, reconcile

with open(f"{bundle_root}/config/pipeline_config.yaml") as f:
    CFG = yaml.safe_load(f)

with open(f"{bundle_root}/config/collections.json") as f:
    COLLECTIONS = json.load(f)

LANDING_VOLUME = CFG["landing_volume"]
BRONZE_SCHEMA = CFG["bronze_schema"]
ROWS_PER_FILE = CFG["write"]["target_rows_per_file"]
LIMIAR = CFG["reconcile"]["limiar_divergencia"]
TENTATIVAS = CFG["retry"]["tentativas"]
BACKOFF = CFG["retry"]["backoff_inicial_seg"]

# COMMAND ----------
# MAGIC %md ## Retry com backoff (R2)
# MAGIC
# MAGIC Leitura do Volume e escrita Delta batem em storage de objeto, que
# MAGIC falha de forma transitória. Toda operação de I/O passa por aqui.

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
# MAGIC %md ## Leitura da landing
# MAGIC
# MAGIC Sempre a pasta `exec=` mais recente. Limitação documentada em
# MAGIC `docs/ARQUITETURA.md`: cada `exec=` é um snapshot completo, então
# MAGIC pular pastas intermediárias não perde dado novo.

# COMMAND ----------

def latest_landing_path(collection):
    base = f"{LANDING_VOLUME}/{collection}"
    execs = [f.path for f in dbutils.fs.ls(base) if f.name.startswith("exec=")]
    return sorted(execs)[-1].rstrip("/") if execs else base

def read_landing(collection):
    path = latest_landing_path(collection)
    return com_retry(lambda: spark.read.option("mergeSchema", "true").parquet(path))

# COMMAND ----------
# MAGIC %md ## Quarentena (R7)
# MAGIC
# MAGIC Nada é descartado em silêncio: documento sem `id` utilizável ou lote
# MAGIC que falhou na escrita vai pra cá com o JSON original e o motivo.

# COMMAND ----------

def ensure_quarantine_table(quarantine_table):
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {quarantine_table} (
      id string,
      collection string,
      raw_json string,
      motivo string,
      _ingestion_id string,
      _ingestion_timestamp timestamp
    ) using delta
    """)

def rescue(df, collection, motivo, quarantine_table):
    (
        df.select("id", F.col("json").alias("raw_json"), "_ingestion_id", "_ingestion_timestamp")
        .withColumn("collection", F.lit(collection))
        .withColumn("motivo", F.lit(motivo[:300]))
        .select("id", "collection", "raw_json", "motivo", "_ingestion_id", "_ingestion_timestamp")
        .write.mode("append").saveAsTable(quarantine_table)
    )

# COMMAND ----------
# MAGIC %md ## Escrita na Bronze
# MAGIC
# MAGIC MERGE por `id` (idempotente, R3). O `repartition` dimensiona o número
# MAGIC de arquivos pelo volume do lote, pra não gerar small files (R2).

# COMMAND ----------

def write_bronze(df, target_table, qtd):
    df = df.repartition(max(1, math.ceil(qtd / ROWS_PER_FILE)))
    if not spark.catalog.tableExists(target_table):
        com_retry(lambda: df.write.format("delta")
                  .partitionBy("_ingestion_date").saveAsTable(target_table))
    else:
        tbl = DeltaTable.forName(spark, target_table)
        com_retry(lambda: tbl.alias("t")
                  .merge(df.alias("s"), "t.id = s.id")
                  .whenMatchedUpdateAll()
                  .whenNotMatchedInsertAll()
                  .execute())
    return qtd

# COMMAND ----------
# MAGIC %md ## Função principal

# COMMAND ----------

def load_to_bronze(collection, table_prefix):
    cfg = COLLECTIONS[collection]
    load_type = cfg["load_type"]
    watermark_field = cfg.get("watermark_field")

    target_table = f"{BRONZE_SCHEMA}.{table_prefix}{collection}"
    quarantine_table = f"{BRONZE_SCHEMA}.{table_prefix}{CFG['quarantine_table']}"
    ensure_quarantine_table(quarantine_table)

    start_time = datetime.now()
    ingestion_id = get_ingestion_id()
    watermark_inicial = get_last_watermark(collection) if load_type == "incremental" else None
    status, error_message = "SUCCESS", None
    qtd_lida_origem = qtd_gravada_destino = 0
    watermark_final = watermark_inicial
    pct_nulos = qtd_duplicados = None

    try:
        df = read_landing(collection)
        if load_type == "incremental" and watermark_inicial:
            df = df.filter(F.col(watermark_field) > watermark_inicial)
        qtd_lida_origem = df.count()

        df_bronze = (
            df.withColumn("id", F.col("_id").cast("string"))
              .withColumn("json", F.to_json(F.struct(*df.columns)))
              .withColumn("_ingestion_id", F.lit(ingestion_id))
              .withColumn("_ingestion_timestamp", F.current_timestamp())
              .withColumn("_source_path", F.lit(f"mongodb://{CFG['database']}.{collection}"))
              .withColumn("_load_type", F.lit(load_type))
              .withColumn("_ingestion_date", F.current_date())
              .select("id", "json", "_ingestion_id", "_ingestion_timestamp",
                      "_source_path", "_load_type", "_ingestion_date")
        )

        df_sem_id = df_bronze.filter(F.col("id").isNull())
        df_valido = df_bronze.filter(F.col("id").isNotNull())
        qtd_valido = df_valido.count()

        if qtd_lida_origem - qtd_valido > 0:
            rescue(df_sem_id, collection, "id nulo (_id ausente no documento)", quarantine_table)

        try:
            qtd_gravada_destino = write_bronze(df_valido, target_table, qtd_valido)
        except Exception as e:
            rescue(df_valido, collection, str(e), quarantine_table)
            status = "PARTIAL"
            error_message = f"falha na escrita, lote preservado na quarentena: {str(e)[:300]}"

        if load_type == "incremental" and watermark_field and qtd_lida_origem > 0:
            watermark_final = str(df.agg(F.max(watermark_field)).first()[0])

        if status == "SUCCESS":
            status, error_message, pct_nulos, qtd_duplicados = reconcile(
                df_bronze, qtd_lida_origem, qtd_gravada_destino, LIMIAR
            )

    except Exception as e:
        status, error_message = "FAILED", str(e)[:500]

    end_time = datetime.now()
    log_execution(
        collection=collection,
        load_type=load_type,
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
    print(collection, status, "lido:", qtd_lida_origem, "gravado:", qtd_gravada_destino,
          "watermark_final:", watermark_final, "tabela:", target_table, error_message or "")
    return status

# COMMAND ----------

status = load_to_bronze(collection, table_prefix)
dbutils.notebook.exit(status)

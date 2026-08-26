# Databricks notebook source
# MAGIC %md
# MAGIC # bronze_loader — Landing -> Bronze
# MAGIC
# MAGIC Uma coleção por execução, parametrizado via widgets. Cada task de um
# MAGIC Job chama este notebook com parâmetros diferentes — mesmo código,
# MAGIC sem duplicar por tabela (R1). `table_prefix` controla se grava em
# MAGIC tabela `dev_<collection>` (ambiente de desenvolvimento) ou
# MAGIC `<collection>` (produção) — mesmo notebook, dois ambientes.

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "sample_mflix"
LANDING_VOLUME = f"/Volumes/{CATALOG}/landing/mongo"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
CONTROL_TABLE = f"{BRONZE_SCHEMA}.control_ingestion_log"  # tabela real da Carina, sempre a mesma

# COMMAND ----------
# MAGIC %md ## Controle
# MAGIC
# MAGIC Usa a tabela real da Carina (`control_ingestion_log`), não cria nada —
# MAGIC ela já existe. Mesma assinatura das funções dela
# MAGIC (`get_ingestion_id`, `get_last_watermark`, `log_execution`), mas
# MAGIC escritas aqui pra eu não editar o notebook dela. Ela tem 2 colunas a
# MAGIC mais que o mínimo do R5: `pct_nulos_source_id` e
# MAGIC `qtd_duplicados_source_id` (métricas de R8) — como já tenho o lote em
# MAGIC mãos na hora de gravar, calculo os dois aqui (métrica simples do lote;
# MAGIC a decisão de threshold/status por divergência continua sendo dela).

# COMMAND ----------

def get_ingestion_id() -> str:
    return str(uuid.uuid4())

def get_last_watermark(collection: str):
    row = (
        spark.table(CONTROL_TABLE)
        .filter((F.col("collection") == collection) & (F.col("status") == "SUCCESS"))
        .orderBy(F.col("end_time").desc())
        .limit(1)
        .select("watermark_final")
        .first()
    )
    return row["watermark_final"] if row else None

def log_execution(collection, load_type, watermark_inicial, watermark_final,
                   qtd_lida_origem, qtd_gravada_destino, start_time, end_time,
                   status, error_message=None, _ingestion_id=None,
                   pct_nulos_source_id=None, qtd_duplicados_source_id=None):
    _ingestion_id = _ingestion_id or get_ingestion_id()
    duracao_seg = int((end_time - start_time).total_seconds())
    row = [(_ingestion_id, collection, load_type, watermark_inicial, watermark_final,
            qtd_lida_origem, qtd_gravada_destino, start_time, end_time, duracao_seg,
            status, error_message, pct_nulos_source_id, qtd_duplicados_source_id)]
    spark.createDataFrame(row, schema=spark.table(CONTROL_TABLE).schema) \
        .write.mode("append").saveAsTable(CONTROL_TABLE)
    return _ingestion_id

def source_id_metrics(df_bronze):
    total = df_bronze.count()
    if total == 0:
        return None, 0
    nulos = df_bronze.filter(F.col("id").isNull()).count()
    distintos = df_bronze.select("id").distinct().count()
    pct_nulos = round(nulos / total * 100, 4)
    qtd_duplicados = total - distintos
    return pct_nulos, qtd_duplicados

# COMMAND ----------
# MAGIC %md ## Leitura da landing (sempre a partir da última pasta exec=)
# MAGIC
# MAGIC Limitação conhecida (documentada em docs/ARQUITETURA.md): só lê a
# MAGIC pasta mais recente, não as intermediárias não consumidas. Seguro na
# MAGIC prática porque cada exec= é um snapshot completo da coleção, exceto
# MAGIC pro caso de documento apagado do Mongo entre duas extrações.

# COMMAND ----------

def latest_landing_path(collection: str) -> str:
    base = f"{LANDING_VOLUME}/{collection}"
    execs = [f.path for f in dbutils.fs.ls(base) if f.name.startswith("exec=")]
    return sorted(execs)[-1].rstrip("/") if execs else base

def read_landing(collection: str):
    return spark.read.option("mergeSchema", "true").parquet(latest_landing_path(collection))

# COMMAND ----------
# MAGIC %md ## R7 — quarentena (registros que não puderem ser gravados na Bronze)
# MAGIC
# MAGIC Guarda o documento inteiro como JSON, nunca descarta silenciosamente.

# COMMAND ----------

def ensure_quarantine_table(quarantine_table: str):
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

def rescue(df_bronze, collection: str, motivo: str, quarantine_table: str):
    (
        df_bronze
        .select(
            "id",
            F.to_json(F.struct(*[c for c in df_bronze.columns if c != "id"])).alias("raw_json"),
            "_ingestion_id", "_ingestion_timestamp",
        )
        .withColumn("collection", F.lit(collection))
        .withColumn("motivo", F.lit(motivo[:300]))
        .select("id", "collection", "raw_json", "motivo", "_ingestion_id", "_ingestion_timestamp")
        .write.mode("append").saveAsTable(quarantine_table)
    )

# COMMAND ----------
# MAGIC %md ## Escrita na Bronze (MERGE por id, idempotente, com schema evolution)

# COMMAND ----------

def write_bronze(df_bronze, target_table: str) -> int:
    if not spark.catalog.tableExists(target_table):
        df_bronze.write.format("delta").option("mergeSchema", "true") \
            .partitionBy("_ingestion_date").saveAsTable(target_table)
        return df_bronze.count()

    delta_tbl = DeltaTable.forName(spark, target_table)
    (delta_tbl.alias("t")
        .merge(df_bronze.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .withSchemaEvolution()
        .execute())
    return df_bronze.count()

# COMMAND ----------
# MAGIC %md ## Função principal

# COMMAND ----------

def load_to_bronze(collection: str, load_type: str, watermark_field: str = None,
                    table_prefix: str = "") -> str:
    target_table = f"{BRONZE_SCHEMA}.{table_prefix}{collection}"
    quarantine_table = f"{BRONZE_SCHEMA}.{table_prefix}quarentena"
    ensure_quarantine_table(quarantine_table)

    start_time = datetime.now()
    ingestion_id = get_ingestion_id()
    watermark_inicial = get_last_watermark(collection) if load_type == "incremental" else None
    status, error_message = "SUCCESS", None
    qtd_lida_origem = qtd_gravada_destino = 0
    watermark_final = watermark_inicial
    pct_nulos_source_id = qtd_duplicados_source_id = None

    try:
        df = read_landing(collection)

        if load_type == "incremental" and watermark_inicial:
            df = df.filter(F.col(watermark_field) > watermark_inicial)

        qtd_lida_origem = df.count()

        df_bronze = (
            df.withColumn("id", F.col("_id").cast("string"))
              .withColumn("_ingestion_id", F.lit(ingestion_id))
              .withColumn("_ingestion_timestamp", F.current_timestamp())
              .withColumn("_source_path", F.lit(f"mongodb://{CATALOG}.{collection}"))
              .withColumn("_load_type", F.lit(load_type))
              .withColumn("_ingestion_date", F.current_date())
        )
        pct_nulos_source_id, qtd_duplicados_source_id = source_id_metrics(df_bronze)

        try:
            qtd_gravada_destino = write_bronze(df_bronze, target_table)
        except Exception as write_error:
            rescue(df_bronze, collection, str(write_error), quarantine_table)
            status = "PARTIAL"
            error_message = f"Schema incompatível na Bronze, registros preservados em quarentena: {str(write_error)[:300]}"

        if load_type == "incremental" and watermark_field and qtd_lida_origem > 0:
            watermark_final = str(df.agg(F.max(watermark_field)).first()[0])

    except Exception as e:
        status, error_message = "FAILED", str(e)[:500]

    end_time = datetime.now()
    log_execution(
        collection=collection, load_type=load_type,
        watermark_inicial=watermark_inicial, watermark_final=watermark_final,
        qtd_lida_origem=qtd_lida_origem, qtd_gravada_destino=qtd_gravada_destino,
        start_time=start_time, end_time=end_time,
        status=status, error_message=error_message, _ingestion_id=ingestion_id,
        pct_nulos_source_id=pct_nulos_source_id, qtd_duplicados_source_id=qtd_duplicados_source_id,
    )
    print(collection, status, "lido:", qtd_lida_origem, "gravado:", qtd_gravada_destino,
          "watermark_final:", watermark_final, "tabela:", target_table, error_message or "")
    return status

# COMMAND ----------
# MAGIC %md ## Execução (parametrizada por widgets)

# COMMAND ----------

dbutils.widgets.text("collection", "users")
dbutils.widgets.text("load_type", "full")
dbutils.widgets.text("watermark_field", "")
dbutils.widgets.text("table_prefix", "dev_")

collection = dbutils.widgets.get("collection")
load_type = dbutils.widgets.get("load_type")
watermark_field = dbutils.widgets.get("watermark_field") or None
table_prefix = dbutils.widgets.get("table_prefix")

status = load_to_bronze(collection, load_type=load_type, watermark_field=watermark_field,
                         table_prefix=table_prefix)
dbutils.notebook.exit(status)

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, DoubleType, TimestampType, IntegerType, StructField, StructType
import uuid

TABLE_LOG = "sample_mflix.bronze.control_ingestion_log"

def get_ingestion_id() -> str:
    return str(uuid.uuid4())

def get_last_watermark(collection: str):
    df = spark.table(TABLE_LOG) \
        .select(F.col("watermark_final")) \
        .filter((F.col("collection") == collection) & (F.col("status") == "SUCCESS")) \
        .orderBy(F.col("end_time").desc()) \
        .limit(1)

    return df.first()["watermark_final"] if df.count() > 0 else None

def log_execution(collection, load_type, watermark_inicial, watermark_final, qtd_lida_origem, qtd_gravada_destino, start_time, end_time, status, pct_nulos_source_id, qtd_duplicados_source_id, error_message = None, _ingestion_id = None):
    _ingestion_id = _ingestion_id or get_ingestion_id()
    
    SCHEMA = StructType([
        StructField("_ingestion_id", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("load_type", StringType(), True),
        StructField("watermark_inicial", StringType(), True),
        StructField("watermark_final", StringType(), True),
        StructField("qtd_lida_origem", LongType(), True),
        StructField("qtd_gravada_destino", LongType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duracao_seg", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("pct_nulos_source_id", DoubleType(), True),
        StructField("qtd_duplicados_source_id", LongType(), True),
        StructField("error_message", StringType(), True),
    ])
    duracao_seg = int ((end_time - start_time).total_seconds())

    row = [(_ingestion_id, collection, load_type, watermark_inicial, watermark_final, qtd_lida_origem, qtd_gravada_destino, start_time, end_time, duracao_seg, status, pct_nulos_source_id, qtd_duplicados_source_id, error_message)]

    df = spark.createDataFrame(row, schema=SCHEMA)
    df.write.mode("append").saveAsTable(TABLE_LOG)
    return _ingestion_id

def reconcile(df_lote, qtd_lida_origem, qtd_gravada_destino, limiar=0.05):
    qtd_nulos = df_lote.filter(F.col("id").isNull()).count()
    pct_nulos = qtd_nulos / qtd_lida_origem if qtd_lida_origem else 0.0

    qtd_duplicados = (df_lote.groupBy("id").count()
                       .filter("count > 1").count())

    divergencia = abs(qtd_lida_origem - qtd_gravada_destino) / qtd_lida_origem if qtd_lida_origem else 0.0
    status = "SUCCESS"
    mensagem = None
    if divergencia > limiar or pct_nulos > 0 or qtd_duplicados > 0:
        status = "PARTIAL"
        mensagem = f"divergência={divergencia:.2%}, nulos={pct_nulos:.2%}, duplicados={qtd_duplicados}"

    return status, mensagem, pct_nulos, qtd_duplicados
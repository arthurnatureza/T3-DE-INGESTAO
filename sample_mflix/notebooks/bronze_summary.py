# Databricks notebook source
# MAGIC %md
# MAGIC # bronze_summary — fechamento da execução
# MAGIC
# MAGIC Roda depois de todas as coleções. Mostra o resultado consolidado da
# MAGIC execução (é esta saída que vai pra `docs/evidencias/`) e falha o Job
# MAGIC se alguma coleção terminou em FAILED.

# COMMAND ----------

import os
import sys

dbutils.widgets.text("bundle_root", "")
bundle_root = dbutils.widgets.get("bundle_root") or os.path.dirname(os.getcwd())
sys.path.append(f"{bundle_root}/notebooks")

from control import TABLE_LOG

# COMMAND ----------

ultimas = spark.sql(f"""
    WITH ult AS (
      SELECT *, row_number() OVER (PARTITION BY collection ORDER BY start_time DESC) rn
      FROM {TABLE_LOG}
    )
    SELECT collection, load_type, watermark_inicial, watermark_final,
           qtd_lida_origem, qtd_gravada_destino, duracao_seg, status,
           pct_nulos_source_id, qtd_duplicados_source_id, error_message
    FROM ult WHERE rn = 1 ORDER BY collection
""")
ultimas.show(truncate=False)

falhas = ultimas.filter("status = 'FAILED'").count()
parciais = ultimas.filter("status = 'PARTIAL'").count()
print("coleções:", ultimas.count(), "| FAILED:", falhas, "| PARTIAL:", parciais)

if falhas:
    raise Exception(f"{falhas} coleção(ões) terminaram em FAILED — ver control_ingestion_log")

dbutils.notebook.exit("PARTIAL" if parciais else "SUCCESS")

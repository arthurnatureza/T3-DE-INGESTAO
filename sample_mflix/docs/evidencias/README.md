# Evidências de execução

O `SEND_WORK.md` exige três execuções distintas, comprovadas por print ou
saída de query sobre a `control_ingestion_log`. Salvar cada uma aqui com o
nome indicado.

| Arquivo | O que comprova | Situação |
|---|---|---|
| `execucao_01_full_load.png` | carga inicial das 6 coleções | ✅ já registrada no log |
| `execucao_02_incremental_sem_novidades.png` | incremental sem dado novo, `qtd_lida_origem = 0` | ✅ já registrada no log |
| `execucao_03_incremental_com_dados.png` | incremental trazendo só os registros novos | ⏳ pendente (ver abaixo) |

---

## Query padrão

Rodar no SQL Editor do Databricks e printar o resultado. É a mesma saída que
a task `resumo` do Job imprime ao final de cada execução.

```sql
WITH ult AS (
  SELECT *, row_number() OVER (PARTITION BY collection ORDER BY start_time DESC) rn
  FROM sample_mflix.bronze.control_ingestion_log
)
SELECT collection, load_type, watermark_inicial, watermark_final,
       qtd_lida_origem, qtd_gravada_destino, duracao_seg, status,
       pct_nulos_source_id, qtd_duplicados_source_id
FROM ult WHERE rn = 1
ORDER BY collection;
```

Para ver o histórico completo (as três execuções lado a lado):

```sql
SELECT collection, load_type, watermark_inicial, watermark_final,
       qtd_lida_origem, qtd_gravada_destino, status, start_time
FROM sample_mflix.bronze.control_ingestion_log
ORDER BY start_time DESC;
```

---

## Execução 1 — carga full inicial

Já ocorreu. As 6 coleções foram carregadas com as contagens batendo com a
origem:

| Coleção | Origem | Bronze |
|---|---|---|
| `movies` | 23.539 | 23.539 |
| `comments` | 50.315 | 50.315 |
| `users` | 185 | 185 |
| `theaters` | 1.564 | 1.564 |
| `sessions` | 1 | 1 |
| `embedded_movies` | 1.500 | 1.500 |

Para reproduzir do zero (opcional, só se quiser um print "limpo"): dropar as
tabelas Bronze e as linhas do log daquela coleção e rodar
`databricks bundle run bronze_pipeline -t dev`.

Print sugerido: a saída da query padrão **mais** o grafo do Job com as 7
tasks verdes.

## Execução 2 — incremental sem novidades

Já ocorreu. Basta rodar a pipeline uma segunda vez sem mexer na landing:
`movies` e `comments` (as duas incrementais) leem `qtd_lida_origem = 0`,
mantêm `watermark_final = watermark_inicial` e terminam `SUCCESS`.

Print sugerido: a query padrão logo depois da segunda execução, destacando
`qtd_lida_origem = 0` nas linhas incrementais.

Vale printar junto a prova de idempotência:

```sql
SELECT 'comments' t, count(*) linhas, count(DISTINCT id) ids
FROM sample_mflix.bronze.dev_comments
UNION ALL SELECT 'movies', count(*), count(DISTINCT id)
FROM sample_mflix.bronze.dev_movies;
```

`linhas = ids` comprova que rodar duas vezes não duplicou nada.

## Execução 3 — incremental com dados novos

**Restrição:** o MongoDB da disciplina é compartilhado e não é nosso, então
não inserimos documentos na origem. A alternativa é simular na landing, que
é a fonte real da camada Bronze — o comportamento exercitado (filtro por
watermark, carga só do delta, ausência de duplicação) é exatamente o mesmo.

Procedimento, em um notebook no Databricks:

```python
from pyspark.sql import functions as F

COL = "comments"
BASE = f"/Volumes/sample_mflix/landing/mongo/{COL}"

# pasta exec= mais recente, que a Bronze vai ler
ultima = sorted(f.path for f in dbutils.fs.ls(BASE) if f.name.startswith("exec="))[-1]

df = spark.read.parquet(ultima)

# 2 documentos novos com date acima da watermark atual
novos = spark.createDataFrame(
    [("sim001", "2030-01-01T00:00:00", "sim@teste.com", "Simulado 1", "evidencia 3"),
     ("sim002", "2030-01-02T00:00:00", "sim@teste.com", "Simulado 2", "evidencia 3")],
    ["_id", "date", "email", "name", "text"],
)

nova_pasta = f"{BASE}/exec=99999999_999999"
df.unionByName(novos, allowMissingColumns=True).write.mode("overwrite").parquet(nova_pasta)
```

Depois rodar só a task de `comments` (ou a pipeline inteira) e printar a
query padrão: `qtd_lida_origem = 2`, `watermark_final` avançando para
`2030-01-02T00:00:00`, e `count(*) = count(DISTINCT id)` continuando válido.

**Limpeza depois de printar** (senão a simulação vira dado permanente):

```python
dbutils.fs.rm(nova_pasta, recurse=True)
spark.sql("DELETE FROM sample_mflix.bronze.dev_comments WHERE id LIKE 'sim00%'")
spark.sql("""
  DELETE FROM sample_mflix.bronze.control_ingestion_log
  WHERE collection = 'comments' AND watermark_final LIKE '2030%'
""")
```

A última linha é importante: sem ela a watermark fica travada em 2030 e toda
carga incremental seguinte lê 0 registros.

Registrar no print (ou no corpo do PR) que esta execução foi **simulada na
landing**, com o motivo — é mais honesto do que apresentá-la como inserção
na origem.

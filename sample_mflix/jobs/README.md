## Deploy dos Jobs para o Databricks

Os jobs e o dashboard são gerenciados por Databricks Asset Bundle. O comando
roda **na máquina local**, no diretório do `databricks.yml` (`sample_mflix/`),
e não dentro do Databricks.

```bash
cd sample_mflix
databricks bundle deploy -t dev   --profile sample-mflix   # desenvolvimento
databricks bundle deploy -t prod  --profile sample-mflix   # oficial
```

### Executar um job manualmente

```bash
databricks bundle run bronze_pipeline  -t prod --profile sample-mflix
databricks bundle run landing_pipeline -t prod --profile sample-mflix
```

```bash
databricks jobs list --profile sample-mflix | grep -oE "^[0-9]+" | while read id; do
  databricks jobs update --json "{\"job_id\": $id, \"new_settings\": {\"edit_mode\": \"EDITABLE\"}}" --profile sample-mflix
done
```

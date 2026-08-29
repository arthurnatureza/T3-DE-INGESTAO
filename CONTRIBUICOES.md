# Registro de Contribuições

## Grupo: T3-DE-INGESTAO

| Membro | Matrícula | Contribuições principais |
|--------|-----------|--------------------------|
| Amanda Freire Ferreira | 2650353 | Desenvolvimento da **Landing Zone** (`notebooks/mongo-extractor.ipynb`); conexão iterativa com MongoDB; regras de ofuscação (LGPD); conversão forçada de schema para `string` garantindo resiliência; versionamento temporal de arquivos Parquet. |
| Arthur Natureza Reis Bezerra | 2650497 | Desenvolvimento da **Bronze Zone** (`notebooks/bronze_loader.py`); orquestração de jobs (`bronze_pipeline.job.yml`); estruturação da documentação e arquitetura. |
| Carina Barros Timbó | 2650658 | Implementação do módulo de **Controle e Reconciliação** (`control_log.py`); desenvolvimento da lógica de persistência de *watermark* para cargas incrementais; gravação de métricas na tabela `control_ingestion_log`; validação de qualidade de dados (cálculo de divergência, nulos e duplicados). |

## Detalhamento por commit

**Commits - Amanda Freire Ferreira**
```text
00f150d feat(extractor): adicionando pipeline de extracao do mongodb para a landing zone
e04b815 fix(extractor): converte todas as colunas lidas para string
edd9572 Merge pull request #3 from arthurnatureza/feat/extracao-mongo-landing
```

**Commits - Arthur Natureza Reis Bezerra**
```text
1b6e3be feat(readme): primeiro commit
3ae9ad5 feat(README): primeiro commit 
f68e7c9 feat(README): primeiro commit
30c0d18 Merge pull request #1 from arthurnatureza/feat/primeiro-commit-readme
ea8a2ce feat(databricks): add new paths and files
9b238fd Merge pull request #2 from arthurnatureza/feat/add-new-paths-and-files
c49812f feat: add-jobs-and-some-edit
693060c Merge pull request #5 from arthurnatureza/feat/add-jobs-and-some-edit
c0480b9 fix: remove pycache
26dc796 Merge pull request #6 from arthurnatureza/fix/remove-pycache
2bd5107 fix: pipelines condition_task
c7026b4 Merge pull request #7 from arthurnatureza/fix/pipelines-condition_task
8183de2 feat: add some configs
bd44ce1 Merge pull request #8 from arthurnatureza/feat/add-some-configs
```

**Commits - Carina Barros Timbó**
```text
1cde8c7 camada de controle de log
d5ed777 Merge pull request #4 from arthurnatureza/log
```

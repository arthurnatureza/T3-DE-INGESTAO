# Pipeline de Ingestão — `sample_mflix` (MongoDB → Databricks Bronze)

Trabalho final de Ingestão Moderna de Dados. Ingestão das 6 coleções do
banco `sample_mflix` do MongoDB para uma camada Bronze em Delta Lake no
Databricks (Unity Catalog), com rastreabilidade por registro, carga
incremental com watermark persistida e log de execuções.

---

## Arquitetura

```mermaid
flowchart LR
    M[(MongoDB<br/>sample_mflix<br/>6 coleções)]

    subgraph DBX["Databricks — Unity Catalog: sample_mflix"]
        direction TB
        L["landing<br/>Volume /mongo/&lt;colecao&gt;/exec=&lt;timestamp&gt;<br/>Parquet, snapshot completo"]
        B[("bronze.&lt;colecao&gt;<br/>Delta, particionado por _ingestion_date")]
        Q[("bronze.quarentena")]
        C[("bronze.control_ingestion_log")]
        L -->|"bronze_loader.py"| B
        L -.->|"registro sem id / falha de escrita"| Q
        B --> C
        Q --> C
    end

    M -->|"mongo-extractor"| L
```

Três camadas com responsabilidades separadas (R1):

| Camada | Código | O que faz |
|---|---|---|
| Extract | `notebooks/mongo-extractor.ipynb` | Mongo → landing (Parquet versionado por `exec=`) |
| Load | `notebooks/bronze_loader.py` | landing → Bronze (Delta, idempotente) |
| Control | `notebooks/control.py` | watermark, log de execuções, reconciliação |

O fechamento fica em `notebooks/bronze_summary.py`, que consolida o
resultado da execução e falha o Job se alguma coleção terminou em `FAILED`.

Detalhamento das decisões e do histórico de revisões:
[`docs/ARQUITETURA.md`](docs/ARQUITETURA.md).

---

## Modelo da camada Bronze

Cada linha da Bronze é **um documento do Mongo, como veio da landing**.
Não há achatamento em colunas de negócio — isso é responsabilidade da Silver.

| Coluna | Tipo | Origem |
|---|---|---|
| `id` | string | cópia do `_id` do Mongo (o `_id` continua dentro do `json`) |
| `json` | string | documento completo, serializado |
| `_ingestion_id` | string | UUID da execução |
| `_ingestion_timestamp` | timestamp | momento da gravação |
| `_source_path` | string | `mongodb://sample_mflix.<colecao>` |
| `_load_type` | string | `full` ou `incremental` |
| `_ingestion_date` | date | coluna de partição |

`id` é o `_source_id` do enunciado — mantivemos o nome curto porque é
também a chave do `MERGE`. Nunca pode ser nulo; quando é, o registro vai
para a quarentena (ver R7).

**Nomenclatura:** `sample_mflix.bronze.<colecao>` em produção e
`sample_mflix.bronze.dev_<colecao>` em desenvolvimento. O prefixo é a
variável `table_prefix` do bundle — `dev_` no target `dev`, vazio no
target `prod`. Nenhum código muda entre os dois ambientes.

---

## Configuração

Nada de parâmetro no corpo do código (R1). Tudo vem de dois arquivos:

- **`config/pipeline_config.yaml`** — configuração global: catálogo, caminho
  do Volume da landing, tabela de controle, política de retry, tamanho-alvo
  de arquivo e limiar de reconciliação.
- **`config/collections.json`** — parâmetros por coleção: `load_type`,
  `watermark_field` e `exclude_fields` (campos removidos na extração).

Credenciais nunca ficam em arquivo. A URI do Mongo vem de
`dbutils.secrets.get(scope="conn-db", key="cnn-mongodb-sampleflix")`, e a
criação do secret está separada do projeto (`create-secret.py`, na raiz).

### Como adicionar uma coleção nova

1. Acrescentar a entrada em `config/collections.json`.
2. Acrescentar uma task em `jobs/bronze_pipeline.job.yml` com o
   `collection` correspondente e incluí-la no `depends_on` do `resumo`.

Nenhuma linha de Python muda — o mesmo `bronze_loader.py` atende todas.

---

## Modos de carga (R3)

| Coleção | Volume | Modo | Watermark |
|---|---|---|---|
| `movies` | 23.539 | incremental | `lastupdated` |
| `comments` | 50.315 | incremental | `date` |
| `users` | 185 | full | — |
| `theaters` | 1.564 | full | — |
| `sessions` | 1 | full | — |
| `embedded_movies` | 1.500 | full | — |

A watermark fica persistida em `control_ingestion_log.watermark_final` e é
lida por `get_last_watermark()` no início da execução seguinte, que então
filtra `campo > watermark_anterior`. Quando não há watermark anterior
(primeira execução), a carga incremental lê tudo.

**Idempotência:** `MERGE INTO ... ON t.id = s.id` com
`whenMatchedUpdateAll` + `whenNotMatchedInsertAll`. Rodar duas vezes
seguidas atualiza as linhas existentes em vez de duplicá-las — verificado:
em todas as 6 tabelas, `count(*) = count(DISTINCT id)`.

As duas modalidades compartilham o mesmo `write_bronze()`: a diferença
está antes da escrita. A carga incremental aplica o filtro de watermark e
chega ao `MERGE` só com o delta (frequentemente zero linhas); a full chega
com o snapshot completo da landing. Em nenhum dos casos há código
diferente por coleção.

### Sobre "append-only" (R6)

A R6 pede uma Bronze append-only, e é preciso ser transparente: **o
`MERGE` não é append-only**. O `DESCRIBE HISTORY` das tabelas mostra
operações `MERGE` com `numTargetRowsUpdated` maior que zero — em uma
recarga full, todas as linhas são reescritas. Optamos conscientemente por
isso, por três razões:

1. **A própria R3 sanciona o `MERGE`** como estratégia de idempotência,
   ao lado de "append + dedup por chave/hash" e "partição sobrescrita".
   Escolher `MERGE` é exercer uma opção que o enunciado oferece.
2. **A cópia imutável e append-only da origem é a landing.** Cada
   extração grava uma pasta `exec=<timestamp>` nova e jamais reescreve as
   anteriores — o histórico completo de todas as extrações está
   preservado lá, em Parquet, exatamente como veio do Mongo. A Bronze é a
   materialização deduplicada e consultável desse histórico, não o
   arquivo dele. Nenhum dado é perdido em nenhum momento.
3. **O Delta preserva o histórico de qualquer forma.** Cada execução gera
   uma versão nova da tabela, recuperável por *time travel*
   (`DESCRIBE HISTORY`, `VERSION AS OF`). O estado da Bronze em qualquer
   execução passada continua acessível.

O que a R6 pede junto com o append-only — *"preserva o dado como veio: sem
regra de negócio, sem renomear campos de negócio, sem descartar colunas"* —
esse ponto é cumprido integralmente: a coluna `json` é o documento do Mongo
inteiro, sem nenhuma transformação.

**Consequência assumida:** como o `MERGE` atualiza também as colunas de
controle, uma recarga full reescreve `_ingestion_date` das linhas
existentes, que migram para a partição do dia da execução. Ou seja,
`_ingestion_date` significa "última vez que este registro foi confirmado
na origem", não "primeira vez que chegou". A data de primeira chegada
continua recuperável pelo `control_ingestion_log` e pelo time travel.

`movies` usa `lastupdated`, que é string na landing. A comparação
lexicográfica funciona porque o formato é fixo (`YYYY-MM-DD HH:MM:SS...`);
se o formato variasse, seria preciso converter antes de comparar.

---

## Boas práticas de uso de recursos (R2)

Cinco das seis técnicas exigidas estão implementadas:

**1. Leitura em lotes** — o cursor do Mongo usa `batch_size=5000`
(`extract.batch_size`), então os documentos chegam em blocos em vez de uma
resposta única.

**2. Projection / pushdown** — campos largos ou sensíveis nunca saem da
origem: `password` (`users`), `jwt` (`sessions`) e `plot_embedding`
(`embedded_movies`, 1.536 floats por documento). A lista está em
`config/collections.json` e vira `projection` na query do Mongo, ou seja, o
filtro acontece no servidor, não no Spark.

**3. Reuso de conexão** — um único `MongoClient` é criado e reaproveitado
para todas as coleções do loop, em vez de abrir uma conexão por coleção.

**4. Controle de partições no destino** — antes de gravar, o loader faz
`repartition(ceil(linhas / target_rows_per_file))` com alvo de 200.000
linhas por arquivo. Sem isso, `users` (185 linhas) era gravada em 8 arquivos
Parquet; com o controle, vira 1 arquivo. `comments` (50.315) e `movies`
(23.539) também ficam em 1 arquivo cada, eliminando o problema de *small
files* sem cair no extremo oposto para as coleções grandes.

**5. Retry com backoff** — toda operação de I/O do loader (leitura do Volume
e escrita Delta) passa por `com_retry()`, que tenta 3 vezes com backoff
exponencial (2s, 4s). Cobre falha transitória de storage de objeto, que é a
origem real de I/O da camada Bronze.

**Exceção declarada:** o método `read()` do extrator materializa o cursor em
uma lista Python (`[... for d in cursor]`) antes de criar o DataFrame. Isso
é equivalente a `list(cursor)` e é a única exceção ao item "ausência de
`collect()`/`toPandas()`/`list(cursor)`". Justificativa: a maior coleção tem
50.315 documentos já sem os campos largos (removidos por projection), o que
cabe com folga na memória do driver; o `batch_size` limita o tráfego por
ida à rede. A alternativa correta para volume maior seria o conector
oficial `mongo-spark`, que paraleliza a leitura entre os executores — não
foi usado por indisponibilidade do JAR no ambiente da disciplina. **A camada
Bronze não tem nenhum `collect()`/`toPandas()`**: tudo é DataFrame
distribuído, do `spark.read.parquet` até o `MERGE`.

---

## Rastreabilidade e controle (R4, R5)

Toda tabela Bronze carrega as cinco colunas de controle da tabela acima.
Toda execução — com sucesso ou não — grava uma linha em
`sample_mflix.bronze.control_ingestion_log`:

`_ingestion_id`, `collection`, `load_type`, `watermark_inicial`,
`watermark_final`, `qtd_lida_origem`, `qtd_gravada_destino`, `start_time`,
`end_time`, `duracao_seg`, `status`, `error_message`,
`pct_nulos_source_id`, `qtd_duplicados_source_id`.

O `log_execution()` fica dentro do bloco de fechamento da função, então uma
exceção no meio do carregamento ainda produz uma linha com `status=FAILED`
e a mensagem do erro — o log nunca fica com um buraco.

---

## Tratamento de schema (R7)

O enunciado aceita duas estratégias; escolhemos **persistir o documento como
JSON bruto**. Consequências:

- O schema da Bronze é fixo (`id`, `json`, colunas de controle) e **não muda
  nunca**, independentemente do que aconteça na origem. Campo novo, campo
  ausente ou tipo divergente entre documentos não quebram a carga nem exigem
  `mergeSchema` na escrita — o documento inteiro entra como está.
- `mergeSchema=true` continua ativo na **leitura** da landing, porque
  arquivos Parquet da mesma pasta `exec=` podem ter conjuntos de colunas
  diferentes entre si.
- **Impacto na Silver:** o custo é deslocado para lá. A Silver precisa fazer
  `from_json` com schema explícito (ou `schema_of_json`) para tipar os
  campos, e é lá que mora o achatamento de `imdb`/`tomatoes` e o `explode`
  de `cast`/`genres`. Em troca, a Bronze fica imune a schema drift e nunca
  precisa ser reprocessada a partir da origem: qualquer mudança de
  interpretação é resolvida relendo o `json` que já está armazenado.

**Quarentena:** registros com `id` nulo (documento sem `_id` utilizável) e
lotes que falham na escrita vão para `bronze.quarentena` com o JSON
original, a coleção, o motivo e o `_ingestion_id`. Nada é descartado em
silêncio; a execução é marcada `PARTIAL` em vez de derrubar o Job.

---

## Reconciliação e qualidade (R8)

Ao final de cada carga, `reconcile()` compara origem e destino e devolve o
status que vai para o log:

- **divergência** = `|qtd_lida_origem − qtd_gravada_destino| / qtd_lida_origem`
- **percentual de nulos** em `id`
- **duplicidade** de `id` dentro do lote

**Limiar adotado: 5% de divergência** (`reconcile.limiar_divergencia` no
`pipeline_config.yaml`). A execução é marcada `PARTIAL` se a divergência
passar de 5% **ou** se houver qualquer `id` nulo **ou** qualquer duplicado —
para nulos e duplicados a tolerância é zero, porque `id` é a chave do
`MERGE`: um nulo ou duplicado ali comprometeria a idempotência. Os 5% de
folga existem só para a diferença de contagem, que pode variar
legitimamente quando registros são desviados para a quarentena.

---

## Dashboard de Monitoramento de Ingestão

Para acompanhar e garantir a qualidade da carga de dados da camada **Landing** para a **Bronze**, utilizou-se um dashboard de monitoramento no Databricks construído sobre a tabela de controle `bronze.control_ingestion_log`.

### Indicadores Principais (KPIs)

- **Total de Ingestões:** Total acumulado de execuções de pipeline.
- **Total de Falhas:** Quantidade de execuções com erro.

### Painéis e Gráficos

- **Volume de Ingestões por Dia:** Acompanhamento temporal das cargas divididas em volume total, sucessos e falhas.
- **Status por Collection:** Distribuição das execuções agrupadas por coleção (ex.: `comments`, `movies`, `sessions`, `theaters`, `users`).
- **Duração Média (segundos):** Tendência de tempo de execução das cargas ao longo do tempo.
- **Taxa de Falha (%):** Percentual de erro nas cargas diárias.
- **Detalhes das Ingestões:** Tabela detalhada por Data, *Collection* e Tipo de Carga (*Load Type*), incluindo métricas de registros lidos/gravados e duração.

### Filtros Globais

- **Collection:** Permite filtrar todas as métricas do painel por uma coleção específica ou visualizar todas de forma consolidada.

### Visualização Final

![alt text](./docs/imgs/image.png)
---

## Como executar

O projeto é um Databricks Asset Bundle. Requer a CLI do Databricks
autenticada (`databricks auth login --host <workspace>`).

```bash
cd sample_mflix

databricks bundle validate -t dev        # confere a configuração
databricks bundle deploy   -t dev        # sobe notebooks + cria/atualiza o Job
databricks bundle run bronze_pipeline -t dev
```

O target `dev` cria tudo com prefixo `dev_`; `-t prod` usa os nomes finais,
sem nenhuma alteração de código. A extração (Mongo → landing) é executada
antes, pelo `notebooks/mongo-extractor.ipynb`.

O Job tem 7 tasks: uma por coleção (as 6 rodam em paralelo) e a `resumo`,
que depende de todas com `run_if: ALL_DONE` — ela roda mesmo se alguma
coleção falhar, para que o log de fechamento sempre seja produzido.

---

## Limitações conhecidas

**Leitura só da pasta `exec=` mais recente.** A Bronze consome sempre o
último snapshot da landing, nunca as pastas intermediárias. É seguro porque
cada `exec=` é uma extração completa da coleção (não um delta), então pular
pastas não perde dado novo. A exceção é um documento **apagado** do Mongo
entre duas extrações: ele nunca chega à Bronze. Aceito conscientemente —
tratar isso exigiria controlar quais pastas já foram consumidas.

**Inferência de schema por amostra na landing.** A extração infere o schema
com `sample_size=1000`. Em coleção heterogênea, o schema inferido pode variar
entre execuções sem que nada tenha mudado na origem. Não afeta a Bronze
(que só serializa o que chegou), mas afeta a estabilidade das colunas da
landing.

**`_ingestion_date` é a última confirmação, não a chegada original**, e a
Bronze não é append-only no sentido estrito. Decisão consciente, detalhada
na seção "Sobre 'append-only' (R6)" acima.

**Quarentena nunca acionada em dado real.** Até agora todos os documentos
tinham `_id` válido e nenhuma escrita falhou, então a tabela está com 0
linhas. O caminho feliz está validado; o de exceção, apenas por inspeção de
código.

---

## Credenciais no histórico do repositório

O comando de verificação sugerido no `SEND_WORK.md` encontra uma URI de
conexão no histórico:

```
mongodb://root:***@167.88.45.227:27017/?directConnection=true
```

Ela vem dos commits `7c6ac2a` e `eef71d6`, no arquivo `create-secret.py` da
raiz — **material original da disciplina, anterior ao trabalho do grupo**.
Nenhum arquivo dentro de `sample_mflix/` contém credencial: a conexão é
sempre resolvida por `dbutils.secrets`.

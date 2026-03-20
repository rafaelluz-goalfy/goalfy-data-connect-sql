# goalfy-data-connect-sql

Worker de sincronização SQL da plataforma Goalfy Data Connect.

## Responsabilidades

- Conectar em bancos SQL externos (PostgreSQL, MySQL, SQL Server)
- Testar conectividade e validar credenciais
- Descobrir schemas, tabelas, views e colunas
- Executar leitura full ou incremental em batches
- Persistir dados em `raw_ingestion` e `normalized`
- Publicar eventos de execução no Kafka
- Controlar checkpoint e lock de execução

## Arquitetura

```
cmd/
  goalfy-data-connect-sql/    ← entrypoint, event loop
internal/
  config/                     ← configuração + secret loader
  models/                     ← domain types
  connectors/sql/             ← factory de conectores (pg/mysql/mssql)
  discovery/                  ← schema discovery via information_schema
  reader/                     ← leitura full/incremental em batches
  checkpoint/                 ← watermark de sincronização incremental
  persistence/                ← escrita em raw_ingestion e normalized
  locks/                      ← Redis distributed lock
  kafka/                      ← consumer e publisher de eventos
  jobs/                       ← orquestrador do ciclo de execução
migrations/                   ← SQL migrations do banco interno
deploy/k8s/                   ← manifests Kubernetes (GKE)
```

## Stack

| Componente | Tecnologia |
|---|---|
| Linguagem | Go 1.22 |
| Mensageria | Kafka (segmentio/kafka-go) |
| Lock distribuído | Redis |
| Banco interno | PostgreSQL |
| Deploy | GKE |

## Fluxo de execução

```
Kafka: data.connect.sync.requested
  → Acquire Redis lock (tenant + source + dataset)
  → Load SourceConfig + DatasetConfig from internal DB
  → Resolve password from secret
  → Publish sync.started
  → Connect to external SQL source
  → Full load OR Incremental (with checkpoint)
      → Read batch
      → Write raw_ingestion (upsert)
      → Write normalized (upsert)
      → Publish batch.processed
      → Refresh Redis lock
  → Save checkpoint (if incremental)
  → Publish sync.completed OR sync.failed
  → Update sync_executions table
  → Release lock
```

## Eventos Kafka

| Tópico | Quando |
|---|---|
| `data.connect.sync.requested` | consumido para iniciar execução |
| `data.connect.sync.started` | publicado ao iniciar |
| `data.connect.batch.processed` | publicado após cada batch |
| `data.connect.sync.completed` | publicado ao concluir com sucesso |
| `data.connect.sync.failed` | publicado em caso de erro |

## Variáveis de ambiente

| Variável | Descrição | Padrão |
|---|---|---|
| `KAFKA_BROKERS` | Lista de brokers separada por vírgula | — |
| `KAFKA_GROUP_ID` | Consumer group | `goalfy-data-connect-sql` |
| `REDIS_ADDR` | Endereço Redis | — |
| `REDIS_PASSWORD` | Senha Redis | — |
| `POSTGRES_DSN` | DSN do banco interno | — |
| `SECRET_PROVIDER` | `env` ou `gcp` | `env` |
| `SECRET_PREFIX` | Prefixo para env vars de secrets | `SECRET_` |
| `APP_LOG_LEVEL` | `debug`, `info`, `warn`, `error` | `info` |
| `APP_ENVIRONMENT` | Ambiente de execução | `development` |

## Segredos de fontes externas

A senha da fonte nunca é armazenada em texto puro. O campo `password_ref` na tabela `data_sources` contém uma referência (ex: `MY_PG_SOURCE`) que é resolvida pelo `SecretLoader`.

Com `SECRET_PROVIDER=env`, o worker busca a variável `SECRET_MY_PG_SOURCE` no ambiente.

## Rodando localmente

```bash
# Subir infraestrutura local
docker-compose up -d postgres redis kafka

# Aplicar migrations
psql $POSTGRES_DSN -f migrations/001_initial_schema.sql

# Rodar o worker
go run ./cmd/goalfy-data-connect-sql
```

## Modos de sync

### Full load
- Lê todos os registros da tabela/view
- Usa keyset pagination quando `technical_key` está configurado (mais eficiente em tabelas grandes)
- Apaga checkpoint ao concluir

### Incremental por data
- Busca registros com `updated_at > last_checkpoint`
- Salva o maior valor encontrado como novo checkpoint

### Incremental por chave crescente
- Busca registros com `id > last_id`
- Keyset scan — evita `OFFSET` em tabelas grandes

## Idempotência

Toda persistência usa `INSERT ... ON CONFLICT DO UPDATE` (upsert). Reprocessamento não duplica dados.

## Observabilidade

Todos os logs são estruturados (JSON) e rastreáveis por:
- `execution_id`
- `tenant_id`
- `source_id`
- `dataset_id`

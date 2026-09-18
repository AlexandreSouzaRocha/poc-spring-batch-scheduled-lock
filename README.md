# POC — Spring Batch + Scheduler + ShedLock: particionamento atômico de arquivos

Um arquivo grande chega no **Azure Blob Storage** (Azurite, localmente). Um scheduler com
**ShedLock no MongoDB** garante que só uma instância processa cada ciclo. O
**Spring Batch** divide o arquivo em N arquivos **em paralelo, com virtual threads**,
move o original para `processados/`, grava cada partição na pasta do seu tipo de
movimento e publica os metadados de cada partição no **Kafka**. O resume e o recovery
usam o JobRepository customizado no MongoDB, com a mesma estrutura do
[poc-spring-batch](https://github.com/AlexandreSouzaRocha/poc-spring-batch).

```
generator ──► blob entrada/ ──► scheduler único (@Scheduled + @SchedulerLock)
                                     │ 1. lista entrada/ e registra em received_file_management
             partitioner-1 ┐         │ 2. particiona os pendentes no mesmo ciclo
                           ├─ disputam o lock ──► filePartitionJob (Spring Batch, JobRepository no Mongo)
             partitioner-2 ┘                          │
                                                      ├─► aberto/ fechado/ saldo/ ultima/  (N partições em paralelo)
                                                      ├─► processados/                    (original)
                                                      └─► Kafka movimentos-particionados  (1 msg por partição)
```

**Stack:** Spring Boot 4.1 · Java 25 (virtual threads) · Spring Batch 6 · ShedLock 7.10 ·
MongoDB driver 5.11 · Azure Storage Blob SDK 12.35 · Kafka · Docker (Red Hat UBI9, ParallelGC).

## Quickstart

Pré-requisitos: Docker e Docker Compose. Java 25 só é necessário para build e testes fora de container.

```bash
make up                                   # infra + generator (:8090) + partitioner-1 (:8081) + partitioner-2 (:8082)
make generate LINES=5000000 TYPE=FECHADO  # gera o arquivo grande direto no blob (entrada/)
make status                               # acompanha: PENDING -> PARTITIONING -> COMPLETED
make metrics                              # STEP_METRICS / JOB_METRICS (tempo de particionamento)
make lock-check                           # ciclos por instância e overlaps=0
make kafka-tail                           # mensagens publicadas
```

Testes automatizados:

```bash
make test                                 # unitários + integração do lock (Testcontainers)
make e2e LINES=5000000 FILES=2            # ponta a ponta com validações no blob, Mongo, Kafka e lock
make chaos-test SCENARIO=all              # partition-fail, publish-fail, invalid-file, slow-io, kill-owner
make load-test SIZES="50 100 200 250"     # benchmark por volume, limpando o ambiente entre execuções
```

`make help` lista todos os alvos. Entre execuções pesadas, `make reset-data` zera blob, Mongo e
Kafka e devolve o disco; `make disk` e `make blob-usage` mostram o espaço em uso.

Collections, índices, tópico do Kafka e container do blob são criados pelos containers de init
(`mongo-init`, `kafka-init`, `azurite-init`), nunca pela aplicação. Ver
[ARCHITECTURE.md](docs/ARCHITECTURE.md#infraestrutura-criada-fora-da-aplicação).

## Layout do arquivo

```
H2026-09-16ABERTO \n                        header: 'H' + yyyy-MM-dd + tipo (7 bytes, padding à direita)
D0042000012345620...(150 bytes)\n           detalhe: 150 bytes fixos
```

Tipos: `ABERTO`, `FECHADO`, `SALDO`, `ULTIMA`. **Cada partição recebe uma cópia do header.**

## Configuração principal

| Propriedade | Env | Padrão | Descrição |
|---|---|---|---|
| `app.partition.count` | `APP_PARTITION_COUNT` | `10` | Arquivos gerados por arquivo grande |
| `app.partition.max-attempts` | `APP_PARTITION_MAX_ATTEMPTS` | `3` | Tentativas antes de mover para `erros/` |
| `app.partition.max-concurrent-files` | `APP_PARTITION_MAX_CONCURRENT_FILES` | `1` | Arquivos grandes em paralelo no mesmo ciclo |
| `app.scheduler.file-processing.interval` | `APP_SCHEDULER_INTERVAL` | `30s` | Intervalo do ciclo (polling + particionamento) |
| `app.scheduler.file-processing.lock-at-most-for` | `APP_SCHEDULER_LOCK_AT_MOST_FOR` | `60s` | Validade do lock (renovada pelo keep-alive) |
| `app.shedlock.fields.name` | `APP_SHEDLOCK_FIELD_NAME` | `_id` | Campo que guarda o nome do lock |
| `app.shedlock.collection` | `APP_SHEDLOCK_COLLECTION` | `scheduler_locks` | Collection do lock |
| `app.shedlock.fields.*` | `APP_SHEDLOCK_FIELD_*` | `_id`, `lock_until`, `locked_at`, `locked_by` | Nomes dos campos do lock |
| `app.blob.upload-block-size-mb` | `APP_BLOB_UPLOAD_BLOCK_SIZE_MB` | `8` | Tamanho do bloco de upload (memória por partição) |
| `app.kafka.topic` | `APP_KAFKA_TOPIC` | `movimentos-particionados` | Tópico de saída |
| — | `OTEL_ENABLED` | `true` | Liga o agente do OpenTelemetry (gera `trace_id`/`span_id` nos logs) |

O MongoDB roda com `transactionLifetimeLimitSeconds=60`, igual a produção. Nenhum I/O de blob ou
Kafka acontece dentro de transação do Mongo (ver [ARCHITECTURE.md](docs/ARCHITECTURE.md#transações-do-mongodb)).

## Endpoints

| Método | Endpoint | Instância | Descrição |
|---|---|---|---|
| `POST` | `/generator/files?lines=&movementType=&movementDate=&files=&invalidHeader=` | generator | Gera arquivos grandes em `entrada/` |
| `GET` | `/files?status=&limit=` | todas | Arquivos grandes mais recentes |
| `GET` | `/files/summary` | todas | Contagem por status |
| `GET` | `/files/{id}` | todas | Original e partições |
| `GET` | `/files/{id}/verification` | todas | Conferência no blob (tamanho, header, publicação) |
| `GET` | `/locks` | todas | Documentos de lock do ShedLock |
| `PUT` / `DELETE` / `GET` | `/chaos` | particionadores | Injeção de falhas para teste de resiliência |
| `GET` | `/actuator/prometheus` | todas | Timers `partitioner_step_duration` e `partitioner_job_duration` |

## Documentação

* [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): fluxo, steps do job, resume e recovery, ShedLock configurável, collections e pastas.
* [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md): formato de log, STEP_METRICS/JOB_METRICS, Prometheus e auditoria do lock.
* [docs/TESTING.md](docs/TESTING.md): testes unitários, e2e e cenários de chaos.

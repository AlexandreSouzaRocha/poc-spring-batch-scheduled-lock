# Observabilidade

## Formato de log

Toda linha da aplicação segue o mesmo formato:

```
[request_id] context operation message campo=valor campo2=valor {"json":"dados complementares"}
```

| Parte | Exemplo | Significado |
|---|---|---|
| `request_id` | `part-1e646ce7/d303b501` | Correlação. Os ciclos do scheduler geram `poll-*` e `part-*`, cada arquivo do ciclo ganha um sufixo (`/<fileId>`) e o HTTP usa o header `X-Request-Id`. O valor é propagado para as virtual threads das partições |
| `context` | `partition-job` | Área da aplicação: `scheduler`, `blob-polling`, `file-partitioning`, `partition-job`, `partition-recovery`, `metrics`, `generator`, `chaos`, `http` |
| `operation` | `partition.upload` | Operação que gerou o log |
| `message` | `partição enviada ao blob` | Texto para leitura humana |
| `campo=valor` | `fileId=... partitionIndex=4` | Campos para filtrar com `grep` |
| JSON | `{"target":"...","byteRange":{...}}` | Dados complementares |

Erros **nunca** saem com stack trace. `ErrorSummary` gera um resumo com o que é preciso para troubleshooting:

```
[part-3f1a9c2e/66851c48] file-partitioning file.process falha ao executar o job fileId=66851c48-... {"error":{"type":"org.springframework.dao.DataAccessResourceFailureException","message":"...","root_cause_type":"com.mongodb.MongoSocketReadException","root_cause_message":"Prematurely reached end of stream","at":"br.com.spring.batch.partitioner.service.FilePartitionJobRunner.start:57"}}
```

Nos logs de bibliotecas (Spring, Kafka, Azure), `logging.exception-conversion-word: "%wEx{short}"` limita a exceção à primeira linha.

## Métricas de processamento

### STEP_METRICS: uma linha por step e por partição

```
[part-1e646ce7/d303b501] metrics step.metrics STEP_METRICS step=partitionMasterStep fileId=d303b501-... jobExecutionId=1 status=COMPLETED durationMs=3091 lines=5000000 bytes=755000190 linesPerSec=1617599 mbPerSec=232.94 {"stepExecutionId":3,"failures":[]}
```

O `durationMs` do `partitionMasterStep` é o **tempo de particionamento do arquivo**: vai do
plano de partições até a última partição gravada. `lines` e `bytes` somam as partições.
O `partitionWorkerStep:partitionNNNN` mostra o tempo de cada partição.

### JOB_METRICS: uma linha por execução do job

```
[part-1e646ce7/d303b501] metrics job.metrics JOB_METRICS job=filePartitionJob fileId=d303b501-... jobExecutionId=1 status=COMPLETED attempt=1 durationMs=3517 lines=5000000 bytes=755000019 partitions=10 linesPerSec=1421666 mbPerSec=204.73 {"fileName":"MOV_ABERTO_...dat","failures":[]}
```

### Comandos

```bash
make metrics              # JOB_METRICS e STEP_METRICS (sem as partições individuais)
make metrics-partitions   # STEP_METRICS de cada partição
make prometheus           # timers expostos no /actuator/prometheus
```

### Micrometer / Prometheus

| Métrica | Tags | Descrição |
|---|---|---|
| `partitioner_step_duration_seconds` | `step`, `status` | Duração de cada step (o worker aparece como `partitionWorkerStep`) |
| `partitioner_job_duration_seconds` | `status` | Duração total do job por arquivo |

Endpoint: `GET /actuator/prometheus` em cada particionador (`:8081` e `:8082`).

## Lock

```bash
make locks        # documento de cada lock: dono (locked_by) e validade (lock_until)
make lock-check   # ciclos executados por instância e sobreposição entre elas (deve ser overlaps=0)
```

Cada ciclo com lock gera dois logs, `lock.acquired` e `cycle.end`. O `scripts/lock-audit.py`
monta os intervalos de cada instância e confere que nenhum intervalo de uma cruza com o da outra:

```
  partitioner-1  scheduler=blob-polling       ciclos_executados=14
  partitioner-2  scheduler=blob-polling       ciclos_executados=9
  partitioner-1  scheduler=file-partitioning  ciclos_executados=15
  partitioner-2  scheduler=file-partitioning  ciclos_executados=8
overlaps=0
```

## Estado dos arquivos

```bash
make status          # contagem por status + últimos arquivos (tentativas, linhas, partições, duração)
make file ID=<id>    # documento do original e das partições
make verify ID=<id>  # confere no blob: partições existem, tamanho, header igual ao original, publicação
```

# Observabilidade

## Formato de log

Todas as linhas saem em `logfmt`: campos `chave=valor`, a mensagem em `msg` e os dados
complementares como JSON. É um formato que o Dynatrace e qualquer parser de log entendem sem
regra customizada.

```
<timestamp> level=<nível> logger=<classe> thread=<thread> context=<área> operation=<operação> <campos> msg="[request_id] <mensagem>" data={<json>} ex={<json do erro>}
```

Exemplo:

```
2026-09-17T00:41:29.912Z level=INFO logger=b.c.s.b.p.b.t.PartitionWriterTasklet thread=virtual-57 context=partition-job operation=partition.upload fileId=d303b501-7f46-3944-89be-38ff7d1fd97e partitionIndex=4 lines=100000 bytes=15100019 durationMs=620 msg="[cycle-1e646ce7/d303b501] partição enviada ao blob" data={"target":"fechado/2026-09-15/d303b501.../MOV_..._part_0004.dat","byteRange":{"start":45300019,"end":60400019}}
```

| Campo | Exemplo | Significado |
|---|---|---|
| `level`, `logger`, `thread` | `INFO`, `b.c.s.b.p...PartitionWriterTasklet`, `virtual-57` | Vêm do pattern do Logback |
| `context` | `partition-job` | Área da aplicação: `scheduler`, `blob-polling`, `file-partitioning`, `partition-job`, `partition-recovery`, `metrics`, `generator`, `chaos`, `http`, `blob-storage` |
| `operation` | `partition.upload` | Operação que gerou a linha |
| campos livres | `fileId=... partitionIndex=4 durationMs=620` | Os campos importantes de cada operação, fáceis de filtrar e de virar atributo no Dynatrace |
| `msg` | `"[cycle-1e64/d303] partição enviada ao blob"` | O `request_id` de correlação e o texto para leitura humana. Entre aspas porque tem espaço e vem antes de `data` |
| `data` | `{"target":"...","byteRange":{...}}` | Dados complementares em JSON |
| `ex` | `{"type":"...","message":"...","root_cause_type":"...","at":"..."}` | Só em erros |

O `request_id` correlaciona tudo de um ciclo: `cycle-<id>` para o ciclo do scheduler e
`cycle-<id>/<fileId>` para cada arquivo, inclusive nos logs das virtual threads das partições.
No HTTP ele vem do header `X-Request-Id`, ou é gerado como `http-<id>`.

Erros **nunca** saem com stack trace. O campo `ex` traz o resumo necessário para troubleshooting:

```
2026-09-17T00:44:02.118Z level=ERROR logger=b.c.s.b.p.s.FilePartitionJobRunner thread=scheduling-1 context=file-partitioning operation=file.process fileId=66851c48 msg="[cycle-3f1a9c2e/66851c48] falha ao executar o job" ex={"type":"org.springframework.dao.DataAccessResourceFailureException","message":"...","root_cause_type":"com.mongodb.MongoSocketReadException","root_cause_message":"Prematurely reached end of stream","at":"br.com.spring.batch.partitioner.service.FilePartitionJobRunner.start:57"}
```

Os logs de bibliotecas (Spring, Kafka, Mongo, Azure) saem no mesmo esqueleto, com a mensagem
original em `msg` e a exceção resumida a uma linha. A configuração está em
[`logback-spring.xml`](../src/main/resources/logback-spring.xml): um appender para a aplicação e
outro para o resto.

## Métricas de processamento

### STEP_METRICS: uma linha por step e por partição

```
2026-09-17T00:41:30.104Z level=INFO logger=b.c.s.b.p.b.l.StepMetricsListener thread=scheduling-1 context=metrics operation=step.metrics step=partitionMasterStep fileId=d303b501-... jobExecutionId=1 status=COMPLETED durationMs=3091 lines=5000000 bytes=755000190 linesPerSec=1617599 mbPerSec=232.94 msg="[cycle-1e646ce7/d303b501] STEP_METRICS" data={"stepExecutionId":3,"failures":[]}
```

O `durationMs` do `partitionMasterStep` é o **tempo de particionamento do arquivo**: vai do
plano de partições até a última partição gravada. `lines` e `bytes` somam as partições.
O `partitionWorkerStep:partitionNNNN` mostra o tempo de cada partição.

### JOB_METRICS: uma linha por execução do job

```
2026-09-17T00:41:30.417Z level=INFO logger=b.c.s.b.p.b.l.JobMetricsListener thread=scheduling-1 context=metrics operation=job.metrics job=filePartitionJob fileId=d303b501-... jobExecutionId=1 status=COMPLETED attempt=1 durationMs=3517 lines=5000000 bytes=755000019 partitions=10 linesPerSec=1421666 mbPerSec=204.73 msg="[cycle-1e646ce7/d303b501] JOB_METRICS" data={"fileName":"MOV_ABERTO_...dat","failures":[]}
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

Cada ciclo com lock gera dois logs, `operation=lock.acquired` e `operation=cycle.end`. O `scripts/lock-audit.py`
monta os intervalos de cada instância e confere que nenhum intervalo de uma cruza com o da outra:

```
  partitioner-1  scheduler=file-processing  ciclos_executados=15
  partitioner-2  scheduler=file-processing  ciclos_executados=8
overlaps=0
```

## Estado dos arquivos

```bash
make status          # contagem por status + últimos arquivos (tentativas, linhas, partições, duração)
make file ID=<id>    # documento do original e das partições
make verify ID=<id>  # confere no blob: partições existem, tamanho, header igual ao original, publicação
```

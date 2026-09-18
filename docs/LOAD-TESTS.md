# Testes de carga

Escalada de 50MM até 250MM de linhas, uma execução por volume, com o ambiente zerado entre elas.
Cada execução está documentada abaixo com o comando, as métricas coletadas e as observações.

## Ambiente

| Item | Valor |
|---|---|
| Host | Apple Silicon, Docker Desktop com 12 CPUs e 36 GB |
| Disco da VM do Docker | 252 GB (191 GB livres no início da bateria) |
| Partitioner | 2 containers, 2 CPUs e 2 GB cada, ParallelGC, `MaxRAMPercentage=70` |
| Generator | 1 container, 2 CPUs e 1 GB |
| Mongo | `transactionLifetimeLimitSeconds=60` (igual a produção) |
| Blob | Azurite (Node), volume local |
| Partições por arquivo | 10 |
| OpenTelemetry | agente ligado, exportação desligada |

O Azurite é um emulador em Node e roda no mesmo host do particionador: os números de MB/s medem o
caminho completo (leitura do blob, montagem das partições e escrita de volta), não a capacidade do
Azure Storage real.

## Método

```bash
make load-test SIZES="50"      # e assim por diante: 100, 200, 250
```

Para cada volume, `scripts/load-test.sh`:

1. confere o disco livre contra o necessário (~3,2× o arquivo) e aborta o volume se não couber;
2. zera blob, Mongo e Kafka (`scripts/reset-data.sh`);
3. gera o arquivo em `entrada/` pelo generator e mede o tempo de geração;
4. espera o ciclo do scheduler pegar o arquivo e o job terminar em `COMPLETED`;
5. coleta `STEP_METRICS`/`JOB_METRICS`, o delta de mensagens no Kafka, o pico de memória dos
   containers (`docker stats` a cada 5 s) e o mínimo de disco livre durante a execução.

Métricas que importam:

* **partição ms** — `durationMs` do `partitionMasterStep`: do plano de partições até a última
  partição gravada no blob. É o tempo de particionamento propriamente dito.
* **job ms** — `durationMs` do `filePartitionJob`: inclui validação do header, cleanup, registro no
  Mongo, move do original e publicação no Kafka.
* **geração s** — tempo do generator para criar o arquivo no blob. Não faz parte do SLA do
  particionador, mas define quanto tempo cada rodada leva.

## Resultados

| Volume | Arquivo | Geração | Partição | Partição MB/s | Job | Partições | Kafka | Pico mem | Disco livre mín. | Status |
|---|---|---|---|---|---|---|---|---|---|---|
| 50MM | 7,03 GB | 50 s | 31,8 s | 226,7 | 32,5 s | 10 | 10 | 1694 MB | 181 GB | COMPLETED |
| 100MM | 14,06 GB | 100 s | 100,3 s | 143,5 | 101,9 s | 10 | 10 | 1722 MB | 166 GB | COMPLETED |
| 200MM | 28,13 GB | 347 s | 373,6 s | 77,1 | 375,4 s | 10 | 10 | 1779 MB | 136 GB | COMPLETED |

## Execuções

### 50MM — 7,03 GB

```bash
RESULTS_FILE=benchmarks/load-test-050MM.md ./scripts/load-test.sh 50
```

Resultado em [benchmarks/load-test-050MM.md](../benchmarks/load-test-050MM.md).

| Métrica | Valor |
|---|---|
| Linhas | 50.000.000 |
| Bytes do original | 7.550.000.019 |
| Geração no blob | 50 s (≈151 MB/s) |
| `partitionMasterStep` | **31.761 ms** — 1.574.258 linhas/s, **226,70 MB/s** |
| `filePartitionJob` | 32.483 ms — 1.539.267 linhas/s, 221,66 MB/s |
| Tentativas | 1 |
| Mensagens no Kafka | 10 (1 por partição) |
| Pico de memória do container | 1.694 MB de 2 GB |
| Disco livre mínimo | 181 GB |

Tempo por step:

| Step | Duração |
|---|---|
| `validateHeaderStep` | 71 ms |
| `cleanupPartitionsStep` | 22 ms |
| `partitionMasterStep` | 31.761 ms |
| `registerPartitionsStep` | 31 ms |
| `moveOriginalStep` | 52 ms |
| `publishPartitionsStep` | 391 ms |

Observações:

* O particionamento é 97,8% do job. Todo o resto — validação, cleanup, registro no Mongo, move do
  original e publicação das 10 mensagens — soma 567 ms e não cresce com o volume.
* O `moveOriginalStep` leva 52 ms para 7,03 GB porque o move é uma operação de servidor do blob
  (`copy` + `delete`), não um download/upload pela aplicação.
* Ocupação no blob ao final: 7,03 GB em `aberto/` (10 partições) + 7,03 GB em `processados/`.
* O pico de 1.694 MB em um container de 2 GB é o ponto a observar na escalada: o consumo vem dos
  buffers de upload (10 partições × bloco de 8 MB) mais o heap do Spring Batch, e não do tamanho do
  arquivo, mas está perto do limite configurado.

### 100MM — 14,06 GB

```bash
RESULTS_FILE=benchmarks/load-test-100MM.md ./scripts/load-test.sh 100
```

Resultado em [benchmarks/load-test-100MM.md](../benchmarks/load-test-100MM.md).

| Métrica | Valor |
|---|---|
| Linhas | 100.000.000 |
| Bytes do original | 15.100.000.019 |
| Geração no blob | 100 s (≈144 MB/s) |
| `partitionMasterStep` | **100.327 ms** — 996.741 linhas/s, **143,54 MB/s** |
| `filePartitionJob` | 101.942 ms — 141,26 MB/s |
| Tentativas | 1 |
| Mensagens no Kafka | 10 |
| Pico de memória do container | 1.722 MB de 2 GB |
| Disco livre mínimo | 166 GB |

Tempo por step:

| Step | Duração |
|---|---|
| `validateHeaderStep` | 63 ms |
| `cleanupPartitionsStep` | 21 ms |
| `partitionMasterStep` | 100.327 ms |
| `registerPartitionsStep` | 57 ms |
| `moveOriginalStep` | 164 ms |
| `publishPartitionsStep` | 1.089 ms |

Tempo de cada partição (`partitionWorkerStep`), em ms:

```
partition0008 96059   partition0010 96058   partition0009 96339   partition0006 96877
partition0005 97700   partition0007 98415   partition0001 98490   partition0002 99262
partition0004 99898   partition0003 100199
```

Observações:

* **O throughput caiu de 226,7 para 143,5 MB/s.** Dobrar o volume multiplicou o tempo por 3,15, não
  por 2. A escala deixou de ser linear entre 50MM e 100MM.
* A queda **não** vem de uma partição lenta: as dez terminam em 96–100 s, com 4% de diferença entre a
  mais rápida e a mais lenta. Todas desaceleraram junto, o que aponta para um recurso compartilhado
  (Azurite ou disco), não para desbalanceamento do plano de partições.
* A hipótese principal é o cache de página da VM do Docker (36 GB): no 50MM o arquivo de 7 GB e suas
  partições cabem em cache; no 100MM são 15 GB lidos mais 15 GB escritos, e a leitura passa a bater
  no disco de verdade.
* O pico de memória continua estável (1.722 MB contra 1.694 MB do 50MM), confirmando que o consumo
  do particionador não acompanha o tamanho do arquivo.
* A partir daqui as execuções também amostram a CPU do container do Azurite, para separar
  "Azurite saturado" de "disco saturado".

### 200MM — 28,13 GB

```bash
RESULTS_FILE=benchmarks/load-test-200MM.md ./scripts/load-test.sh 200
```

Resultado em [benchmarks/load-test-200MM.md](../benchmarks/load-test-200MM.md), amostras de CPU e
memória em `benchmarks/load-test-200MM-detalhes.log`.

| Métrica | Valor |
|---|---|
| Linhas | 200.000.000 |
| Bytes do original | 30.200.000.019 |
| Geração no blob | 347 s (≈83 MB/s) |
| `partitionMasterStep` | **373.566 ms** — 535.381 linhas/s, **77,10 MB/s** |
| `filePartitionJob` | 375.379 ms — 76,73 MB/s |
| Tentativas | 1 |
| Mensagens no Kafka | 10 |
| Pico de memória do container | 1.779 MB de 2 GB |
| Disco livre mínimo | 136 GB |

Tempo por step:

| Step | Duração |
|---|---|
| `validateHeaderStep` | 225 ms |
| `cleanupPartitionsStep` | 117 ms |
| `partitionMasterStep` | 373.566 ms |
| `registerPartitionsStep` | 45 ms |
| `moveOriginalStep` | 111 ms |
| `publishPartitionsStep` | 992 ms |

Observações:

* **O gargalo é o Azurite, não o particionador.** Durante o particionamento, o container do Azurite
  ficou entre 310% e **356% de CPU**, enquanto o partitioner ficou em 124% do limite de 200%. O
  particionador passa a maior parte do tempo esperando I/O.
* A geração também caiu para 83 MB/s (era 151 MB/s no 50MM), e a geração usa um caminho de código
  totalmente diferente do particionamento. Duas cargas independentes desacelerando na mesma
  proporção confirmam que o limite é do armazenamento local.
* O consumo de memória segue estável (1.779 MB), sem relação com os 28 GB do arquivo.
* Todos os steps fora do particionamento continuam na casa de milissegundos.

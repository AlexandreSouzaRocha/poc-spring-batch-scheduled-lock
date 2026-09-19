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
| 250MM | 35,16 GB | 488 s | 484,5 s | 74,3 | 486,6 s | 10 | 10 | 1749 MB | 123 GB | COMPLETED |

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

### 250MM — 35,16 GB

```bash
RESULTS_FILE=benchmarks/load-test-250MM.md ./scripts/load-test.sh 250
```

Resultado em [benchmarks/load-test-250MM.md](../benchmarks/load-test-250MM.md).

| Métrica | Valor |
|---|---|
| Linhas | 250.000.000 |
| Bytes do original | 37.750.000.019 |
| Geração no blob | 488 s (≈77 MB/s) |
| `partitionMasterStep` | **484.453 ms** — 516.046 linhas/s, **74,31 MB/s** |
| `filePartitionJob` | 486.565 ms — 73,99 MB/s |
| Tentativas | 1 |
| Mensagens no Kafka | 10 |
| Pico de memória do container | 1.749 MB de 2 GB |
| Disco livre mínimo | 123 GB |
| Pico de CPU do Azurite | 330% |

Observações:

* O maior volume do teste passou na primeira tentativa, sem retentativa e sem erro de transação do
  Mongo, com 70,3 GB ocupados no blob ao final (35,16 GB em `aberto/` e 35,16 GB em `processados/`).
* **O throughput estabilizou:** 77,1 MB/s no 200MM contra 74,3 MB/s no 250MM. A queda forte acontece
  entre 50MM e 200MM e depois encosta num piso, que é a capacidade do Azurite local.
* O pico de memória (1.749 MB) é praticamente o mesmo do 50MM (1.694 MB), com um arquivo 5 vezes
  maior. O particionamento não carrega o arquivo em memória em nenhum momento.

## Conclusões da bateria de baseline

| Volume | Arquivo | Particionamento | MB/s | Linhas/s | Pico mem |
|---|---|---|---|---|---|
| 50MM | 7,03 GB | 31,8 s | 226,7 | 1.574.258 | 1.694 MB |
| 100MM | 14,06 GB | 100,3 s | 143,5 | 996.741 | 1.722 MB |
| 200MM | 28,13 GB | 373,6 s | 77,1 | 535.381 | 1.779 MB |
| 250MM | 35,16 GB | 484,5 s | 74,3 | 516.046 | 1.749 MB |

1. **Todos os volumes completam na primeira tentativa**, com 10 partições íntegras, 10 mensagens no
   Kafka e o original movido para `processados/`. Nenhum erro de transação do Mongo mesmo com o
   particionamento levando 8 minutos, porque nenhum I/O acontece dentro de transação.
2. **A memória não acompanha o volume.** De 7 GB para 35 GB de arquivo, o pico variou 3%. O consumo
   vem dos buffers de upload, não do tamanho do arquivo.
3. **O throughput cai até ~75 MB/s e estabiliza.** O limite é o Azurite: 330–356% de CPU no
   emulador contra 124% de 200% no particionador, e a geração de massa — caminho de código
   independente — degradou na mesma proporção (151 → 77 MB/s).
4. **O tempo fora do particionamento é constante e irrelevante:** validação, cleanup, registro no
   Mongo, move e publicação somam menos de 2 s em qualquer volume, porque o move é server-side no
   blob e o registro no Mongo é uma transação curta.

## Segunda bateria: paralelismo por partição

Depois do baseline, três mudanças entraram juntas: **4 threads por partição**, **ambiente com mais
folga** (4 GB e 4 CPUs no partitioner, uma única instância) e **Azurite com `UV_THREADPOOL_SIZE=16`**
no lugar do padrão 4 do Node.

```bash
PARTITION_COUNT=10 PARTITION_THREADS=4 PARTITIONER_MEMORY=4g PARTITIONER_CPUS=4 \
  PARTITIONER_INSTANCES=1 AZURITE_THREADS=16 \
  RESULTS_FILE=benchmarks/load-test-10x4.md ./scripts/load-test.sh 50 100 200 250
```

| Volume | Baseline 10×1, 2 GB | 10×4, 4 GB, solo | Tempo | Throughput |
|---|---|---|---|---|
| 50MM | 31,8 s · 226,7 MB/s | 36,0 s · 199,9 MB/s | +13% | −12% |
| 100MM | 100,3 s · 143,5 MB/s | 69,1 s · 208,4 MB/s | **−31%** | +45% |
| 200MM | 373,6 s · 77,1 MB/s | 135,7 s · 212,3 MB/s | **−64%** | +175% |
| 250MM | 484,5 s · 74,3 MB/s | 157,3 s · 228,9 MB/s | **−68%** | +208% |

**A degradação por volume desapareceu.** O baseline perdia metade do throughput a cada dobra de
volume (226,7 → 143,5 → 77,1 → 74,3 MB/s); a configuração nova se mantém entre 200 e 229 MB/s, com
o *maior* throughput no *maior* arquivo. A queda anterior era sintoma de I/O sequencial esperando
latência, não de falta de banda.

Em 50MM a configuração nova é mais lenta: o arquivo é pequeno o bastante para o Azurite responder
de cache, e as 40 conexões simultâneas só adicionam disputa. O paralelismo interno paga a partir de
100MM.

O gargalo trocou de lado: no baseline o Azurite ficava em 356% de CPU contra 124% (de 200%) do
partitioner; com 4 subthreads o partitioner chega a 352% (de 400%) e o Azurite fica em 242–247%.

### Controle: separando ambiente de paralelismo

Como três variáveis mudaram juntas, uma terceira execução isolou o efeito, mantendo o ambiente novo
e voltando para 1 thread por partição:

```bash
PARTITION_COUNT=10 PARTITION_THREADS=1 PARTITIONER_MEMORY=4g PARTITIONER_CPUS=4 \
  PARTITIONER_INSTANCES=1 AZURITE_THREADS=16 \
  RESULTS_FILE=benchmarks/load-test-10x1-controle.md ./scripts/load-test.sh 100 250
```

| Config (100MM) | Tempo | MB/s | Pico mem | Ganho |
|---|---|---|---|---|
| Baseline: 2 GB, 2 instâncias, Azurite com 4 threads de I/O | 100,3 s | 143,5 | 1.722 MB | — |
| Ambiente novo, ainda 10×1 | 77,2 s | 186,5 | 1.804 MB | +30% (ambiente) |
| Ambiente novo + 4 subthreads | 69,1 s | 208,4 | 3.288 MB | +12% (paralelismo) |

| Config (250MM) | Tempo | MB/s | Ganho acumulado |
|---|---|---|---|
| Baseline: 2 GB, 2 instâncias, Azurite com 4 threads de I/O | 484,5 s | 74,3 | — |
| Ambiente novo, ainda 10×1 | 199,9 s | 180,1 | 2,42× |
| Ambiente novo + 4 subthreads | 157,3 s | 228,9 | **3,08×** |

No 100MM, dois terços do ganho vêm do ambiente — sobretudo do threadpool do Azurite — e um terço das
subthreads. No 250MM as subthreads pesam mais: **+27%** sobre o ambiente já ajustado, contra +12% no
100MM. Quanto maior o arquivo, mais o paralelismo interno rende, que é o comportamento esperado de
um processo limitado por latência de I/O.

O custo do paralelismo é memória: 3.288 MB contra 1.804 MB no 100MM, porque são
`partições × threads × bloco` buffers simultâneos. Os 4 GB de container só são necessários por causa
dele.

### O que levar para produção

| Ajuste | Ganho medido | Custo |
|---|---|---|
| `UV_THREADPOOL_SIZE` do Azurite (4 → 16) | grande parte do 2,42× | nenhum — só vale para o emulador local |
| `app.partition.threads-per-partition` = 4 | +27% no 250MM, +12% no 100MM | memória: `partições × threads × bloco` |
| Container com 4 GB | viabiliza o item acima | — |

O `UV_THREADPOOL_SIZE` é uma característica do **Azurite**, não do Azure Storage: em produção esse
gargalo não existe, então o ganho equivalente deve vir de graça. Já as subthreads valem em qualquer
backend, e tendem a render mais contra o Azure real, que distribui a carga entre vários nós.

## Incidente: degradação do ambiente entre baterias

Durante a sessão de tuning, a mesma configuração que havia medido 157,3 s passou a medir
**2.069,7 s** — 13 vezes mais lenta. A investigação descartou o código e isolou o ambiente:

| Evidência | Conclusão |
|---|---|
| `git diff` da refatoração | Apenas nome de classe, `@Override` e a interface; nenhuma mudança funcional |
| CPU do partitioner em 80% de 400% e do Azurite em 230% | Ninguém processando: todos esperando I/O |
| Geração (escrita sequencial) normal em 255 s | O problema é leitura concorrente, não escrita |
| `Docker.raw` em 113 GB, host com 106 GB livres | Pressão de disco acumulada por ~500 GB escritos e apagados na sessão |
| Após `docker compose down -v` + prune: `Docker.raw` em 52 GB, host com 166 GB | Ambiente recuperado |
| Nova medição da mesma configuração | **150,7 s** — desempenho integralmente restaurado |

Duas lições operacionais:

1. **O tempo de geração não é um controle confiável do ambiente.** Houve uma execução com geração
   normal (255 s) e particionamento catastrófico (2.069 s). Escrita sequencial e leitura concorrente
   degradam de forma independente.
2. **Toda medição precisa começar de disco recuperado.** O `down -v` que o `reset-data.sh` faz entre
   execuções não impediu o acúmulo ao longo da sessão; foi preciso `down -v` com prune e verificar o
   espaço livre no host antes de confiar em um número.

Uma comparação ficou comprometida por esse efeito: a execução do Azurite 3.37 aconteceu com o disco
já degradado, então **a diferença de desempenho entre as versões 3.35 e 3.37 não está estabelecida**
por esses dados. Por isso a fase 2 compara streaming e cópia server-side **dentro** da 3.37, onde o
delta mede a técnica de cópia e não a versão do emulador.

## Tuning: o que mexer e o que não mexer

Depois de descobrir que a máquina na bateria distorcia as medições, a bateria de tuning foi refeita
com o host na tomada. Três réplicas da mesma configuração deram 153,5 s, 153,2 s e 146,1 s —
**dispersão de 5,1%**, contra 31% na bateria. Só a partir daí as comparações abaixo passaram a
significar alguma coisa; qualquer diferença menor que ~5% continua sendo ruído.

Todas as execuções: 250MM, 10 partições × 4 threads, 4 GB, 4 vCPUs, instância única, OTEL ligado,
Azurite 3.35.

| Variação | Particionamento | vs. melhor |
|---|---|---|
| **10 partições, bloco 8 MB, G1** | **153,2 s** (mediana de 3) | — |
| 8 partições, bloco 8 MB | 154,4 s | igual (0,8%) |
| Bloco de 4 MB | 161,2 s | −5% |
| Bloco de 16 MB | 170,3 s | −11% |
| 15 partições | 208,6 s | −36% |
| OTEL desligado | 143,4 s | +6% |

Conclusões:

* **Mais partições não ajudam.** Entre 8 e 10 não há diferença mensurável, e 15 degrada 36%: com
  60 streams simultâneos o Azurite vira gargalo de requisições. O `app.partition.count` foi mantido
  em 10.
* **O bloco de 8 MB é ótimo nos dois sentidos.** Diminuir para 4 MB aumenta o número de requisições;
  aumentar para 16 MB multiplica a memória transiente e a pressão de GC. Os dois lados pioram.
* **O agente do OpenTelemetry custa 6%.** Como produção roda com collectors ativos, a configuração
  recomendada mantém o agente ligado e o número oficial é o com OTEL.
* **O G1 vence pelo perfil de pausa, não pelo relógio.** A diferença de tempo total contra o
  ParallelGC (3–5%) está dentro do ruído, mas os logs de GC medem o fenômeno diretamente e não
  dependem de comparar execuções.

### G1 contra ParallelGC

Mesma configuração, mudando apenas o coletor:

| GC | Tempo | Pausas | Total STW | Full GCs | Pausa máxima |
|---|---|---|---|---|---|
| ParallelGC | 170,3 s | 432 | 8.123 ms | **67** | 144,6 ms |
| **G1GC** | 161,0 s | 686 | **4.252 ms** | **8** | **79,4 ms** |

O G1 faz mais pausas, porém muito mais curtas: metade do stop-the-world total e praticamente nenhum
Full GC. Em uma execução com bloco de 16 MB e 15 partições, o ParallelGC chegou a **244 Full GCs**
somando 15,5 s — quase todas as pausas do job. O G1 é o coletor recomendado.

### Configuração recomendada

| Parâmetro | Valor | Motivo |
|---|---|---|
| `app.partition.count` | `10` | 8 e 10 empatam; 15 degrada 36% |
| `app.partition.threads-per-partition` | `4` | 3,08× sobre 1 thread — o maior efeito medido |
| `app.blob.upload-block-size-mb` | `8` | Ótimo; 4 MB e 16 MB pioram |
| GC | `G1` | Metade do stop-the-world e 8 Full GCs contra 67 |
| Memória do container | `4 GB` | `partições × threads × bloco` = 320 MB de buffers + heap |
| CPUs do container | `4` | Teto disponível em produção |
| OpenTelemetry | ligado | Cenário real; custa 6% |

## Fase 2: cópia server-side (`Put Block From URL`)

A operação `Put Block From URL` faz o **serviço de blob** copiar a faixa de bytes da origem direto
para o bloco de destino, sem os bytes passarem pela aplicação. Ela exige Azurite 3.37, então a
comparação foi feita **dentro da mesma versão**, para não misturar o efeito da técnica com o da
versão do emulador.

250MM, 10 partições × 4 threads, bloco 8 MB, G1, OTEL on, Azurite 3.37:

| Métrica | Streaming | Server-side | Diferença |
|---|---|---|---|
| Particionamento | 451,8 s | 472,0 s | −4,5% (empate dentro do ruído) |
| **Pico de memória do container** | 3.486 MB | **945 MB** | **−73%** |
| **CPU do particionador** | 231% | **13%** | **−94%** |
| Pausas de GC | 4.372 ms | **148 ms** em 29 pausas | −97% |
| CPU do Azurite | 304% | 249% | −18% |

**O tempo de parede empata, mas o consumo da aplicação desaba.** O empate tem explicação: o Azurite
busca a origem por *loopback*, então o serviço continua lendo e escrevendo os mesmos 70 GB, só que
internamente. O que muda é quem faz o trabalho — a aplicação deixa de manipular bytes e passa
apenas a orquestrar identificadores de bloco.

Consequência para produção:

* O container deixa de precisar de 4 GB e 4 vCPUs. Com 13% de CPU e menos de 1 GB, ele volta ao
  patamar de um serviço de orquestração — relevante em cluster compartilhado.
* Contra o Azure Storage real, onde a cópia server-side é um caminho nativo otimizado e não um
  loopback de emulador, o tempo de parede tende a melhorar também. **Isso o emulador não consegue
  demonstrar**, e é a medição que fica pendente para o ambiente real.

O flag `app.partition.server-side-copy` continua `false` por padrão: localmente não há ganho de
tempo, e a versão do Azurite que suporta a operação é mais lenta no geral (ver abaixo).

### Azurite 3.35 contra 3.37: não foi possível concluir

As execuções na 3.37 mediram ~3× menos throughput que a 3.35 tomada 18 minutos antes, o que sugeria
uma regressão de versão. Um teste **A-B-A** — voltar para a 3.35 logo depois, na mesma configuração —
derrubou essa leitura:

| Ordem | Azurite | Geração | Particionamento |
|---|---|---|---|
| 1 | 3.35 | 249 s | 147,9 s |
| 2 | 3.37 | 536 s | 451,8 s |
| 3 | 3.37 | 534 s | 472,0 s |
| 4 | **3.35** | **1.178 s** | **187,7 s** |

A quarta execução, na versão supostamente rápida, teve a **geração mais lenta de todas**. Como disco
e energia estavam em melhor estado que durante a execução rápida (`Docker.raw` em 82 GB contra
90 GB, host com 136 GiB livres contra 126 GiB, máquina na tomada), nenhuma das duas explicações
candidatas se sustenta.

As duas cargas degradaram de formas diferentes, o que é em si um achado: a **geração** piorou 4,7×
(249 → 1.178 s) enquanto o **particionamento** piorou 27% (147,9 → 187,7 s). Geração é um fluxo
sequencial único; particionamento são 40 streams concorrentes. O ambiente penaliza muito mais o
primeiro.

Por isso a comparação de versões fica em aberto e não negada: mesmo degradada, a 3.35 particiona em
187,7 s contra 451,8 s da 3.37 — um sinal que sobrevive à degradação. Uma amostra degradada contra
duas, com ruído que chega a 4,7×, não é base para concluir.

**A conclusão honesta é sobre o instrumento, não sobre o Azurite:** o ambiente local perde entre 3 e
5× de desempenho ao longo de horas de carga pesada, sem causa identificada e sem sinal nos
indicadores óbvios. Comparar versões exigiria reiniciar o ambiente entre medições e alternar a ordem
várias vezes.

O que continua válido apesar disso:

* **Comparações feitas em janelas curtas**, com execuções consecutivas — é o caso de streaming ×
  server-side, cujas gerações levaram 536 s e 534 s, indicando ambiente estável entre elas.
* **Métricas estruturais** — memória, participação de CPU e pausas de GC — que medem *onde* o
  trabalho acontece, não a velocidade absoluta da máquina. São elas que sustentam a conclusão sobre
  a cópia server-side.
* **A bateria final**, cujas quatro medições foram tomadas em sequência numa janela de 25 minutos,
  com throughput coerente entre si (227 a 250 MB/s).

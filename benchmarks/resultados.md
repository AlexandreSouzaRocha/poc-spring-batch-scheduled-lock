# Resultados brutos dos benchmarks

Todas as execuções de carga da POC, em ordem cronológica de bateria. A análise e as conclusões
estão em [docs/LOAD-TESTS.md](../docs/LOAD-TESTS.md); aqui ficam apenas os números medidos.

Ambiente comum a tudo: Apple Silicon, Docker Desktop com 12 CPUs e 36 GB, Azurite como blob,
MongoDB com `transactionLifetimeLimitSeconds=60`, arquivo de 151 bytes por linha.

**Leia a coluna "confiança" antes de comparar duas linhas.** Réplicas da mesma configuração
variaram até 31% nesta sessão (ver [Confiabilidade](#confiabilidade) no final).

## Bateria 1 — baseline

10 partições, 1 thread por partição, 2 instâncias, 2 GB, 2 vCPUs, ParallelGC, OTEL on,
`UV_THREADPOOL_SIZE` padrão do Node (4).

| Volume | Geração | Particionamento | MB/s | Pico mem | Confiança |
|---|---|---|---|---|---|
| 50MM | 50 s | 31.761 ms | 226,70 | 1.694 MB | alta |
| 100MM | 100 s | 100.327 ms | 143,54 | 1.722 MB | alta |
| 200MM | 347 s | 373.566 ms | 77,10 | 1.779 MB | alta |
| 250MM | 488 s | 484.453 ms | 74,31 | 1.749 MB | alta |

## Bateria 2 — paralelismo por partição

10 partições, **4 threads por partição**, 1 instância, 4 GB, 4 vCPUs, ParallelGC, OTEL on,
`UV_THREADPOOL_SIZE=16`.

| Volume | Geração | Particionamento | MB/s | Confiança |
|---|---|---|---|---|
| 50MM | 67 s | 36.019 ms | 199,90 | alta |
| 100MM | 108 s | 69.091 ms | 208,43 | alta |
| 200MM | 211 s | 135.690 ms | 212,26 | alta |
| 250MM | 258 s | 157.296 ms | 228,88 | alta |

## Bateria 3 — controle: ambiente novo com 1 thread

Igual à bateria 2, mas com **1 thread por partição**. Isola o ganho do ambiente do ganho do
paralelismo interno.

| Volume | Geração | Particionamento | MB/s | Pico mem | Confiança |
|---|---|---|---|---|---|
| 100MM | 106 s | 77.218 ms | 186,49 | 1.804 MB | alta |
| 250MM | 263 s | 199.920 ms | 180,08 | 2.865 MB | alta |

## Bateria final — configuração recomendada, host na tomada

10 partições × 4 threads, bloco 8 MB, G1GC, OTEL on, 1 instância, 4 GB, 4 vCPUs,
`UV_THREADPOOL_SIZE=16`, Azurite 3.35. **Estes são os números oficiais da POC.**

| Volume | Geração | Particionamento | MB/s | Pico mem | Kafka | Confiança |
|---|---|---|---|---|---|---|
| 50MM | 52 s | 30.209 ms | 238,35 | 3.355 MB | 10 | alta |
| 100MM | 104 s | 63.325 ms | 227,41 | 3.308 MB | 10 | alta |
| 200MM | 200 s | 115.404 ms | 249,57 | 3.397 MB | 10 | alta |
| 250MM | 249 s | **147.860 ms** | 243,48 | 3.371 MB | 10 | alta |

Réplicas da mesma configuração em 250MM, para estimar a dispersão: 153.517 ms, 153.153 ms e
146.124 ms — **5,1%**. Com o host na bateria, a mesma configuração chegou a variar 31%.

A melhor marca da sessão para 250MM foi **143.041 ms (251,68 MB/s)**, medida logo após reiniciar o
Docker Desktop. Os números da tabela acima foram tomados no meio da sessão e são, portanto,
ligeiramente conservadores.

## Bateria 4 — tuning em 250MM

Base: 10 partições × 4 threads, 1 instância, 4 GB, 4 vCPUs, `UV_THREADPOOL_SIZE=16`,
Azurite 3.35 e `StreamingPartitionCopy`, salvo indicação contrária.

| Execução | GC | Bloco | Partições | OTEL | Geração | Particionamento | MB/s | Confiança |
|---|---|---|---|---|---|---|---|---|
| `otel-off` | Parallel | 8 MB | 10 | **off** | 247 s | 143.446 ms | 250,97 | baixa |
| `g1` | **G1** | 8 MB | 10 | on | 246 s | 145.957 ms | 246,66 | baixa |
| `a335-otel-on-parallel-limpo` | Parallel | 8 MB | 10 | on | 340 s | 150.650 ms | 238,97 | baixa |
| `b10-block16-g1` | **G1** | **16 MB** | 10 | on | 244 s | 161.039 ms | 223,56 | baixa |
| `b10-block16-parallel` | Parallel | **16 MB** | 10 | on | 250 s | 170.332 ms | 211,36 | baixa |
| `p15-block16` | Parallel | **16 MB** | **15** | on | 260 s | 176.388 ms | 204,10 | baixa |
| `b10-block8-g1-r2` | **G1** | 8 MB | 10 | on | 286 s | 190.958 ms | 188,53 | baixa |
| `p15` | Parallel | 8 MB | **15** | on | 253 s | 208.641 ms | 172,55 | baixa |

As duas linhas `g1` e `b10-block8-g1-r2` são **a mesma configuração**: 145.957 ms e 190.958 ms,
31% de diferença. É essa dispersão que rebaixa a confiança de toda a bateria 4.

### Execuções descartadas

| Execução | Particionamento | Motivo |
|---|---|---|
| `a335-otel-on-parallel` | 2.069.724 ms | Ambiente degradado; a mesma config mediu 150.650 ms depois da limpeza |
| `otel-on-parallel` (Azurite 3.37) | 441.054 ms | Mesmo período de degradação; não serve para comparar versões do Azurite |

## Perfis de GC

Medidos a partir do `gc.log` de cada execução, com `scripts/gc-stats.py`. Diferente dos tempos
acima, estes números medem o fenômeno diretamente e **não dependem de comparar execuções**.

| Execução | GC | Bloco | Partições | Pausas | Total STW | Full GCs | Máx |
|---|---|---|---|---|---|---|---|
| `b10-block16-g1` | **G1** | 16 MB | 10 | 686 | **4.252 ms** | **8** | 79,4 ms |
| `b10-block8-g1-r2` | **G1** | 8 MB | 10 | 620 | 4.655 ms | **3** | 108,0 ms |
| `b10-block16-parallel` | Parallel | 16 MB | 10 | 432 | 8.123 ms | 67 | 144,6 ms |
| `p15` | Parallel | 8 MB | 15 | 484 | 15.296 ms | 89 | 440,1 ms |
| `p15-block16` | Parallel | 16 MB | 15 | 248 | 15.758 ms | **244** | 166,4 ms |

Comparando as duas execuções de configuração idêntica exceto pelo coletor
(`b10-block16-g1` e `b10-block16-parallel`): o G1 reduz o stop-the-world de 8.123 ms para
4.252 ms e os Full GCs de 67 para 8.

Os logs brutos estão em [`gc/`](gc/).

## Confiabilidade

Três fatores comprometeram a precisão das medições desta sessão:

1. **Dispersão entre réplicas de até 31%** na mesma configuração. Qualquer diferença menor que
   isso — como os 3–5% entre coletores ou os 13% entre tamanhos de bloco — não está estabelecida
   pelos tempos medidos.
2. **Degradação progressiva ao longo da sessão**, com duas explicações candidatas que não foi
   possível separar: pressão de disco no host (o `Docker.raw` chegou a 113 GB) e restrição de
   desempenho do Mac em bateria (chegou a 27%, embora sem Low Power Mode ativo).
3. **O Azurite é o gargalo**, não a aplicação: durante o particionamento ele chega a 260% de CPU
   enquanto o partitioner fica abaixo de 150% do seu limite de 400%.

Por isso, as execuções passaram a registrar o estado de energia do host, e conclusões só são
tiradas de efeitos muito maiores que a dispersão — como o 3,08× da bateria 2 sobre a 1 — ou de
medições diretas, como os perfis de GC.

## Streaming × cópia server-side (Azurite 3.37)

Docker reiniciado antes de cada par; as duas estratégias medidas em sequência. 10 partições ×
4 threads, bloco 8 MB, G1, OTEL on, 4 GB, 4 vCPUs.

| Volume | Estratégia | Geração | Particionamento | MB/s | Pico mem | CPU app |
|---|---|---|---|---|---|---|
| 50MM | streaming | 105 s | 86.026 ms | 83,70 | 3.444 MB | 133% |
| 50MM | server-side | 106 s | 97.206 ms | 74,07 | **815 MB** | **16%** |
| 100MM | streaming | 209 s | 173.342 ms | 83,08 | 3.376 MB | 87% |
| 100MM | server-side | 216 s | **1.148.394 ms** | 12,54 | **836 MB** | — |
| 250MM | streaming | 536 s | 451.761 ms | 79,69 | 3.486 MB | 231% |
| 250MM | server-side | 534 s | 471.991 ms | 76,28 | **945 MB** | **13%** |

Os pares de 200MM e 250MM desta bateria foram abandonados: o tempo do server-side cresce de forma
não-linear no emulador (ver [docs/LOAD-TESTS.md](../docs/LOAD-TESTS.md)), e mediriam apenas essa
patologia. As medições de 250MM acima vêm da fase 2, feitas em janela própria.

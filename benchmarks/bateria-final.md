# Teste de carga — 2026-09-19 09:50

Energia no inicio: fonte=AC Power bateria=100% lowpower=0

Particoes: 10 · threads/particao: 4 · instancias: 1 · memoria: 4g · CPUs: 4 · azurite threads: 16 · OTEL: true

| tamanho | linhas | arquivo GB | geracao s | particao ms | particao MB/s | linhas/s | job ms | job MB/s | particoes | threads | tentativas | kafka | pico mem MB | disco livre min GB | status |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 50MM | 50000000 | 7.03 | 52 | 30209 | 238.35 | 1655136 | 31090 | 231.59 | 10 | 4 | 1 | 10 | 3355 | 180 | COMPLETED |
| 100MM | 100000000 | 14.06 | 104 | 63325 | 227.41 | 1579155 | 64257 | 224.11 | 10 | 4 | 1 | 10 | 3308 | 167 | COMPLETED |
| 200MM | 200000000 | 28.13 | 200 | 115404 | 249.57 | 1733042 | 116208 | 247.84 | 10 | 4 | 1 | 10 | 3397 | 137 | COMPLETED |
| 250MM | 250000000 | 35.16 | 249 | 147860 | 243.48 | 1690789 | 148661 | 242.17 | 10 | 4 | 1 | 10 | 3371 | 123 | COMPLETED |

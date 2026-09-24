# Testes

## Unitários e integração

```bash
make test
```

| Classe | O que cobre |
|---|---|
| `FileHeaderTest` | Parse e formatação do header posicional (os 4 tipos, padding e headers inválidos) |
| `FileLayoutTest` | Contagem de linhas pelo tamanho, arquivo truncado e linha de detalhe de 150 bytes |
| `PartitionPlanTest` | Faixas contíguas, distribuição do resto, menos linhas que partições e ida e volta pelo `ExecutionContext` |
| `BlobPathsTest` | Pastas `entrada/`, `processados/` e `<tipo>/` |
| `InboxFilesTest` | Identidade do arquivo pelo nome (mesmo nome = mesmo arquivo, independente do etag); agrupamento por tipo pelo nome do arquivo, nenhum arquivo descartado e inclusão dos não concluídos que já saíram de `entrada/` |
| `AzureBlockUploadTest` | Blocos de tamanho fixo em ordem e nenhum commit sem `commit()` |
| `ChaosRuleTest` | Regras de injeção de falha por tentativa e partição |
| `ErrorSummaryTest` | Resumo de erro sem stack trace |
| `OriginalFileRepositoryIntegrationTest` | MongoDB real (Testcontainers): fencing (a retomada revoga o dono anterior, que não renova heartbeat nem muda status; só uma execução vira dona); insert duplicado gera `DuplicateKeyException`; arquivo ativo não é reservado; `FAILED_PARTITIONING` e em andamento parado viram `REPROCESSING`; 8 reservas simultâneas com um único vencedor; tentativas esgotadas viram `FAILED`; `FAILED` só volta via requeue; heartbeat mantém o arquivo vivo e não toca arquivo encerrado |
| `ConfigurableMongoLockProviderIntegrationTest` | MongoDB real (Testcontainers): exclusão mútua com nome em `_id` e em campo customizado, collection e campos customizados, `lockAtLeastFor`, extensão só pelo dono e lock expirado assumido por outra instância |

## Teste integrado ponta a ponta

```bash
make up
make e2e LINES=5000000 FILES=2 TYPE=ABERTO
```

`scripts/e2e.sh`:

1. Pede ao **generator** os arquivos grandes, que são enviados direto para `entrada/`.
2. Aguarda o polling registrar cada arquivo e o particionamento terminar.
3. Para cada arquivo, confere via `GET /files/{id}/verification`, que lê o próprio blob:
   * status `COMPLETED` na primeira tentativa;
   * quantidade de partições no blob e no Mongo igual a `app.partition.count`;
   * soma das linhas das partições igual às linhas geradas;
   * cada partição com tamanho exato, header idêntico ao do original e `published_at` preenchido;
   * partições na pasta do tipo de movimento;
   * original em `processados/` e ausente de `entrada/`.
4. Confere que o tópico recebeu exatamente 1 mensagem por partição.
5. Roda `lock-audit.py`: ciclos por instância e **nenhuma sobreposição** entre as duas.
6. Imprime o `STEP_METRICS` e o `JOB_METRICS` de cada arquivo.

Resultado local (Apple Silicon, containers com 2 CPUs e 1 GB, Azurite):

```
2 arquivos × 5.000.000 linhas (755 MB cada), 10 partições
partitionMasterStep durationMs≈3100  mbPerSec≈230
filePartitionJob    durationMs≈3400
mensagens publicadas no Kafka = 20
overlaps=0
TODAS AS VALIDAÇÕES PASSARAM
```

## Resume e recovery (chaos)

```bash
make chaos-test SCENARIO=all          # ou partition-fail | publish-fail | invalid-file | slow-io | kill-owner | zombie-owner
LINES=2000000 ./scripts/chaos-test.sh kill-owner
```

A falha é injetada nas duas instâncias via `PUT /chaos`, porque não se sabe qual delas vai reservar
o arquivo. Um arquivo em `FAILED` não bloqueia os demais, então os cenários rodam encadeados sem
limpeza entre eles.

| Cenário | Falha injetada | Validações |
|---|---|---|
| `partition-fail` | A partição 3 falha na 1ª tentativa, depois de gravada | `COMPLETED` na 2ª tentativa; o cleanup fez **rollback** das partições da 1ª tentativa; partições íntegras; Kafka só com as mensagens da tentativa bem-sucedida |
| `publish-fail` | A publicação no Kafka falha na 1ª tentativa | `COMPLETED` na 2ª tentativa; cleanup **pulado**; cada partição gravada **uma única vez**; original movido uma única vez (resume a partir do `publishPartitionsStep`) |
| `slow-io` | 90 s de atraso no worker (`PARTITION`), no `MOVE` e no `PUBLISH`, com o Mongo limitado a 60 s de transação | `COMPLETED` na 1ª tentativa; nenhum `NoSuchTransaction`; partições íntegras e publicadas |
| `invalid-file` | Header com indicador `X` | `FAILED` sem retentativa; arquivo mantido em `entrada/`; nenhuma partição e nenhuma mensagem |
| `zombie-owner` | Uma partição fica parada por 20 s e a instância que processa o arquivo é **congelada** com `docker pause` (não morre) até a outra concluir | A outra instância retoma e conclui; ao ser descongelada, a antiga registra `file.ownership.lost`, não conclui o job, não altera o status e não publica no Kafka; partições íntegras |
| `kill-owner` | Uma partição fica parada por 300 s e a instância que processa o arquivo recebe `docker kill` | Sem heartbeat, o `updated_at` envelhece; depois de `stale-after` a outra instância reserva o arquivo em `REPROCESSING` (`file.reprocess`), marca a execução órfã como `FAILED` (`execution.recover`), refaz o particionamento e termina `COMPLETED` |

### Manualmente

```bash
make chaos POINT=PUBLISH ACTION=FAIL ATTEMPT=1
make generate LINES=1000000 TYPE=SALDO
make status
make chaos-off

make chaos POINT=PARTITION ACTION=DELAY DELAY=300 ATTEMPT=1 PARTITION=1
make generate LINES=1000000
make kill-owner
make status
make start-all
```

Pontos de falha: `VALIDATE`, `CLEANUP`, `PARTITION`, `REGISTER`, `MOVE`, `PUBLISH`. Ações: `FAIL` (lança exceção) e `DELAY` (dorme `DELAY` segundos).

## Testes de carga

```bash
# 1. reinicie o Docker Desktop  <- obrigatorio antes de cada bateria
make prune                                   # recupera cache de build e imagens orfas
make disk                                    # disco livre na VM do Docker + uso por pasta no blob
make load-test SIZES="50 100 200 250"        # benchmark, limpando o ambiente entre os tamanhos
```

> **Reinicie o Docker Desktop antes de cada bateria.** O daemon acumula degradação ao longo de horas
> de carga pesada: numa medição, a geração do arquivo de 250MM passou de 229 s para 1.178 s (5,1×)
> sem nenhuma mudança de configuração, com disco e energia em bom estado. Reiniciar o Docker
> restaurou o desempenho integralmente. Sem esse cuidado, qualquer comparação entre configurações
> mede o cansaço do ambiente em vez da mudança testada.

`scripts/load-test.sh` roda um tamanho por vez e, para cada um:

1. Calcula o espaço necessário (≈3,2 × o tamanho do arquivo: original + partições + cópia do move)
   e **aborta aquele tamanho** se o disco livre não cobrir, em vez de encher a VM do Docker.
2. Zera o ambiente com `scripts/reset-data.sh` (`docker compose down -v` + `up`), então cada
   execução começa com blob, Mongo e Kafka vazios e o disco da execução anterior é devolvido.
3. Gera o arquivo, aguarda `COMPLETED` (timeout proporcional ao tamanho) e coleta
   `STEP_METRICS`/`JOB_METRICS`, mensagens no Kafka, pico de memória dos containers
   (amostragem de `docker stats`) e o mínimo de disco livre durante a execução.
4. Escreve a linha do resultado em `benchmarks/load-test-<timestamp>.md`.

| Variável | Padrão | Uso |
|---|---|---|
| `SIZES` | `50 100 200 250` | Tamanhos em milhões de linhas |
| `OTEL_ENABLED` | `true` | `false` mede sem a sobrecarga da instrumentação |
| `PARTITION_COUNT` | `10` | Partições por arquivo |
| `PARTITIONER_MEMORY` / `PARTITIONER_CPUS` | `2g` / `2` | Limites dos containers, para tuning |
| `KEEP_DATA` | `false` | `true` não limpa entre execuções (acumula disco) |

Tamanho de arquivo por volume (151 bytes/linha): 50MM ≈ 7,0 GB · 100MM ≈ 14,1 GB ·
200MM ≈ 28,1 GB · 250MM ≈ 35,2 GB. O pico de disco é ~3× isso, então **250MM exige ~118 GB livres**.

Para mudar limites de container ou instrumentação, exporte as variáveis antes:

```bash
OTEL_ENABLED=false PARTITIONER_MEMORY=4g PARTITION_COUNT=20 make load-test SIZES="200"
```

### Limpeza e inspeção do blob

```bash
make reset-data     # zera blob/mongo/kafka e libera o disco entre execuções
make blob-usage     # GB por pasta (entrada/, processados/, aberto/ ...)
make blob-ls PREFIX=processados/
```

O Azurite não tem interface própria. Para navegar visualmente, use o **Azure Storage Explorer**
(`brew install --cask microsoft-azure-storage-explorer`) conectado ao emulador local em
`http://127.0.0.1:10000/devstoreaccount1` com a conta `devstoreaccount1` e a chave do
`docker-compose.yml`.

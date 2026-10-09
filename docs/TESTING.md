# Testes

## Unitários e integração

```bash
make test
```

| Classe | O que cobre |
|---|---|
| `LineTypeTest` | Reconhecimento de cada linha pelo indicador (`H`, `D`, `T`) e descrição de indicador desconhecido |
| `FileHeaderTest` | Parse e formatação do header posicional (os 4 tipos, padding, header maior com preenchimento e headers inválidos) |
| `FileTrailerTest` | Quantidade com 15 dígitos e data de processamento, com e sem quebra final, trailer maior completado com espaços e trailers inválidos |
| `FileLayoutTest` | Contagem de linhas descontando header e trailer (com e sem quebra final), tamanhos configurados, posição do trailer, arquivo sem detalhe e tamanhos abaixo do mínimo |
| `DetailLineScannerTest` | Conferência e contagem das linhas `D`, linhas atravessando blocos, `H`/`T` no meio, indicador desconhecido, quebra fora do lugar e CRLF |
| `PartitionPlanTest` | Faixas contíguas, distribuição do resto, menos linhas que partições e ida e volta pelo `ExecutionContext` |
| `BlobPathsTest` | Pastas `entrada/`, `processados/` e `<tipo>/` |
| `FileIntakeServiceTest` | Identidade do arquivo pelo nome (mesmo nome = mesmo arquivo, independente do etag) e inclusão dos não concluídos que já saíram de `entrada/` |
| `PartitionCycleServiceTest` | Agrupamento por tipo pelo nome do arquivo, sem descartar nenhum arquivo |
| `StreamingPartitionCopyTest` / `ServerSidePartitionCopyTest` | Cópia paralela da faixa de bytes: header + faixa + trailer idênticos com 1 a 8 threads, partição sem trailer com a flag desligada e linha `H` no meio reprovada (streaming); blocos contíguos e em ordem, trailer no último bloco e sem conferência de linhas (server-side) |
| `PartitionChunksTest` | Divisão da faixa em trechos alinhados ao bloco |
| `BlockOutputStreamTest` | Blocos de tamanho fixo em ordem e nenhum commit sem `commit()` |
| `ChaosRuleTest` | Regras de injeção de falha por tentativa e partição |
| `ChaosTaskletInterceptorTest` | Falha antes da tasklet nos pontos de step e depois dela em `PARTITION`, step normal sem regra e beans que não são tasklet intactos |
| `ErrorSummaryTest` | Resumo de erro sem stack trace |
| `OriginalFileRepositoryIntegrationTest` | MongoDB real (Testcontainers): documento com `tentativas` na raiz, status `PARTICIONANDO` e `tipo` `ORIGINAL` gravados em português e sem dados de execução; `_id` derivado do nome; nome repetido gera `DuplicateKeyException`; `PARTICIONANDO` interrompido e `FALHA_PARTICIONAMENTO` retomados com uma tentativa a mais; tentativas esgotadas viram `FALHA`; `FALHA` só volta via requeue |
| `PartitionFileRepositoryIntegrationTest` | MongoDB real com replica set (Testcontainers): `replaceAll` transacional por arquivo, partições ordenadas por índice, pendentes de publicação, `deleteByParentFileId` só do arquivo e valores gravados (`PARTICAO`, `CONCLUIDO`, `tentativas = 1`) |
| `ProcessingIndicatorRepositoryIntegrationTest` | MongoDB real (Testcontainers): campos `data_processamento_movimento_<tipo>` e `quantidade_movimento_<tipo>` sob a data do header, reprocessamento sobrescrevendo e upserts concorrentes de tipos diferentes na mesma data |
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
   * status `CONCLUIDO` na primeira tentativa;
   * quantidade de partições no blob e no Mongo igual a `app.partition.count`;
   * soma das linhas das partições igual às linhas geradas;
   * cada partição com tamanho exato, header idêntico ao do original, trailer próprio e `publicado_em` preenchido;
   * partições na pasta do tipo de movimento;
   * original em `processados/` e ausente de `entrada/`;
   * documento do arquivão removido da `gerenciamento_arquivo_recebido`;
   * `indicador_processamento` da data do header com a quantidade e a data do trailer.
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
make chaos-test SCENARIO=all          # ou partition-fail | publish-fail | invalid-file | trailer-mismatch | slow-io | kill-owner | zombie-owner
LINES=2000000 ./scripts/chaos-test.sh kill-owner
```

O chaos existe **só localmente**: tudo fica em `support/chaos` e só é carregado com o profile
`chaos`, que o `docker-compose.yml` liga nos particionadores (`SPRING_PROFILES_ACTIVE=partitioner,chaos`).
O código de produção não referencia o chaos. O `ChaosTaskletInterceptor` (um `BeanPostProcessor`)
envolve cada tasklet num proxy do `spring-aop` e aplica a regra ativa pelo nome do step: antes da
tasklet em `VALIDATE`, `CLEANUP`, `REGISTER`, `PUBLISH`, `MOVE` e `FINISH`, e depois dela em
`PARTITION`, quando a partição já foi gravada. Sem o profile não há interceptor, injetor nem a rota
`/chaos`, que responde 404.

A falha é injetada nas duas instâncias via `PUT /chaos`, porque não se sabe qual delas vai obter o
lock do tipo. Um arquivo em `FALHA` não bloqueia os demais, então os cenários rodam encadeados sem
limpeza entre eles.

| Cenário | Falha injetada | Validações |
|---|---|---|
| `partition-fail` | A partição 3 falha na 1ª tentativa, depois de gravada | `CONCLUIDO` na 2ª tentativa; o cleanup fez **rollback** das partições da 1ª tentativa; partições íntegras; Kafka só com as mensagens da tentativa bem-sucedida |
| `publish-fail` | A publicação no Kafka falha na 1ª tentativa | `CONCLUIDO` na 2ª tentativa; cleanup **pulado**; cada partição gravada **uma única vez**; original movido uma única vez (resume a partir do `publishPartitionsStep`) |
| `slow-io` | 90 s de atraso no worker (`PARTITION`), no `MOVE` e no `PUBLISH`, com o Mongo limitado a 60 s de transação | `CONCLUIDO` na 1ª tentativa; nenhum `NoSuchTransaction`; partições íntegras e publicadas |
| `invalid-file` | Gerador com `defect=HEADER` (header com indicador `X`), `defect=TRAILER` (trailer com indicador `X`) e `defect=DETAIL` (linha `H` no meio dos detalhes) | `FALHA` sem retentativa; arquivo mantido em `entrada/`; nenhuma partição registrada e nenhuma mensagem |
| `trailer-mismatch` | Gerador com `defect=COUNT` (quantidade do trailer = linhas + 1) | `CONCLUIDO`; `WARN` `file.trailer.mismatch`; partições íntegras; arquivão removido; indicador com a quantidade **do trailer** |
| `zombie-owner` | Uma partição fica parada por 20 s e a instância que processa o arquivo é **congelada** com `docker pause` (não morre) até a outra concluir | O lock do tipo expira e a outra instância retoma e conclui; ao ser descongelada, a antiga não é mais a execução corrente no JobRepository: registra `file.ownership.lost`, não conclui o job, não altera o status e não publica no Kafka; partições íntegras |
| `kill-owner` | Uma partição fica parada por 300 s e a instância que processa o arquivo recebe `docker kill` | O lock do tipo expira em até `lock-at-most-for`; a outra instância o adquire, encontra o arquivo em `PARTICIONANDO` e o retoma (`file.resume`), marca a execução órfã como `FAILED` (`execution.recover`), refaz o particionamento e termina `CONCLUIDO` |

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
3. Gera o arquivo, aguarda `CONCLUIDO` (timeout proporcional ao tamanho) e coleta
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

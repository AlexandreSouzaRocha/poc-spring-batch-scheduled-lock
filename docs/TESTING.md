# Testes

## Unitários e integração do lock

```bash
make test
```

| Classe | O que cobre |
|---|---|
| `FileHeaderTest` | Parse e formatação do header posicional (os 4 tipos, padding e headers inválidos) |
| `FileLayoutTest` | Contagem de linhas pelo tamanho, arquivo truncado e linha de detalhe de 150 bytes |
| `PartitionPlanTest` | Faixas contíguas, distribuição do resto, menos linhas que partições e ida e volta pelo `ExecutionContext` |
| `BlobPathsTest` | Pastas `entrada/`, `processados/`, `<tipo>/` e `erros/` |
| `AzureBlockUploadTest` | Blocos de tamanho fixo em ordem e nenhum commit sem `commit()` |
| `ChaosRuleTest` | Regras de injeção de falha por tentativa e partição |
| `ErrorSummaryTest` | Resumo de erro sem stack trace |
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
make chaos-test SCENARIO=all          # ou partition-fail | publish-fail | invalid-file | slow-io | kill-owner
LINES=2000000 ./scripts/chaos-test.sh kill-owner
```

A falha é injetada nas duas instâncias via `PUT /chaos`, porque não se sabe qual delas terá o lock.

| Cenário | Falha injetada | Validações |
|---|---|---|
| `partition-fail` | A partição 3 falha na 1ª tentativa, depois de gravada | `COMPLETED` na 2ª tentativa; o cleanup fez **rollback** das partições da 1ª tentativa; partições íntegras; Kafka só com as mensagens da tentativa bem-sucedida |
| `publish-fail` | A publicação no Kafka falha na 1ª tentativa | `COMPLETED` na 2ª tentativa; cleanup **pulado**; cada partição gravada **uma única vez**; original movido uma única vez (resume a partir do `publishPartitionsStep`) |
| `slow-io` | 90 s de atraso no worker (`PARTITION`), no `MOVE` e no `PUBLISH`, com o Mongo limitado a 60 s de transação | `COMPLETED` na 1ª tentativa; nenhum `NoSuchTransaction`; partições íntegras e publicadas |
| `invalid-file` | Header com indicador `X` | `ERROR` sem retentativa; arquivo em `erros/`; nenhuma partição e nenhuma mensagem |
| `kill-owner` | Uma partição fica parada por 300 s e o dono do lock recebe `docker kill` | A outra instância assume depois que o lock expira, marca a execução órfã como `FAILED` (`execution.recover`), refaz o particionamento e termina `COMPLETED` |

### Manualmente

```bash
make chaos POINT=PUBLISH ACTION=FAIL ATTEMPT=1
make generate LINES=1000000 TYPE=SALDO
make status
make chaos-off

make chaos POINT=PARTITION ACTION=DELAY DELAY=300 ATTEMPT=1 PARTITION=1
make generate LINES=1000000
make locks
make kill-owner
make status
make start-all
```

Pontos de falha: `VALIDATE`, `CLEANUP`, `PARTITION`, `REGISTER`, `MOVE`, `PUBLISH`. Ações: `FAIL` (lança exceção) e `DELAY` (dorme `DELAY` segundos).

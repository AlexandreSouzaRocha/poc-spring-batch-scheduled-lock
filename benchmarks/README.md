# Benchmarks

Evidência bruta das medições de carga. A análise, as conclusões e a configuração recomendada estão
em [docs/LOAD-TESTS.md](../docs/LOAD-TESTS.md) — este diretório existe para conferir os números.

| Arquivo | Conteúdo |
|---|---|
| [resultados.md](resultados.md) | Todas as execuções, com a configuração de cada uma e uma coluna de confiança |
| [gc/resumo.md](gc/resumo.md) | Pausas de GC por execução, já processadas |
| `gc/*.log` | Logs brutos de GC, um por container (ou seja, um por execução) |

## Como reproduzir

Os padrões do `docker-compose.yml` **já são a configuração recomendada**, então basta:

```bash
# 1. reinicie o Docker Desktop — obrigatório, ver docs/TESTING.md
make load-test SIZES="250"
```

Use uma única instância para medir sem interferência do paralelismo entre elas:

```bash
PARTITIONER_INSTANCES=1 make load-test SIZES="50 100 200 250"
```

Para analisar o GC de uma execução:

```bash
python3 scripts/gc-stats.py benchmarks/gc/gc-<timestamp>.log
```

## Antes de comparar dois números

1. **Reinicie o Docker Desktop antes de cada bateria.** O daemon acumula degradação: numa medição, a
   mesma carga passou de 229 s para 1.178 s sem nenhuma mudança de configuração.
2. **Compare apenas execuções próximas no tempo.** A dispersão entre réplicas idênticas foi de 5%
   com o host na tomada e chegou a 31% com o host na bateria.
3. **Confie mais nas métricas estruturais** — memória, participação de CPU, pausas de GC — do que no
   tempo de parede. Elas medem onde o trabalho acontece, não a velocidade da máquina no momento.

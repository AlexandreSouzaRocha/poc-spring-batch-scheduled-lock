GENERATOR_URL  ?= http://localhost:8090
P1_URL         ?= http://localhost:8081
P2_URL         ?= http://localhost:8082
PARTITIONERS   := partitioner-1 partitioner-2
APPS           := generator $(PARTITIONERS)
LINES          ?= 1000000
FILES          ?= 1
TYPE           ?= ABERTO
DATE           ?= $(shell date +%Y-%m-%d)
ID             ?=
INVALID        ?= false
POINT          ?= PARTITION
ACTION         ?= FAIL
ATTEMPT        ?= 1
PARTITION      ?=
DELAY          ?= 90
SCENARIO       ?= all
SIZES          ?= 50 100 200 250
SINCE          ?= 24h
TOPIC          := movimentos-particionados
BLOB_ACCOUNT   := devstoreaccount1
BLOB_KEY       := Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
BLOB_ENDPOINT  := http://azurite:10000/devstoreaccount1
BLOB_CONTAINER := movimentos
PREFIX         ?=
AZ_CLI         := docker run --rm --network poc-partitioner_default mcr.microsoft.com/azure-cli:latest az
MVN            := ./mvnw
JSON           := python3 -m json.tool

.DEFAULT_GOAL := help

.PHONY: help up up-infra infra-init down restart ps logs app-logs build test run generate status file verify locks lock-check \
	metrics metrics-partitions prometheus kafka-count kafka-tail blob-ls chaos chaos-off kill-owner start-all \
	e2e chaos-test load-test reset-data prune disk blob-usage clean

help: ## Lista os alvos disponíveis
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

## ---------- Stack ----------
up: ## Build da imagem + sobe infra, generator (:8090) e partitioner-1/2 (:8081/:8082)
	docker compose up -d --build --wait
	@echo "kafka-ui: http://localhost:8080 | generator: $(GENERATOR_URL) | partitioners: $(P1_URL) $(P2_URL)"

up-infra: ## Sobe só a infra (kafka, mongo, azurite) e roda os inits, para rodar a app local com 'make run'
	docker compose up -d --wait kafka kafka-ui mongo azurite
	docker compose up mongo-init kafka-init azurite-init

infra-init: ## Reexecuta os scripts de init da infra (collections, índices, tópico e container do blob)
	docker compose up --force-recreate mongo-init kafka-init azurite-init

down: ## Derruba a stack e remove volumes (blob, mongo)
	docker compose down -v

restart: down up ## Recria a stack do zero

ps: ## Status dos containers
	docker compose ps

logs: ## Segue os logs de toda a stack
	docker compose logs -f

app-logs: ## Segue os logs do generator e dos particionadores
	docker compose logs -f $(APPS)

## ---------- Build/testes locais ----------
build: ## Compila e empacota (sem testes)
	$(MVN) -B -DskipTests package

test: ## Testes unitários + integração do lock (Testcontainers/MongoDB)
	$(MVN) -B test

run: ## Sobe a app local com os profiles partitioner+generator (use com 'make up-infra')
	$(MVN) spring-boot:run

## ---------- Operação ----------
generate: ## Gera arquivo(s) grande(s) no blob. LINES=1000000 FILES=1 TYPE=ABERTO|FECHADO|SALDO|ULTIMA DATE=yyyy-MM-dd INVALID=false
	curl -fsS -X POST "$(GENERATOR_URL)/generator/files?lines=$(LINES)&files=$(FILES)&movementType=$(TYPE)&movementDate=$(DATE)&invalidHeader=$(INVALID)" | $(JSON)

status: ## Resumo por status + últimos arquivos grandes
	@curl -fsS "$(P1_URL)/files/summary" | $(JSON)
	@curl -fsS "$(P1_URL)/files?limit=10" | python3 scripts/status.py

file: ## Detalhe de um arquivo e suas partições. ID=<fileId>
	curl -fsS "$(P1_URL)/files/$(ID)" | $(JSON)

verify: ## Verifica no blob as partições de um arquivo (existência, tamanho, header, publicação). ID=<fileId>
	curl -fsS "$(P1_URL)/files/$(ID)/verification" | $(JSON)

locks: ## Documentos de lock do ShedLock (dono e validade)
	curl -fsS "$(P1_URL)/locks" | $(JSON)

lock-check: ## Ciclos executados por instância e verificação de sobreposição entre elas. SINCE=24h
	@python3 scripts/lock-audit.py $(SINCE)

metrics: ## JOB_METRICS e STEP_METRICS (sem as partições individuais) dos particionadores
	@docker compose logs --no-log-prefix $(PARTITIONERS) 2>&1 | grep -E "job.metrics|step.metrics" | grep -v "step=partitionWorkerStep" || echo "nenhuma execução ainda"

metrics-partitions: ## STEP_METRICS de cada partição (partitionWorkerStep)
	@docker compose logs --no-log-prefix $(PARTITIONERS) 2>&1 | grep "step=partitionWorkerStep" || echo "nenhuma execução ainda"

prometheus: ## Timers partitioner.* expostos no /actuator/prometheus das duas instâncias
	@for url in $(P1_URL) $(P2_URL); do echo "== $$url"; curl -fsS "$$url/actuator/prometheus" | grep "^partitioner_" || true; done

kafka-count: ## Total de mensagens no tópico de saída
	@docker exec psl-kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic $(TOPIC) | awk -F: '{s+=$$3} END {print "mensagens no tópico $(TOPIC): " s+0}'

kafka-tail: ## Mostra as mensagens publicadas (key + value)
	docker exec psl-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic $(TOPIC) \
		--from-beginning --property print.key=true --timeout-ms 5000 || true

blob-usage: ## Tamanho ocupado por pasta no blob (entrada/, processados/, aberto/ ...)
	@bash -c 'source scripts/lib.sh && blob_usage'

blob-ls: ## Lista blobs do container (PREFIX=entrada/ | processados/ | aberto/ ...)
	$(AZ_CLI) storage blob list --account-name $(BLOB_ACCOUNT) --account-key "$(BLOB_KEY)" \
		--blob-endpoint "$(BLOB_ENDPOINT)" --container-name $(BLOB_CONTAINER) --prefix "$(PREFIX)" \
		--query "[].{name:name, bytes:properties.contentLength}" --output table

## ---------- Resiliência ----------
chaos: ## Injeta falha nos 2 particionadores. POINT=VALIDATE|CLEANUP|PARTITION|MOVE|PUBLISH ACTION=FAIL|DELAY ATTEMPT=1 PARTITION= DELAY=90
	@for url in $(P1_URL) $(P2_URL); do \
		curl -fsS -X PUT "$$url/chaos?point=$(POINT)&action=$(ACTION)&onAttempt=$(ATTEMPT)&delaySeconds=$(DELAY)$(if $(PARTITION),&partitionIndex=$(PARTITION),)"; echo; \
	done

chaos-off: ## Remove a injeção de falha dos 2 particionadores
	@for url in $(P1_URL) $(P2_URL); do curl -fsS -X DELETE "$$url/chaos" || true; done; echo "chaos desativado"

kill-owner: ## docker kill na instância que iniciou o particionamento mais recente (simula crash)
	@owner=$$(for c in $(PARTITIONERS); do \
		ts=$$(docker logs --since 30m psl-$$c 2>&1 | grep "operation=file.process" | tail -1 | awk '{print $$1}'); \
		[ -n "$$ts" ] && echo "$$ts $$c"; \
	done | sort | tail -1 | awk '{print $$2}'); \
	[ -n "$$owner" ] || { echo "nenhum particionamento recente encontrado"; exit 1; }; \
	echo "instância processando: $$owner"; docker kill psl-$$owner

start-all: ## Sobe de novo containers parados/mortos
	docker compose up -d --wait $(APPS)

e2e: ## Teste integrado: gera, aguarda e valida partições, blob, Kafka, lock e métricas. LINES FILES TYPE
	./scripts/e2e.sh $(LINES) $(FILES) $(TYPE)

chaos-test: ## Cenários de resume/recovery: SCENARIO=partition-fail|publish-fail|invalid-file|slow-io|kill-owner|zombie-owner|all
	./scripts/chaos-test.sh $(SCENARIO)

## ---------- Testes de carga ----------
load-test: ## Benchmark por tamanho, limpando o ambiente entre execucoes. SIZES="50 100 200 250" TYPE=ABERTO
	./scripts/load-test.sh $(SIZES)

reset-data: ## Zera blob, mongo e kafka (down -v + up) e libera o disco usado pela execucao anterior
	./scripts/reset-data.sh

disk: ## Disco livre na VM do Docker, uso por pasta no blob e espaco ocupado pelo Docker
	@bash -c 'source scripts/lib.sh && disk_free_report && blob_usage'
	@docker system df

prune: ## Recupera cache de build e imagens orfas (nao toca em volumes de outros projetos)
	docker builder prune -f
	docker image prune -f
	@bash -c 'source scripts/lib.sh && disk_free_report'

clean: ## Limpa o build local
	$(MVN) clean

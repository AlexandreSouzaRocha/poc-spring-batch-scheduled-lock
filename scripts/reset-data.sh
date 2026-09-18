#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

info "derrubando a stack e removendo os volumes de dados (azurite + mongo)"
docker compose down -v --remove-orphans

info "removendo cache de build e imagens orfas deste projeto"
docker builder prune -f >/dev/null
docker image prune -f >/dev/null

info "subindo a stack novamente com $PARTITIONER_INSTANCES particionador(es)"
docker compose up -d --wait kafka kafka-ui mongo azurite generator partitioner-1
if [ "$PARTITIONER_INSTANCES" -gt 1 ]; then
  docker compose up -d --wait partitioner-2
fi

wait_healthy $(app_urls)
info "ambiente limpo e pronto"
disk_free_report

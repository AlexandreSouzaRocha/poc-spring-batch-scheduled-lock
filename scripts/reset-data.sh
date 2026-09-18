#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

info "derrubando a stack e removendo os volumes de dados (azurite + mongo)"
docker compose down -v --remove-orphans

info "removendo cache de build e imagens orfas deste projeto"
docker builder prune -f >/dev/null
docker image prune -f >/dev/null

info "subindo a stack novamente (inits recriam collections, indices, topico e container)"
docker compose up -d --wait

wait_healthy "$GENERATOR_URL" "$P1_URL" "$P2_URL"
info "ambiente limpo e pronto"
disk_free_report

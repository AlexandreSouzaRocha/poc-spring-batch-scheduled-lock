#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

LINES=${LINES:-5000000}
TODAY=$(date +%Y-%m-%d)
YESTERDAY=$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d "yesterday" +%Y-%m-%d)

info "aguardando instâncias e fila vazia"
wait_healthy $(app_urls)
chaos_off
wait_idle

info "parando os particionadores para a fila acumular antes do primeiro poll"
docker compose stop "${PARTITIONERS[@]}" >/dev/null 2>&1

START=$(now_utc)

info "gerando 4 arquivos de $LINES linhas fora de ordem: SALDO, ABERTO, FECHADO(hoje), FECHADO(ontem)"
DATE=$TODAY generate "$LINES" 1 SALDO >/dev/null
DATE=$TODAY generate "$LINES" 1 ABERTO >/dev/null
DATE=$TODAY generate "$LINES" 1 FECHADO >/dev/null
DATE=$YESTERDAY generate "$LINES" 1 FECHADO >/dev/null

info "subindo os particionadores com a fila já formada"
docker compose start "${PARTITIONERS[@]}" >/dev/null
wait_healthy $(app_urls)

info "aguardando o registro dos quatro arquivos"
wait_registered 4

info "aguardando o processamento dos quatro arquivos"
wait_idle

info "ordem da fila registrada no despacho"
DISPATCH=$(logs_since "$START" | grep "operation=queue.dispatch" | grep "files=4 " | head -1)
ORDER=$(echo "$DISPATCH" | python3 "$(dirname "$0")/queue-order.py")
echo "$ORDER" | sed 's/^/  /'

EXPECTED="FECHADO $YESTERDAY
FECHADO $TODAY
ABERTO $TODAY
SALDO $TODAY"
check "fila ordenada por tipo e depois por data" "$([ "$ORDER" = "$EXPECTED" ] && echo true || echo false)"

FECHADO_ONTEM=$(file_id_of "FECHADO" "$YESTERDAY")
FECHADO_HOJE=$(file_id_of "FECHADO" "$TODAY")
check "o FECHADO de $YESTERDAY concluiu antes do de $TODAY" \
  "$(completed_before "$FECHADO_ONTEM" "$FECHADO_HOJE")"

check "os quatro arquivos terminaram em COMPLETED" \
  "$(curl -fsS "$(partitioner_url)/files?limit=4" \
      | python3 -c 'import json,sys; print(str(all(f["status"]=="COMPLETED" for f in json.load(sys.stdin))).lower())')"

info "locks adquiridos por instância"
for app in "${PARTITIONERS[@]}"; do
  docker logs --since "$START" "psl-$app" 2>&1 | grep -oE "operation=lock.acquired lock=[a-z-]+" \
    | sed "s/operation=lock.acquired //" | sort | uniq -c | sed "s/^/  $app: /"
done

INSTANCES_WITH_TYPE_LOCK=$(for app in "${PARTITIONERS[@]}"; do
  instance_logs=$(docker logs --since "$START" "psl-$app" 2>&1 || true)
  if echo "$instance_logs" | grep -Eq "operation=lock.acquired lock=file-processing-(fechado|aberto|saldo|ultima)"; then
    echo "$app"
  fi
done | wc -l | tr -d ' ')
check "as duas instâncias processaram tipos de movimento (paralelismo)" \
  "$([ "$INSTANCES_WITH_TYPE_LOCK" -ge 2 ] && echo true || echo false)"

info "auditoria de lock"
AUDIT=$(python3 "$(dirname "$0")/lock-audit.py" "$START")
echo "$AUDIT" | sed 's/^/  /'
check "nenhum lock mantido por duas instâncias ao mesmo tempo" \
  "$(echo "$AUDIT" | grep -q '^overlaps=0$' && echo true || echo false)"

finish

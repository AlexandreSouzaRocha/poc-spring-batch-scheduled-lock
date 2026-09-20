#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

LINES=${LINES:-300000}
TODAY=$(date +%Y-%m-%d)
YESTERDAY=$(date -v-1d +%Y-%m-%d)

wait_healthy $(app_urls); chaos_off; wait_idle
info "parando particionadores para formar a fila"
docker compose stop "${PARTITIONERS[@]}" >/dev/null 2>&1
START=$(now_utc)

info "gerando: ULTIMA(hoje), ABERTO(hoje), ULTIMA(ontem), ABERTO(ontem), FECHADO(ontem)"
DATE=$TODAY     generate "$LINES" 1 ULTIMA  >/dev/null
DATE=$TODAY     generate "$LINES" 1 ABERTO  >/dev/null
DATE=$YESTERDAY generate "$LINES" 1 ULTIMA  >/dev/null
DATE=$YESTERDAY generate "$LINES" 1 ABERTO  >/dev/null
DATE=$YESTERDAY generate "$LINES" 1 FECHADO >/dev/null

docker compose start "${PARTITIONERS[@]}" >/dev/null
wait_healthy $(app_urls)
wait_registered 5
wait_idle

info "despachos por ciclo (data e ordem)"
logs_since "$START" | grep "operation=queue.dispatch" \
  | grep -oE 'data=[0-9-]+|"ordem":\[[^]]*\]' | paste - - | sed -E 's/MOV_([A-Z]+)_[0-9.]+\.txt/\1/g' | sed 's/^/  /'

info "ordem de conclusao"
curl -fsS "$(partitioner_url)/files?limit=5" | python3 -c '
import json, sys
for e in sorted(json.load(sys.stdin), key=lambda x: x["audit"]["completedAt"]):
    m = e["movement"]
    print("  {:8} {}  {}".format(m["type"], m["date"], e["audit"]["completedAt"][11:23]))'

ULTIMA_ONTEM=$(file_id_of ULTIMA "$YESTERDAY")
ABERTO_ONTEM=$(file_id_of ABERTO "$YESTERDAY")
ABERTO_HOJE=$(file_id_of ABERTO "$TODAY")
check "ABERTO de ontem concluiu antes do ULTIMA de ontem (dependencia)" \
  "$(completed_before "$ABERTO_ONTEM" "$ULTIMA_ONTEM")"
check "ULTIMA de ontem concluiu antes do ABERTO de hoje (barreira de data)" \
  "$(completed_before "$ULTIMA_ONTEM" "$ABERTO_HOJE")"
check "os cinco arquivos terminaram em COMPLETED" \
  "$(curl -fsS "$(partitioner_url)/files?limit=5" | python3 -c 'import json,sys; print(str(all(f["status"]=="COMPLETED" for f in json.load(sys.stdin))).lower())')"

info "locks adquiridos por instância"
for app in "${PARTITIONERS[@]}"; do
  instance_logs=$(docker logs --since "$START" "psl-$app" 2>&1 || true)
  echo "$instance_logs" | grep -oE "operation=lock.acquired lock=[a-z-]+" | sed "s/operation=lock.acquired //" \
    | sort | uniq -c | sed "s/^/  $app: /"
done

info "auditoria de lock"
AUDIT=$(python3 "$(dirname "$0")/lock-audit.py" "$START")
echo "$AUDIT" | sed 's/^/  /'
check "nenhum lock mantido por duas instâncias ao mesmo tempo" \
  "$(echo "$AUDIT" | grep -q '^overlaps=0$' && echo true || echo false)"

finish

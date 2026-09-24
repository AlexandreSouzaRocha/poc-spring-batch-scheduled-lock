#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

LINES=${1:-1000000}
FILES=${2:-1}
TYPE=${3:-ABERTO}
FOLDER=$(echo "$TYPE" | tr '[:upper:]' '[:lower:]')
TIMEOUT_SECONDS=${TIMEOUT_SECONDS:-900}

info "aguardando generator e particionadores ficarem saudáveis"
wait_healthy $(app_urls)
chaos_off
wait_idle

START=$(now_utc)
KAFKA_BEFORE=$(kafka_count)

info "gerando $FILES arquivo(s) de $LINES linhas ($TYPE) e publicando em entrada/"
GENERATED=$(generate "$LINES" "$FILES" "$TYPE")
echo "$GENERATED" | python3 -m json.tool
NAMES=$(echo "$GENERATED" | file_names_of)

TOTAL_PARTITIONS=0
for NAME in $NAMES; do
  info "aguardando registro de $NAME pelo polling"
  ID=$(wait_file_registered "$NAME" 120)
  check "arquivo registrado na received_file_management" "$([ -n "$ID" ] && echo true || echo false)"
  [ -z "$ID" ] && continue

  info "aguardando particionamento de $ID"
  STATUS=$(wait_file_status "$ID" "COMPLETED FAILED" "$TIMEOUT_SECONDS")
  check "status final COMPLETED (obtido: $STATUS)" "$([ "$STATUS" = "COMPLETED" ] && echo true || echo false)"

  PARTITIONS=$(file_field "$ID" "d['file']['partitioning']['count']")
  TOTAL_PARTITIONS=$((TOTAL_PARTITIONS + PARTITIONS))
  check "partições criadas = $PARTITIONS" "$(verification_field "$ID" "d['partitionDocuments'] == $PARTITIONS and d['partitionBlobs'] == $PARTITIONS")"
  check "soma das linhas das partições = $LINES" "$(verification_field "$ID" "d['partitionLines'] == $LINES")"
  check "todas as partições com tamanho, header e publicação corretos" "$(verification_field "$ID" "d['allPartitionsValid']")"
  check "partições na pasta $FOLDER/" "$(verification_field "$ID" "all(p['path'].startswith('$FOLDER/') for p in d['partitions'])")"
  check "original movido para processados/ e removido de entrada/" "$(verification_field "$ID" "d['currentPath'].startswith('processados/') and d['currentPathExists'] and not d['sourcePathExists']")"
  check "processado na primeira tentativa" "$(file_field "$ID" "str(d['file']['execution']['attempts'] == 1).lower()")"

  info "métricas do arquivo $ID"
  print_metrics "$START" "$ID"
done

KAFKA_AFTER=$(kafka_count)
check "mensagens publicadas no Kafka = $TOTAL_PARTITIONS (delta: $((KAFKA_AFTER - KAFKA_BEFORE)))" \
  "$([ $((KAFKA_AFTER - KAFKA_BEFORE)) -eq "$TOTAL_PARTITIONS" ] && echo true || echo false)"

info "auditoria do lock (ciclos por instância e sobreposição)"
AUDIT=$(python3 "$(dirname "$0")/lock-audit.py" "$START")
echo "$AUDIT"
check "nenhum lock mantido por duas instâncias ao mesmo tempo" "$(echo "$AUDIT" | grep -q '^overlaps=0$' && echo true || echo false)"

finish

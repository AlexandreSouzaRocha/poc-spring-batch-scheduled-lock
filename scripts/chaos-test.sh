#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

SCENARIO=${1:-all}
LINES=${LINES:-200000}

setup() {
  wait_healthy $(app_urls)
  chaos_off
  wait_idle
}

generate_one() {
  generate "$LINES" 1 "$1" "${2:-}" | file_names_of | head -1
}

partition_fail() {
  info "CENÁRIO partition-fail: partição 3 falha na 1ª tentativa -> rollback das partições e reprocessamento completo"
  setup
  local start kafka_before name id status partitions
  start=$(now_utc)
  kafka_before=$(kafka_count)
  chaos_on "point=PARTITION&action=FAIL&onAttempt=1&partitionIndex=3"
  name=$(generate_one FECHADO)
  id=$(wait_file_registered "$name" 120)
  status=$(wait_file_status "$id" "COMPLETED FAILED" 600)
  chaos_off
  partitions=$(file_field "$id" "d['file']['partitioning']['count']")
  check "status COMPLETED ($status)" "$([ "$status" = "COMPLETED" ] && echo true || echo false)"
  check "concluído na 2ª tentativa" "$(file_field "$id" "str(d['file']['execution']['attempts'] == 2).lower()")"
  check "cleanup fez rollback das partições da 1ª tentativa" "$([ "$(count_logs "$start" "partition.cleanup.*fileId=$id.*action=rollback")" -ge 1 ] && echo true || echo false)"
  check "partições íntegras após reprocessamento" "$(verification_field "$id" "d['allPartitionsValid'] and d['partitionBlobs'] == $partitions")"
  check "Kafka recebeu só as mensagens da tentativa bem-sucedida" "$([ $(( $(kafka_count) - kafka_before )) -eq "$partitions" ] && echo true || echo false)"
}

publish_fail() {
  info "CENÁRIO publish-fail: falha ao publicar na 1ª tentativa -> resume só da publicação, sem reparticionar"
  setup
  local start kafka_before name id status partitions
  start=$(now_utc)
  kafka_before=$(kafka_count)
  chaos_on "point=PUBLISH&action=FAIL&onAttempt=1"
  name=$(generate_one SALDO)
  id=$(wait_file_registered "$name" 120)
  status=$(wait_file_status "$id" "COMPLETED FAILED" 600)
  chaos_off
  partitions=$(file_field "$id" "d['file']['partitioning']['count']")
  check "status COMPLETED ($status)" "$([ "$status" = "COMPLETED" ] && echo true || echo false)"
  check "concluído na 2ª tentativa" "$(file_field "$id" "str(d['file']['execution']['attempts'] == 2).lower()")"
  check "cleanup pulado (particionamento já concluído)" "$([ "$(count_logs "$start" "partition.cleanup.*fileId=$id.*action=skip")" -ge 1 ] && echo true || echo false)"
  check "partições enviadas ao blob uma única vez" "$([ "$(count_logs "$start" "partition.upload.*fileId=$id")" -eq "$partitions" ] && echo true || echo false)"
  check "partições registradas no Mongo uma única vez" "$([ "$(count_logs "$start" "partition.register.*fileId=$id")" -eq 1 ] && echo true || echo false)"
  check "original movido uma única vez" "$([ "$(count_logs "$start" "original.move.*fileId=$id")" -eq 1 ] && echo true || echo false)"
  check "Kafka recebeu $partitions mensagens" "$([ $(( $(kafka_count) - kafka_before )) -eq "$partitions" ] && echo true || echo false)"
}

kill_owner() {
  info "CENÁRIO kill-owner: instância morre no meio do particionamento -> após stale-after sem heartbeat, outra assume em REPROCESSING e retoma"
  setup
  local start name id owner survivor status
  start=$(now_utc)
  chaos_on "point=PARTITION&action=DELAY&onAttempt=1&partitionIndex=1&delaySeconds=300"
  name=$(generate_one ULTIMA)
  id=$(wait_file_registered "$name" 120)
  until [ "$(count_logs "$start" "chaos.delay.*fileId=$id")" -ge 1 ]; do sleep 2; done
  owner=$(logs_since "$start" | grep "chaos.delay.*fileId=$id" | head -1 | awk '{print $1}')
  info "matando a instância que processa o arquivo: $owner"
  docker kill "psl-$owner" >/dev/null
  survivor=$([ "$owner" = "partitioner-1" ] && echo partitioner-2 || echo partitioner-1)
  status=$(wait_file_status "$id" "COMPLETED FAILED" 600)
  check "status COMPLETED após falha do dono ($status)" "$([ "$status" = "COMPLETED" ] && echo true || echo false)"
  check "assumido em REPROCESSING pela outra instância ($survivor)" "$([ "$(docker logs --since "$start" "psl-$survivor" 2>&1 | grep -c "file.reprocess.*fileId=$id")" -ge 1 ] && echo true || echo false)"
  check "retomado pela outra instância ($survivor)" "$([ "$(docker logs --since "$start" "psl-$survivor" 2>&1 | grep -c "job.metrics.*fileId=$id.*status=COMPLETED")" -ge 1 ] && echo true || echo false)"
  check "execução órfã recuperada (STARTED -> FAILED)" "$([ "$(docker logs --since "$start" "psl-$survivor" 2>&1 | grep -c "execution.recover.*fileId=$id")" -ge 1 ] && echo true || echo false)"
  check "partições íntegras" "$(verification_field "$id" "d['allPartitionsValid']")"
  info "religando $owner"
  docker start "psl-$owner" >/dev/null
  wait_healthy $(app_urls)
}

zombie_owner() {
  info "CENÁRIO zombie-owner: instância congela (docker pause) além de stale-after -> outra retoma; ao voltar, o zumbi perde a posse e não altera nada"
  setup
  local start kafka_before name id owner survivor status partitions
  start=$(now_utc)
  kafka_before=$(kafka_count)
  chaos_on "point=PARTITION&action=DELAY&onAttempt=1&partitionIndex=1&delaySeconds=20"
  name=$(generate_one ABERTO)
  id=$(wait_file_registered "$name" 120)
  until [ "$(count_logs "$start" "chaos.delay.*fileId=$id")" -ge 1 ]; do sleep 2; done
  owner=$(logs_since "$start" | grep "chaos.delay.*fileId=$id" | head -1 | awk '{print $1}')
  survivor=$([ "$owner" = "partitioner-1" ] && echo partitioner-2 || echo partitioner-1)
  info "congelando $owner (processo e heartbeat parados, sem morrer)"
  docker pause "psl-$owner" >/dev/null
  status=$(wait_file_status "$id" "COMPLETED FAILED" 600)
  info "descongelando $owner"
  docker unpause "psl-$owner" >/dev/null
  local deadline=$((SECONDS + 120))
  until [ "$(docker logs --since "$start" "psl-$owner" 2>&1 | grep -c "file.ownership.lost.*fileId=$id")" -ge 1 ] || [ $SECONDS -ge $deadline ]; do sleep 2; done
  sleep 10
  chaos_off
  partitions=$(file_field "$id" "d['file']['partitioning']['count']")
  check "concluído pela outra instância enquanto $owner estava congelada ($status)" "$([ "$status" = "COMPLETED" ] && echo true || echo false)"
  check "assumido em REPROCESSING por $survivor" "$([ "$(docker logs --since "$start" "psl-$survivor" 2>&1 | grep -c "file.reprocess.*fileId=$id")" -ge 1 ] && echo true || echo false)"
  check "zumbi ($owner) detectou a perda de posse" "$([ "$(docker logs --since "$start" "psl-$owner" 2>&1 | grep -c "file.ownership.lost.*fileId=$id")" -ge 1 ] && echo true || echo false)"
  check "zumbi não concluiu o job" "$([ "$(docker logs --since "$start" "psl-$owner" 2>&1 | grep -c "job.metrics.*fileId=$id.*status=COMPLETED")" -eq 0 ] && echo true || echo false)"
  check "status continua COMPLETED depois que o zumbi voltou" "$(file_field "$id" "str(d['file']['status'] == 'COMPLETED').lower()")"
  check "2 tentativas (original + retomada)" "$(file_field "$id" "str(d['file']['execution']['attempts'] == 2).lower()")"
  check "partições íntegras" "$(verification_field "$id" "d['allPartitionsValid'] and d['partitionBlobs'] == $partitions")"
  check "Kafka recebeu $partitions mensagens (zumbi não publicou)" "$([ $(( $(kafka_count) - kafka_before )) -eq "$partitions" ] && echo true || echo false)"
}

slow_io_at() {
  local point=$1 type=$2 start name id result
  info "CENÁRIO slow-io ($point): I/O de 90s, acima do limite de 60s da transação do Mongo -> conclui na 1ª tentativa"
  setup
  start=$(now_utc)
  chaos_on "point=$point&action=DELAY&onAttempt=1&delaySeconds=90"
  name=$(generate_one "$type")
  id=$(wait_file_registered "$name" 120)
  result=$(wait_file_status "$id" "COMPLETED FAILED" 600)
  chaos_off
  check "status COMPLETED ($result)" "$([ "$result" = "COMPLETED" ] && echo true || echo false)"
  check "concluído na 1ª tentativa" "$(file_field "$id" "str(d['file']['execution']['attempts'] == 1).lower()")"
  check "atraso de 90s aplicado em $point" "$([ "$(count_logs "$start" "chaos.delay.*point=$point.*fileId=$id")" -ge 1 ] && echo true || echo false)"
  check "nenhuma transação do Mongo abortada" "$([ "$(count_logs "$start" "NoSuchTransaction")" -eq 0 ] && echo true || echo false)"
  check "partições íntegras e publicadas" "$(verification_field "$id" "d['allPartitionsValid']")"
}

slow_io() {
  slow_io_at PARTITION ABERTO
  slow_io_at MOVE FECHADO
  slow_io_at PUBLISH SALDO
}

invalid_file() {
  info "CENÁRIO invalid-file: header inválido -> FAILED imediato, sem retentativa, arquivo mantido em entrada/"
  setup
  local kafka_before name id status
  kafka_before=$(kafka_count)
  name=$(generate_one ABERTO "&invalidHeader=true")
  id=$(wait_file_registered "$name" 120)
  status=$(wait_file_status "$id" "COMPLETED FAILED" 300)
  check "status FAILED ($status)" "$([ "$status" = "FAILED" ] && echo true || echo false)"
  check "sem retentativas (1 tentativa)" "$(file_field "$id" "str(d['file']['execution']['attempts'] == 1).lower()")"
  check "arquivo mantido em entrada/" "$(verification_field "$id" "d['currentPath'].startswith('entrada/') and d['currentPathExists']")"
  check "nenhuma partição criada" "$(verification_field "$id" "d['partitionDocuments'] == 0")"
  check "nenhuma mensagem no Kafka" "$([ "$(kafka_count)" -eq "$kafka_before" ] && echo true || echo false)"
}

case "$SCENARIO" in
  partition-fail) partition_fail ;;
  publish-fail) publish_fail ;;
  kill-owner) kill_owner ;;
  zombie-owner) zombie_owner ;;
  invalid-file) invalid_file ;;
  slow-io) slow_io ;;
  all) partition_fail; publish_fail; invalid_file; slow_io; kill_owner; zombie_owner ;;
  *) echo "cenário desconhecido: $SCENARIO"; exit 2 ;;
esac

finish

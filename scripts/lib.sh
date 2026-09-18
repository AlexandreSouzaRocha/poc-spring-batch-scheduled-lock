#!/usr/bin/env bash

GENERATOR_URL=${GENERATOR_URL:-http://localhost:8090}
P1_URL=${P1_URL:-http://localhost:8081}
P2_URL=${P2_URL:-http://localhost:8082}
TOPIC=${TOPIC:-movimentos-particionados}
PARTITIONERS=(partitioner-1 partitioner-2)
FAILURES=0

now_utc() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

info() {
  printf '\033[36m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"
}

check() {
  local description=$1
  local result=$2
  if [ "$result" = "true" ]; then
    printf '  \033[32mPASS\033[0m %s\n' "$description"
    return
  fi
  printf '  \033[31mFAIL\033[0m %s\n' "$description"
  FAILURES=$((FAILURES + 1))
}

finish() {
  if [ "$FAILURES" -eq 0 ]; then
    printf '\n\033[32mTODAS AS VALIDAÇÕES PASSARAM\033[0m\n'
    exit 0
  fi
  printf '\n\033[31m%s VALIDAÇÃO(ÕES) FALHARAM\033[0m\n' "$FAILURES"
  exit 1
}

partitioner_url() {
  curl -fsS "$P1_URL/actuator/health" >/dev/null 2>&1 && echo "$P1_URL" && return
  echo "$P2_URL"
}

wait_healthy() {
  for url in "$@"; do
    until curl -fsS "$url/actuator/health" 2>/dev/null | grep -q '"status":"UP"'; do
      sleep 2
    done
  done
}

wait_idle() {
  until curl -fsS "$(partitioner_url)/files/summary" \
      | python3 -c 'import json,sys; d=json.load(sys.stdin); sys.exit(0 if not any(k in d for k in ("PENDING","PARTITIONING","FAILED")) else 1)'; do
    sleep 3
  done
}

kafka_count() {
  docker exec psl-kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic "$TOPIC" 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum + 0}'
}

generate() {
  local lines=$1 files=$2 type=$3 extra=${4:-}
  curl -fsS -X POST "$GENERATOR_URL/generator/files?lines=$lines&files=$files&movementType=$type&movementDate=${DATE:-$(date +%Y-%m-%d)}$extra"
}

file_names_of() {
  python3 -c 'import json,sys; [print(f["fileName"]) for f in json.load(sys.stdin)]'
}

file_id_by_name() {
  local name=$1
  curl -fsS "$(partitioner_url)/files?limit=500" \
    | python3 -c "import json,sys; print(next((f['id'] for f in json.load(sys.stdin) if f['fileName']=='$name'), ''))"
}

wait_file_registered() {
  local name=$1 deadline=$((SECONDS + ${2:-120})) id=""
  while [ -z "$id" ] && [ $SECONDS -lt $deadline ]; do
    id=$(file_id_by_name "$name")
    [ -z "$id" ] && sleep 2
  done
  echo "$id"
}

file_json() {
  curl -fsS "$(partitioner_url)/files/$1"
}

file_field() {
  local id=$1 expression=$2
  file_json "$id" | python3 -c "import json,sys; d=json.load(sys.stdin); print($expression)"
}

wait_file_status() {
  local id=$1 statuses=$2 deadline=$((SECONDS + ${3:-900})) current_status=""
  while [ $SECONDS -lt $deadline ]; do
    current_status=$(file_field "$id" "d['file']['status']" 2>/dev/null || echo "")
    if echo " $statuses " | grep -q " $current_status "; then
      echo "$current_status"
      return
    fi
    sleep 3
  done
  echo "TIMEOUT:$current_status"
}

verification() {
  curl -fsS "$(partitioner_url)/files/$1/verification"
}

verification_field() {
  local id=$1 expression=$2
  verification "$id" | python3 -c "import json,sys; d=json.load(sys.stdin); print(str($expression).lower())"
}

logs_since() {
  local since=$1
  for app in "${PARTITIONERS[@]}"; do
    docker logs --since "$since" "psl-$app" 2>&1 | sed "s/^/$app /"
  done
}

count_logs() {
  local since=$1 pattern=$2
  logs_since "$since" | grep -E -c "$pattern" || true
}

chaos_on() {
  for url in "$P1_URL" "$P2_URL"; do
    curl -fsS -X PUT "$url/chaos?$1" >/dev/null 2>&1 || true
  done
}

chaos_off() {
  for url in "$P1_URL" "$P2_URL"; do
    curl -fsS -X DELETE "$url/chaos" >/dev/null 2>&1 || true
  done
}

print_metrics() {
  local since=$1 file_id=$2
  logs_since "$since" | grep "fileId=$file_id" | grep -E "operation=(step|job)\.metrics" \
    | grep -v "step=partitionWorkerStep" \
    | sed -E 's/ level=[A-Z]+ logger=[^ ]+ thread=[^ ]+ context=metrics operation=[a-z.]+//; s/ msg="[^"]*"//; s/ (data|ex)=.*$//; s/^/  /'
}

disk_free_gb() {
  docker run --rm alpine sh -c 'df -P / | awk "NR==2 {print int(\$4 / 1048576)}"'
}

disk_free_report() {
  info "disco livre na VM do Docker: $(disk_free_gb) GB"
}

blob_usage() {
  docker run --rm --network poc-partitioner_default mcr.microsoft.com/azure-cli:latest \
    az storage blob list --account-name devstoreaccount1 \
      --account-key "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" \
      --blob-endpoint http://azurite:10000/devstoreaccount1 --container-name movimentos \
      --query "[].{name:name, bytes:properties.contentLength}" -o json 2>/dev/null \
    | python3 "$(dirname "${BASH_SOURCE[0]}")/blob-usage.py"
}

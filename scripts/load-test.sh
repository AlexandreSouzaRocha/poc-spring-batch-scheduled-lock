#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

SIZES=${*:-50 100 200 250}
TYPE=${TYPE:-ABERTO}
KEEP_DATA=${KEEP_DATA:-false}
RESULTS_DIR=${RESULTS_DIR:-benchmarks}
RESULTS_FILE=${RESULTS_FILE:-$RESULTS_DIR/load-test-$(date +%Y%m%d-%H%M%S).md}
SAMPLE_FILE=$(mktemp)
BYTES_PER_LINE=151
DISK_FACTOR=3.2

trap 'stop_sampler; rm -f "$SAMPLE_FILE"' EXIT

lines_of() {
  echo $(($1 * 1000000))
}

gigabytes_of() {
  python3 -c "print(f'{$1 * $BYTES_PER_LINE / 1024 ** 3:.2f}')"
}

required_gigabytes() {
  python3 -c "print(int($1 * $BYTES_PER_LINE / 1024 ** 3 * $DISK_FACTOR) + 5)"
}

timeout_of() {
  python3 -c "print(max(900, int($1 / 50000)))"
}

start_sampler() {
  : > "$SAMPLE_FILE"
  sample_resources &
  SAMPLER_PID=$!
  disown
}

stop_sampler() {
  [ -n "${SAMPLER_PID:-}" ] && kill "$SAMPLER_PID" 2>/dev/null || true
  SAMPLER_PID=""
}

sampled_containers() {
  printf 'psl-%s ' "${PARTITIONERS[@]}"
  echo "psl-azurite"
}

sample_resources() {
  local tick=0
  while true; do
    docker stats --no-stream --format '{{.Name}} {{.MemUsage}} {{.CPUPerc}}' $(sampled_containers) \
      2>/dev/null >> "$SAMPLE_FILE" || true
    tick=$((tick + 1))
    if [ "$tick" -eq 1 ] || [ $((tick % 4)) -eq 0 ]; then
      echo "disk $(disk_free_gb)" >> "$SAMPLE_FILE"
    fi
    sleep 5
  done
}

peak_memory_mb() {
  { grep -E '^psl-partitioner' "$SAMPLE_FILE" || true; } | awk '{print $2}' \
    | python3 "$(dirname "$0")/peak-memory.py"
}

peak_cpu_percent() {
  local container=$1 peak
  peak=$({ grep -E "^$container " "$SAMPLE_FILE" || true; } | awk '{print $5}' | tr -d '%' | sort -n | tail -1)
  echo "${peak:--}"
}

save_details() {
  local since=$1 file_id=$2 size=$3
  local details="${RESULTS_FILE%.md}-detalhes.log"
  {
    echo "== ${size}MM fileId=$file_id"
    logs_since "$since" | grep "fileId=$file_id" | grep -E "operation=(step|job)\.metrics"
    echo "== amostras de docker stats"
    cat "$SAMPLE_FILE"
  } >> "$details"
}

min_free_disk_gb() {
  local minimum
  minimum=$({ grep '^disk ' "$SAMPLE_FILE" || true; } | awk '{print $2}' | sort -n | head -1)
  echo "${minimum:--}"
}

metric_field() {
  local line=$1 field=$2
  local value
  value=$(echo "$line" | grep -oE "(^| )$field=[^ ]+" | tail -1 | cut -d= -f2)
  echo "${value:--}"
}

metrics_line() {
  local since=$1 file_id=$2 operation=$3
  logs_since "$since" | grep "fileId=$file_id" | grep "operation=$operation" | grep -v "step=partitionWorkerStep" | tail -1
}

run_size() {
  local size=$1
  local lines file_gb needed free timeout start generated name id status
  lines=$(lines_of "$size")
  file_gb=$(gigabytes_of "$lines")
  needed=$(required_gigabytes "$lines")
  timeout=$(timeout_of "$lines")

  info "=== ${size}MM linhas (${file_gb} GB por arquivo) — precisa de ~${needed} GB livres ==="
  free=$(disk_free_gb)
  if [ "$free" -lt "$needed" ]; then
    printf '  \033[31mABORTADO\033[0m disco livre %s GB < necessario %s GB\n' "$free" "$needed"
    record_row "$size" "$lines" "$file_gb" "-" "-" "-" "-" "-" "-" "-" "-" "-" "-" "-" "-" "SEM_DISCO"
    return
  fi

  reset_environment
  start=$(now_utc)
  local kafka_before generation_start generation_seconds
  kafka_before=$(kafka_count)
  start_sampler

  info "gerando o arquivo (timeout do particionamento: ${timeout}s)"
  generation_start=$SECONDS
  generated=$(generate "$lines" 1 "$TYPE")
  generation_seconds=$((SECONDS - generation_start))
  name=$(echo "$generated" | file_names_of)
  info "arquivo $name gerado em ${generation_seconds}s"

  id=$(wait_file_registered "$name" 180)
  if [ -z "$id" ]; then
    stop_sampler
    record_row "$size" "$lines" "$file_gb" "$generation_seconds" "-" "-" "-" "-" "-" "-" "-" "-" "-" "-" "-" "NAO_REGISTRADO"
    return
  fi

  status=$(wait_file_status "$id" "COMPLETED ERROR" "$timeout")
  stop_sampler
  collect_result "$size" "$lines" "$file_gb" "$generation_seconds" "$start" "$id" "$status" "$kafka_before"
}

collect_result() {
  local size=$1 lines=$2 file_gb=$3 generation_seconds=$4 start=$5 id=$6 status=$7 kafka_before=$8
  local job step partitions kafka_delta attempts

  job=$(metrics_line "$start" "$id" "job.metrics")
  step=$(metrics_line "$start" "$id" "step.metrics" | grep "step=partitionMasterStep" || true)
  if [ -z "$step" ]; then
    step=$(logs_since "$start" | grep "fileId=$id" | grep "step=partitionMasterStep" | tail -1 || true)
  fi
  partitions=$(metric_field "$job" partitions)
  attempts=$(metric_field "$job" attempt)
  kafka_delta=$(($(kafka_count) - kafka_before))

  save_details "$start" "$id" "$size"
  info "resultado ${size}MM: status=$status  cpu_azurite=$(peak_cpu_percent psl-azurite)%  cpu_partitioner=$(peak_cpu_percent psl-partitioner-1)%"
  print_metrics "$start" "$id"
  blob_usage || true

  record_row "$size" "$lines" "$file_gb" "$generation_seconds" \
    "$(metric_field "$step" durationMs)" "$(metric_field "$step" mbPerSec)" "$(metric_field "$step" linesPerSec)" \
    "$(metric_field "$job" durationMs)" "$(metric_field "$job" mbPerSec)" \
    "${partitions:--}" "${PARTITION_THREADS:-1}" "${attempts:--}" "$kafka_delta" \
    "$(peak_memory_mb)" "$(min_free_disk_gb)" "$status"
}

record_row() {
  printf '| %sMM | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' "$@" >> "$RESULTS_FILE"
}

reset_environment() {
  if [ "$KEEP_DATA" = "true" ]; then
    return 0
  fi
  info "limpando ambiente (volumes do azurite e do mongo)"
  "$(dirname "$0")/reset-data.sh" >/dev/null
  wait_healthy $(app_urls)
}

write_header() {
  mkdir -p "$(dirname "$RESULTS_FILE")"
  if [ "${RESULTS_APPEND:-false}" = "true" ] && [ -s "$RESULTS_FILE" ]; then
    return 0
  fi
  {
    echo "# Teste de carga — $(date +'%Y-%m-%d %H:%M')"
    echo
    echo "Particoes: ${PARTITION_COUNT:-10} · threads/particao: ${PARTITION_THREADS:-1} · instancias: ${PARTITIONER_INSTANCES:-2} · memoria: ${PARTITIONER_MEMORY:-2g} · CPUs: ${PARTITIONER_CPUS:-2} · azurite threads: ${AZURITE_THREADS:-16} · OTEL: ${OTEL_ENABLED:-true}"
    echo
    echo "| tamanho | linhas | arquivo GB | geracao s | particao ms | particao MB/s | linhas/s | job ms | job MB/s | particoes | threads | tentativas | kafka | pico mem MB | disco livre min GB | status |"
    echo "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"
  } > "$RESULTS_FILE"
}

main() {
  wait_healthy $(app_urls)
  write_header
  disk_free_report
  for size in $SIZES; do
    run_size "$size"
  done
  info "resultados em $RESULTS_FILE"
  cat "$RESULTS_FILE"
}

main

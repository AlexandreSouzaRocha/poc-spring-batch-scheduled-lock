#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

VOLUME=${VOLUME:-100}
CONFIGS=${*:-10x1 12x1 15x1 10x2 10x4 15x4}
RESULTS_FILE=${RESULTS_FILE:-benchmarks/compare-${VOLUME}MM.md}

export RESULTS_FILE

for config in $CONFIGS; do
  count=${config%x*}
  threads=${config#*x}
  printf '\033[36m[%s]\033[0m === %s partições × %s thread(s) por partição ===\n' "$(date +%H:%M:%S)" "$count" "$threads"
  PARTITION_COUNT=$count PARTITION_THREADS=$threads RESULTS_APPEND=true \
    ./scripts/load-test.sh "$VOLUME"
done

printf '\033[36m[%s]\033[0m comparativo em %s\n' "$(date +%H:%M:%S)" "$RESULTS_FILE"
cat "$RESULTS_FILE"

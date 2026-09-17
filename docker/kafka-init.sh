#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
TOPIC="${APP_KAFKA_TOPIC:-movimentos-particionados}"
PARTITIONS="${APP_KAFKA_TOPIC_PARTITIONS:-10}"
REPLICATION="${APP_KAFKA_TOPIC_REPLICATION:-1}"

echo "[kafka-init] aguardando broker em ${BOOTSTRAP}..."
until kafka-broker-api-versions --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; do
  sleep 2
done

echo "[kafka-init] criando tópico ${TOPIC} (${PARTITIONS} partições)..."
kafka-topics --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists \
  --topic "${TOPIC}" --partitions "${PARTITIONS}" --replication-factor "${REPLICATION}"

kafka-topics --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC}"
echo "[kafka-init] concluído."

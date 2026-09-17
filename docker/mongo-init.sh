#!/usr/bin/env bash
set -euo pipefail

DB="${MONGO_DATABASE:-partitioner_batch}"
HOST="${MONGO_HOST:-mongo:27017}"
RETENTION_DAYS="${BATCH_RETENTION_DAYS:-7}"
SHEDLOCK_COLLECTION="${APP_SHEDLOCK_COLLECTION:-scheduler_locks}"
SHEDLOCK_NAME_FIELD="${APP_SHEDLOCK_FIELD_NAME:-_id}"
INIT_DIR="$(dirname "$0")"

echo "[mongo-init] aguardando mongod em ${HOST}..."
until mongosh --host "${HOST}" --quiet --eval "db.adminCommand('ping').ok" >/dev/null 2>&1; do
  sleep 1
done

echo "[mongo-init] inicializando replica set rs0 (se necessário)..."
mongosh --host "${HOST}" --quiet --eval "
  try {
    rs.status();
    print('replica set já inicializado');
  } catch (e) {
    rs.initiate({ _id: 'rs0', members: [{ _id: 0, host: '${HOST}' }] });
    print('replica set inicializado');
  }
"

echo "[mongo-init] aguardando eleição do primary..."
until mongosh --host "${HOST}" --quiet --eval "rs.isMaster().ismaster" | grep -q true; do
  sleep 1
done

echo "[mongo-init] criando collections e índices em ${DB}..."
MONGO_URI="mongodb://${HOST}/${DB}?directConnection=true"
SHEDLOCK_COLLECTION="${SHEDLOCK_COLLECTION}" SHEDLOCK_NAME_FIELD="${SHEDLOCK_NAME_FIELD}" \
  mongosh "${MONGO_URI}" --quiet --file "${INIT_DIR}/mongo-collections.js"

echo "[mongo-init] expurgo automático: TTL de ${RETENTION_DAYS} dia(s) nas coleções batch_*..."
BATCH_RETENTION_DAYS="${RETENTION_DAYS}" \
  mongosh "${MONGO_URI}" --quiet --file "${INIT_DIR}/mongo-ttl-indexes.js"

echo "[mongo-init] concluído."

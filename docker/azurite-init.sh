#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${APP_BLOB_CONTAINER:-movimentos}"
ACCOUNT="${APP_BLOB_ACCOUNT_NAME:-devstoreaccount1}"
KEY="${APP_BLOB_ACCOUNT_KEY}"
ENDPOINT="${APP_BLOB_ENDPOINT:-http://azurite:10000/devstoreaccount1}"

echo "[azurite-init] aguardando blob endpoint em ${ENDPOINT}..."
until az storage container list --account-name "${ACCOUNT}" --account-key "${KEY}" \
    --blob-endpoint "${ENDPOINT}" -o none 2>/dev/null; do
  sleep 2
done

echo "[azurite-init] criando container ${CONTAINER}..."
az storage container create --name "${CONTAINER}" --account-name "${ACCOUNT}" --account-key "${KEY}" \
  --blob-endpoint "${ENDPOINT}" -o tsv --query created

echo "[azurite-init] concluído."

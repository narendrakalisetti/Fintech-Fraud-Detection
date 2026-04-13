#!/usr/bin/env bash
# smoke_test.sh
# Triggers the full bronze->silver->gold pipeline and checks for success.
set -euo pipefail

ADF_NAME="adf-uks-energyzero-prod"
RESOURCE_GROUP="rg-uks-energyzero-prod"
PIPELINE_NAME="pl_ingest_ofgem_bronze"
TIMEOUT=300  # 5 minutes

echo "Triggering smoke test pipeline: $PIPELINE_NAME"

RUN_ID=$(az datafactory pipeline create-run \
  --factory-name "$ADF_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --name "$PIPELINE_NAME" \
  --query runId -o tsv)

echo "Run ID: $RUN_ID"
echo "Waiting for completion (max ${TIMEOUT}s)..."

ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
  STATUS=$(az datafactory pipeline-run show \
    --factory-name "$ADF_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --run-id "$RUN_ID" \
    --query status -o tsv)

  echo "  Status: $STATUS (${ELAPSED}s elapsed)"

  if [ "$STATUS" = "Succeeded" ]; then
    echo "Smoke test PASSED."
    exit 0
  elif [ "$STATUS" = "Failed" ] || [ "$STATUS" = "Cancelled" ]; then
    echo "Smoke test FAILED. Status: $STATUS"
    exit 1
  fi

  sleep 15
  ELAPSED=$((ELAPSED + 15))
done

echo "Smoke test TIMED OUT after ${TIMEOUT}s."
exit 1

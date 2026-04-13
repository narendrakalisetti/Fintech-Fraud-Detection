#!/usr/bin/env bash
# deploy_adf_pipelines.sh
# Publishes ADF pipeline JSON definitions via Azure CLI.
set -euo pipefail

ADF_NAME="adf-uks-energyzero-prod"
RESOURCE_GROUP="rg-uks-energyzero-prod"
PIPELINES_DIR="$(dirname "$0")/../adf-pipelines"

echo "Deploying ADF pipelines to $ADF_NAME..."

for pipeline_file in "$PIPELINES_DIR"/*.json; do
  pipeline_name=$(basename "$pipeline_file" .json)
  echo "  Publishing: $pipeline_name"
  az datafactory pipeline create \
    --factory-name "$ADF_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --name "$pipeline_name" \
    --pipeline "$(cat "$pipeline_file")"
done

echo "All pipelines deployed."

#!/usr/bin/env bash
# bootstrap_state.sh
# Creates the Terraform remote state storage account.
# Run once before terraform init.
set -euo pipefail

RESOURCE_GROUP="rg-uks-energyzero-tfstate"
STORAGE_ACCOUNT="sauksenergyzerotfstate"
CONTAINER="tfstate"
LOCATION="uksouth"

echo "Creating Terraform state infrastructure..."

az group create \
  --name "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --tags Project=EnergyZero ManagedBy=Manual Purpose=TerraformState

az storage account create \
  --name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --sku Standard_GRS \
  --kind StorageV2 \
  --min-tls-version TLS1_2 \
  --allow-blob-public-access false \
  --https-only true

az storage container create \
  --name "$CONTAINER" \
  --account-name "$STORAGE_ACCOUNT" \
  --auth-mode login

echo "Done. Terraform state backend ready:"
echo "  Resource Group : $RESOURCE_GROUP"
echo "  Storage Account: $STORAGE_ACCOUNT"
echo "  Container      : $CONTAINER"

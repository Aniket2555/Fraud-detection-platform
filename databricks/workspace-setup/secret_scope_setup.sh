#!/bin/bash
# Create a Databricks secret scope backed by Azure Key Vault
# Run this AFTER the workspace and Key Vault are deployed
# Requires: Databricks CLI configured with workspace URL + PAT

set -euo pipefail

ENVIRONMENT="${1:-dev}"
RG="rg-fraud-detection-${ENVIRONMENT}"
# Key Vault names are globally unique across all Azure tenants, so the
# deployed vault carries a random suffix rather than the bare
# "kv-fraud-${ENVIRONMENT}" -- resolve the real name instead of assuming it.
KV_NAME=$(az keyvault list --resource-group "${RG}" --query "[0].name" -o tsv)
KV_RESOURCE_ID=$(az keyvault show --name "${KV_NAME}" --query id -o tsv)
KV_DNS_NAME=$(az keyvault show --name "${KV_NAME}" --query properties.vaultUri -o tsv)

echo "Creating Databricks secret scope 'kv-fraud' backed by Key Vault '${KV_NAME}'..."

databricks secrets create-scope \
    --scope "kv-fraud" \
    --scope-backend-type AZURE_KEYVAULT \
    --resource-id "${KV_RESOURCE_ID}" \
    --dns-name "${KV_DNS_NAME}"

echo "Verifying secret scope..."
databricks secrets list-scopes
databricks secrets list --scope "kv-fraud"

echo "Secret scope 'kv-fraud' created and linked to Key Vault '${KV_NAME}'"

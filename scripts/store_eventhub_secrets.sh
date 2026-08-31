#!/bin/bash
# Store Event Hubs connection strings in Key Vault
# Run this AFTER applying the eventhubs Terraform module (infrastructure/modules/eventhubs/)
set -euo pipefail

ENV="${1:-dev}"
RG="rg-fraud-detection-${ENV}"
EH_NS="ehns-fraud-${ENV}"
EH_NAME="eh-transactions"
# Key Vault names are globally unique across all Azure tenants (like storage
# accounts), so the deployed vault carries a random suffix
# (infrastructure/modules/key-vault/main.tf) rather than the bare
# "kv-fraud-${ENV}" -- resolve the real name instead of assuming it.
KV_NAME=$(az keyvault list --resource-group "${RG}" --query "[0].name" -o tsv)
if [ -z "${KV_NAME}" ]; then
  echo "ERROR: no Key Vault found in resource group ${RG}" >&2
  exit 1
fi

echo "Fetching Event Hubs connection strings..."
PRODUCER_CONN=$(az eventhubs eventhub authorization-rule keys list \
  --resource-group "${RG}" \
  --namespace-name "${EH_NS}" \
  --eventhub-name "${EH_NAME}" \
  --name producer-send-rule \
  --query primaryConnectionString -o tsv)

CONSUMER_CONN=$(az eventhubs eventhub authorization-rule keys list \
  --resource-group "${RG}" \
  --namespace-name "${EH_NS}" \
  --eventhub-name "${EH_NAME}" \
  --name consumer-listen-rule \
  --query primaryConnectionString -o tsv)

echo "Storing secrets in Key Vault '${KV_NAME}'..."
az keyvault secret set --vault-name "${KV_NAME}" --name eventhub-producer-conn-str --value "$PRODUCER_CONN"
az keyvault secret set --vault-name "${KV_NAME}" --name eventhub-consumer-conn-str --value "$CONSUMER_CONN"
az keyvault secret set --vault-name "${KV_NAME}" --name eventhub-namespace --value "${EH_NS}"
az keyvault secret set --vault-name "${KV_NAME}" --name eventhub-name --value "${EH_NAME}"

echo "✅ Event Hubs secrets stored in Key Vault successfully!"

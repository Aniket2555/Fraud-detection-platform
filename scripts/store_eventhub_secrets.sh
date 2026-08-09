#!/bin/bash
# Store Event Hubs connection strings in Key Vault
# Run this AFTER deploying eventhubs.bicep
set -euo pipefail

ENV="${1:-dev}"
RG="rg-fraud-detection-${ENV}"
EH_NS="ehns-fraud-${ENV}"
EH_NAME="eh-transactions"
KV_NAME="kv-fraud-${ENV}"

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

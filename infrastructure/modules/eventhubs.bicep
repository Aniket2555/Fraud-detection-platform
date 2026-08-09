@description('Environment name — dev only for Free Trial')
param environment string = 'dev'

@description('Location')
param location string

param projectName string = 'fraud-detection'

@description('ADLS Gen2 storage account resource ID (for Capture)')
param storageAccountId string

@description('ADLS Gen2 storage account name (for Capture container)')
param storageAccountName string

var namespaceName = 'ehns-fraud-${environment}'
var eventHubName = 'eh-transactions'

// --- Event Hubs Namespace ---
resource ehNamespace 'Microsoft.EventHub/namespaces@2024-01-01' = {
  name: namespaceName
  location: location
  tags: {
    project: projectName
    environment: environment
    'managed-by': 'bicep'
  }
  sku: {
    name: 'Standard'          // Free Trial: Standard ($11/month base)
    tier: 'Standard'
    capacity: 1               // 1 TU — sufficient for dev throughput
  }
  properties: {
    isAutoInflateEnabled: false  // Free Trial: disabled (cost control)
    maximumThroughputUnits: 0
    publicNetworkAccess: 'Enabled'  // Free Trial: no private endpoints
    disableLocalAuth: false    // Allow SAS auth (simpler for Free Trial)
    zoneRedundant: false       // Free Trial: no zone redundancy
  }
}

// --- Event Hub: Transactions ---
resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: ehNamespace
  name: eventHubName
  properties: {
    partitionCount: 4          // Free Trial: 4 partitions (Standard max: 32)
    messageRetentionInDays: 1  // Free Trial: minimum retention (free)
    captureDescription: {
      enabled: true            // Always-on Capture for disaster recovery
      encoding: 'Avro'
      intervalInSeconds: 900   // 15-minute window (minimizes write cost)
      sizeLimitInBytes: 314572800  // 300 MB window
      skipEmptyArchives: true  // Don't write empty Avro files
      destination: {
        name: 'EventHubArchive.AzureBlockBlob'
        properties: {
          storageAccountResourceId: storageAccountId
          blobContainer: 'eventhubs-capture'
          archiveNameFormat: '{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}'
        }
      }
    }
  }
}

// --- Consumer Groups ---
resource defaultConsumerGroup 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2024-01-01' = {
  parent: eventHub
  name: '$Default'
}

resource bronzeConsumerGroup 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2024-01-01' = {
  parent: eventHub
  name: 'bronze-ingest'
}

// --- Authorization Rule: Producer (Send only) ---
resource producerAuthRule 'Microsoft.EventHub/namespaces/eventhubs/authorizationRules@2024-01-01' = {
  parent: eventHub
  name: 'producer-send-rule'
  properties: {
    rights: ['Send']           // Least privilege: producer can only send
  }
}

// --- Authorization Rule: Consumer (Listen only) ---
resource consumerAuthRule 'Microsoft.EventHub/namespaces/eventhubs/authorizationRules@2024-01-01' = {
  parent: eventHub
  name: 'consumer-listen-rule'
  properties: {
    rights: ['Listen']         // Least privilege: consumer can only receive
  }
}

output namespaceName string = ehNamespace.name
output eventHubName string = eventHub.name
output producerConnectionString string = producerAuthRule.listKeys().primaryConnectionString
output consumerConnectionString string = consumerAuthRule.listKeys().primaryConnectionString

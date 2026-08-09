@description('Environment name')
param environment string

@description('Location')
param location string

param projectName string = 'fraud-detection'

var storageName = 'stfraudlake${environment}'

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageName
  location: location
  tags: {
    project: projectName
    environment: environment
    'managed-by': 'bicep'
  }
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'                 // Free Trial: cheapest redundancy
  }
  properties: {
    isHnsEnabled: true                   // Hierarchical namespace = ADLS Gen2
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
    allowSharedKeyAccess: true           // Free Trial: needed for Databricks access
    defaultToOAuthAuthentication: true
    accessTier: 'Hot'
    networkAcls: {
      defaultAction: 'Allow'             // Free Trial: no VNet/private endpoints
      bypass: 'AzureServices'
    }
  }
}

// --- Blob services configuration ---
resource blobServices 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storageAccount
  name: 'default'
  properties: {
    deleteRetentionPolicy: {
      enabled: true
      days: 7                            // Free Trial: shortest retention to save storage
    }
    containerDeleteRetentionPolicy: {
      enabled: true
      days: 7
    }
    // No blob versioning — Delta Lake handles versioning via transaction log
  }
}

// --- Filesystem containers (ADLS Gen2) ---
var containers = [
  'staging'          // Human/Kaggle-API upload landing zone — pl_ingest_ieee_cis copies staging -> raw/ieee-cis/
  'raw'
  'bronze'
  'silver'
  'gold'
  'quarantine'
  'checkpoints'
  'feature-store'
  'eventhubs-capture'
]

resource filesystems 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = [for container in containers: {
  parent: blobServices
  name: container
  properties: {
    publicAccess: 'None'
  }
}]

output storageAccountId string = storageAccount.id
output storageAccountName string = storageAccount.name
output dfsEndpoint string = storageAccount.properties.primaryEndpoints.dfs

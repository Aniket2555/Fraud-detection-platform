@description('Environment name')
param environment string

@description('Location')
param location string

param projectName string = 'fraud-detection'

@description('Tenant ID for Azure AD')
param tenantId string

@description('Object ID of the deployer for initial access')
param deployerObjectId string

@description('Log Analytics Workspace ID for diagnostics')
param logAnalyticsWorkspaceId string

var kvName = 'kv-fraud-${environment}'

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: kvName
  location: location
  tags: {
    project: projectName
    environment: environment
    'managed-by': 'bicep'
  }
  properties: {
    tenantId: tenantId
    sku: {
      family: 'A'
      name: 'standard'    // Free Trial: always Standard (10k free ops/month)
    }
    enableRbacAuthorization: true     // Use RBAC, not access policies
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
    enablePurgeProtection: false     // Free Trial: disabled for easy cleanup
    enabledForDeployment: false
    enabledForDiskEncryption: false
    enabledForTemplateDeployment: true
    publicNetworkAccess: 'Enabled'   // Free Trial: no private endpoint
    networkAcls: {
      defaultAction: 'Allow'         // Free Trial: allow all (no VNet)
      bypass: 'AzureServices'
    }
  }
}

// --- Deployer gets admin role ---
resource deployerAccess 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: keyVault
  name: guid(keyVault.id, deployerObjectId, 'Key Vault Administrator')
  properties: {
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      '00482a5a-887f-4fb3-b363-3b7fe8e74483'  // Key Vault Administrator
    )
    principalId: deployerObjectId
    principalType: 'User'
  }
}

// --- Placeholder secrets for later phases ---
resource placeholderSecrets 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = [for secretName in [
  'eventhub-conn-str'
  'eventhub-namespace'
  'redis-conn-str'
  'azure-sql-conn-str'
  'cosmos-db-conn-str'
  'service-bus-conn-str'
  'app-config-conn-str'
]: {
  parent: keyVault
  name: secretName
  properties: {
    value: 'PLACEHOLDER-TO-BE-SET-IN-PHASE-${secretName}'
    contentType: 'text/plain'
    attributes: {
      enabled: true
    }
  }
}]

// --- Diagnostic settings ---
resource kvDiagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: '${kvName}-diagnostics'
  scope: keyVault
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logs: [
      {
        category: 'AuditEvent'
        enabled: true
        retentionPolicy: { enabled: true, days: 30 }
      }
    ]
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
        retentionPolicy: { enabled: true, days: 30 }
      }
    ]
  }
}

output keyVaultId string = keyVault.id
output keyVaultName string = keyVault.name
output keyVaultUri string = keyVault.properties.vaultUri

@description('Environment name')
param environment string = 'dev'

@description('Databricks System-Assigned Principal ID')
param databricksPrincipalId string

@description('Decision Function System-Assigned Principal ID')
param decisionFunctionPrincipalId string

@description('Logic App (step-up/review workflow) System-Assigned Principal ID')
param logicAppPrincipalId string

@description('ADLS Gen2 Storage Account Name')
param storageAccountName string

@description('Key Vault Name')
param keyVaultName string

@description('Service Bus Namespace Name (sbns-fraud-{env})')
param serviceBusNamespaceName string

@description('App Configuration Store Name (appcs-fraud-{env})')
param appConfigurationName string

@description('Azure SQL Server Name (sql-fraud-{env})')
param sqlServerName string

@description('Azure SQL Database Name (sqldb-fraud-cases-{env})')
param sqlDatabaseName string

// --- Reference Existing Resources ---
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' existing = {
  name: storageAccountName
}

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' existing = {
  name: keyVaultName
}

resource serviceBusNamespace 'Microsoft.ServiceBus/namespaces@2022-10-01-preview' existing = {
  name: serviceBusNamespaceName
}

resource appConfiguration 'Microsoft.AppConfiguration/configurationStores@2023-03-01' existing = {
  name: appConfigurationName
}

resource sqlServer 'Microsoft.Sql/servers@2023-05-01-preview' existing = {
  name: sqlServerName
}

resource sqlDatabase 'Microsoft.Sql/servers/databases@2023-05-01-preview' existing = {
  name: sqlDatabaseName
  parent: sqlServer
}

// --- Role Definition IDs (Azure Built-In Roles) ---
// NOTE: verify these against `az role definition list --name "<role name>"` for
// the target tenant before first deployment -- built-in role GUIDs are global
// but were not validated against a live subscription as part of this fix.
var storageBlobDataContributorId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
var keyVaultSecretsUserId = '4633005b-87f7-41d1-bf41-8779d9c5332e'
var serviceBusDataSenderId = '69a216fc-b8fb-44d8-bc22-1f3c2cd27a39'
var serviceBusDataReceiverId = '4f6d3b9b-027b-4f4c-9142-0e5a2a2247e0'
var appConfigDataReaderId = '516239f1-63e1-4d78-a4de-a74fb236a071'
var sqlDbContributorId = '9b7fa17d-e63e-47b0-bb0a-15c516ac86ec'

// --- Assign: Databricks → ADLS Gen2 Storage Blob Data Contributor ---
resource storageRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, databricksPrincipalId, storageBlobDataContributorId)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributorId)
    principalId: databricksPrincipalId
    principalType: 'ServicePrincipal'
  }
}

// --- Assign: Databricks → Key Vault Secrets User ---
resource kvRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, databricksPrincipalId, keyVaultSecretsUserId)
  scope: keyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', keyVaultSecretsUserId)
    principalId: databricksPrincipalId
    principalType: 'ServicePrincipal'
  }
}

// --- Assign: Decision Function → Service Bus Data Sender ---
// Per Implementation-details/Phase_7_implementation_plan.md §7.3 RBAC matrix.
// Previously this module accepted decisionFunctionPrincipalId but never used
// it in any roleAssignment -- the Decision Function's managed identity had no
// RBAC-based access to Service Bus/App Configuration, leaving it dependent on
// connection-string/SAS auth despite the platform's documented zero-trust
// managed-identity design.
resource decisionFunctionSbRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(serviceBusNamespace.id, decisionFunctionPrincipalId, serviceBusDataSenderId)
  scope: serviceBusNamespace
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', serviceBusDataSenderId)
    principalId: decisionFunctionPrincipalId
    principalType: 'ServicePrincipal'
  }
}

// --- Assign: Decision Function → App Configuration Data Reader ---
resource decisionFunctionAppConfigRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(appConfiguration.id, decisionFunctionPrincipalId, appConfigDataReaderId)
  scope: appConfiguration
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', appConfigDataReaderId)
    principalId: decisionFunctionPrincipalId
    principalType: 'ServicePrincipal'
  }
}

// --- Assign: Logic App → Service Bus Data Receiver ---
resource logicAppSbRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(serviceBusNamespace.id, logicAppPrincipalId, serviceBusDataReceiverId)
  scope: serviceBusNamespace
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', serviceBusDataReceiverId)
    principalId: logicAppPrincipalId
    principalType: 'ServicePrincipal'
  }
}

// --- Assign: Logic App → SQL DB Contributor ---
resource logicAppSqlRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(sqlDatabase.id, logicAppPrincipalId, sqlDbContributorId)
  scope: sqlDatabase
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', sqlDbContributorId)
    principalId: logicAppPrincipalId
    principalType: 'ServicePrincipal'
  }
}

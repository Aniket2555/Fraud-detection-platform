// ============================================================
// Fraud Detection Platform — Main Infrastructure Orchestrator
// FREE TRIAL VERSION — no VNet, no private endpoints, no Azure ML
// ============================================================
// Deploys: Resource Group → Log Analytics → Key Vault → ADLS Gen2 → Databricks
// Usage: az deployment sub create --location centralindia \
//        --template-file main.bicep \
//        --parameters modules/parameters/dev.parameters.json

targetScope = 'subscription'

// --- Parameters ---
@description('Environment name — dev only for Free Trial')
@allowed(['dev'])
param environment string = 'dev'

@description('Azure region')
param location string = 'centralindia'

@description('Owner email for tagging')
param ownerEmail string

@description('Azure AD tenant ID')
param tenantId string

@description('Object ID of the deployer (for Key Vault admin access)')
param deployerObjectId string

param projectName string = 'fraud-detection'

// --- Step 1: Resource Group ---
module rg 'modules/resource-group.bicep' = {
  name: 'deploy-resource-group'
  params: {
    environment: environment
    location: location
    ownerEmail: ownerEmail
    projectName: projectName
  }
}

// --- Step 2: Log Analytics ---
module logAnalytics 'modules/log-analytics.bicep' = {
  name: 'deploy-log-analytics'
  scope: resourceGroup(rg.outputs.resourceGroupName)
  params: {
    environment: environment
    location: location
    projectName: projectName
  }
}

// --- Step 3: Key Vault ---
module keyVault 'modules/key-vault.bicep' = {
  name: 'deploy-keyvault'
  scope: resourceGroup(rg.outputs.resourceGroupName)
  params: {
    environment: environment
    location: location
    tenantId: tenantId
    deployerObjectId: deployerObjectId
    logAnalyticsWorkspaceId: logAnalytics.outputs.logAnalyticsId
    projectName: projectName
  }
}

// --- Step 4: ADLS Gen2 (no private endpoint for Free Trial) ---
module storage 'modules/storage-account.bicep' = {
  name: 'deploy-storage'
  scope: resourceGroup(rg.outputs.resourceGroupName)
  params: {
    environment: environment
    location: location
    projectName: projectName
  }
}

// --- Step 5: Databricks Workspace (no VNet injection for Free Trial) ---
module databricks 'modules/databricks-workspace.bicep' = {
  name: 'deploy-databricks'
  scope: resourceGroup(rg.outputs.resourceGroupName)
  params: {
    environment: environment
    location: location
    projectName: projectName
  }
}

// --- Outputs ---
output resourceGroupName string = rg.outputs.resourceGroupName
output storageAccountName string = storage.outputs.storageAccountName
output keyVaultName string = keyVault.outputs.keyVaultName
output databricksWorkspaceUrl string = databricks.outputs.workspaceUrl
output logAnalyticsName string = logAnalytics.outputs.logAnalyticsName

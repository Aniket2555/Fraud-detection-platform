@description('Environment name')
param environment string = 'dev'

@description('Location')
param location string

param projectName string = 'fraud-detection'

var configStoreName = 'appcs-fraud-${environment}'

resource appConfig 'Microsoft.AppConfiguration/configurationStores@2023-03-01' = {
  name: configStoreName
  location: location
  tags: {
    project: projectName
    environment: environment
    'managed-by': 'bicep'
  }
  sku: {
    name: 'free' // Free Trial: Free tier (1,000 requests/day)
  }
  properties: {
    publicNetworkAccess: 'Enabled'
  }
}

// Seed default threshold values
resource approveThreshold 'Microsoft.AppConfiguration/configurationStores/keyValues@2023-03-01' = {
  parent: appConfig
  name: 'FraudEngine:ApproveMaxThreshold'
  properties: {
    value: '0.10'
    tags: { description: 'Maximum score for auto-approve' }
  }
}

resource stepUpThreshold 'Microsoft.AppConfiguration/configurationStores/keyValues@2023-03-01' = {
  parent: appConfig
  name: 'FraudEngine:StepUpMaxThreshold'
  properties: {
    value: '0.60'
    tags: { description: 'Maximum score for step-up auth' }
  }
}

resource blockThreshold 'Microsoft.AppConfiguration/configurationStores/keyValues@2023-03-01' = {
  parent: appConfig
  name: 'FraudEngine:BlockMinThreshold'
  properties: {
    value: '0.90'
    tags: { description: 'Minimum score for auto-block' }
  }
}

output configStoreEndpoint string = appConfig.properties.endpoint

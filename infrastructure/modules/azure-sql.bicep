@description('Environment name — dev for Free Trial')
param environment string = 'dev'

@description('Location')
param location string

param projectName string = 'fraud-detection'

@description('SQL Admin login name')
param sqlAdminLogin string = 'fraudsqladmin'

@description('SQL Admin password from Key Vault')
@secure()
param sqlAdminPassword string

var serverName = 'sql-fraud-${environment}'
var dbName = 'sqldb-fraud-cases-${environment}'

// --- Azure SQL Logical Server ---
resource sqlServer 'Microsoft.Sql/servers@2023-05-01-preview' = {
  name: serverName
  location: location
  tags: {
    project: projectName
    environment: environment
    'managed-by': 'bicep'
  }
  properties: {
    administratorLogin: sqlAdminLogin
    administratorLoginPassword: sqlAdminPassword
    version: '12.0'
    publicNetworkAccess: 'Enabled'
  }
}

resource sqlFirewallAzure 'Microsoft.Sql/servers/firewallRules@2023-05-01-preview' = {
  parent: sqlServer
  name: 'AllowAllWindowsAzureIps'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

// --- Azure SQL Database: Serverless GP_S_Gen5_1 ---
resource sqlDatabase 'Microsoft.Sql/servers/databases@2023-05-01-preview' = {
  parent: sqlServer
  name: dbName
  location: location
  tags: {
    project: projectName
    environment: environment
    'managed-by': 'bicep'
  }
  sku: {
    name: 'GP_S_Gen5'
    tier: 'GeneralPurpose'
    family: 'Gen5'
    capacity: 1
  }
  properties: {
    autoPauseDelay: 60
    minCapacity: json('0.5')
    maxSizeBytes: 34359738368
    zoneRedundant: false
  }
}

output sqlServerFqdn string = sqlServer.properties.fullyQualifiedDomainName
output sqlDatabaseName string = sqlDatabase.name

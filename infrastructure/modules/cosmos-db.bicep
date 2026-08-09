@description('Environment name')
param environment string = 'dev'

@description('Location')
param location string

param projectName string = 'fraud-detection'

var accountName = 'cosmos-fraud-${environment}'
var databaseName = 'graph-fraud-db'
var graphName = 'entity-graph'

resource cosmosAccount 'Microsoft.DocumentDB/databaseAccounts@2023-11-15' = {
  name: accountName
  location: location
  kind: 'GlobalDocumentDB'
  tags: {
    project: projectName
    environment: environment
    'managed-by': 'bicep'
  }
  properties: {
    capabilities: [
      { name: 'EnableGremlin' }
      { name: 'EnableServerless' } // Free Trial: Serverless SKU (pay per RU consumed)
    ]
    databaseAccountOfferType: 'Standard'
    locations: [
      {
        locationName: location
        failoverPriority: 0
      }
    ]
    consistencyPolicy: {
      defaultConsistencyLevel: 'Session'
    }
  }
}

resource gremlinDb 'Microsoft.DocumentDB/databaseAccounts/gremlinDatabases@2023-11-15' = {
  parent: cosmosAccount
  name: databaseName
  properties: {
    resource: {
      id: databaseName
    }
  }
}

resource gremlinGraph 'Microsoft.DocumentDB/databaseAccounts/gremlinDatabases/graphs@2023-11-15' = {
  parent: gremlinDb
  name: graphName
  properties: {
    resource: {
      id: graphName
      partitionKey: {
        paths: ['/partitionKey']
        kind: 'Hash'
      }
      indexingPolicy: {
        indexingMode: 'consistent'
        includedPaths: [{ path: '/*' }]
        excludedPaths: [{ path: '/"_etag"/?' }]
      }
    }
  }
}

output cosmosEndpoint string = cosmosAccount.properties.documentEndpoint
output databaseName string = databaseName
output graphName string = graphName

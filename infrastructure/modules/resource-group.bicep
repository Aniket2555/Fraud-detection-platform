targetScope = 'subscription'

@description('The environment name — dev only for Free Trial')
@allowed(['dev'])
param environment string = 'dev'

@description('The Azure region for the resource group')
param location string = 'centralindia'

@description('Project name for naming convention')
param projectName string = 'fraud-detection'

@description('Owner email for tagging')
param ownerEmail string

var rgName = 'rg-${projectName}-${environment}'

resource rg 'Microsoft.Resources/resourceGroups@2023-07-01' = {
  name: rgName
  location: location
  tags: {
    project: projectName
    environment: environment
    owner: ownerEmail
    'managed-by': 'bicep'
  }
}

output resourceGroupName string = rg.name
output resourceGroupId string = rg.id

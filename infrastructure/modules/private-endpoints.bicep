@description('Environment name')
param environment string = 'dev'

@description('Location')
param location string

@description('VNet name for private endpoint subnet')
param vnetName string

@description('Private endpoint subnet name')
param privateEndpointSubnetName string = 'snet-private-endpoints'

@description('ADLS Gen2 Storage Account Resource ID')
param storageAccountId string

@description('Key Vault Resource ID')
param keyVaultId string

@description('Enable private endpoints (false for Free Trial, true for production)')
param enablePrivateEndpoints bool = false

resource storagePe 'Microsoft.Network/privateEndpoints@2023-04-01' = if (enablePrivateEndpoints) {
  name: 'pe-storage-${environment}'
  location: location
  properties: {
    subnet: {
      id: resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, privateEndpointSubnetName)
    }
    privateLinkServiceConnections: [
      {
        name: 'plsc-storage-${environment}'
        properties: {
          privateLinkServiceId: storageAccountId
          groupIds: ['dfs']
        }
      }
    ]
  }
}

resource kvPe 'Microsoft.Network/privateEndpoints@2023-04-01' = if (enablePrivateEndpoints) {
  name: 'pe-keyvault-${environment}'
  location: location
  properties: {
    subnet: {
      id: resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, privateEndpointSubnetName)
    }
    privateLinkServiceConnections: [
      {
        name: 'plsc-keyvault-${environment}'
        properties: {
          privateLinkServiceId: keyVaultId
          groupIds: ['vault']
        }
      }
    ]
  }
}

resource storageDnsZone 'Microsoft.Network/privateDnsZones@2020-06-01' = if (enablePrivateEndpoints) {
  name: 'privatelink.dfs.core.windows.net'
  location: 'global'
}

resource kvDnsZone 'Microsoft.Network/privateDnsZones@2020-06-01' = if (enablePrivateEndpoints) {
  name: 'privatelink.vaultcore.azure.net'
  location: 'global'
}

// --- VNet links: without these, records in the zones above are never
// resolvable from anything on the VNet, private endpoint or not ---
resource storageDnsZoneVnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = if (enablePrivateEndpoints) {
  parent: storageDnsZone
  name: 'link-storage-${environment}'
  location: 'global'
  properties: {
    virtualNetwork: {
      id: resourceId('Microsoft.Network/virtualNetworks', vnetName)
    }
    registrationEnabled: false
  }
}

resource kvDnsZoneVnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = if (enablePrivateEndpoints) {
  parent: kvDnsZone
  name: 'link-keyvault-${environment}'
  location: 'global'
  properties: {
    virtualNetwork: {
      id: resourceId('Microsoft.Network/virtualNetworks', vnetName)
    }
    registrationEnabled: false
  }
}

// --- Zone groups: without these, the private endpoints above exist but are
// never registered into the DNS zones, so *.blob.core.windows.net /
// *.vault.azure.net still resolve to public IPs even with the endpoints and
// zones both provisioned ---
resource storagePeDnsZoneGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2023-04-01' = if (enablePrivateEndpoints) {
  parent: storagePe
  name: 'dns-zone-group-storage'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'config-storage'
        properties: {
          privateDnsZoneId: storageDnsZone.id
        }
      }
    ]
  }
}

resource kvPeDnsZoneGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2023-04-01' = if (enablePrivateEndpoints) {
  parent: kvPe
  name: 'dns-zone-group-keyvault'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'config-keyvault'
        properties: {
          privateDnsZoneId: kvDnsZone.id
        }
      }
    ]
  }
}

@description('Name of the target resource for diagnostic collection')
param targetResourceName string

@description('Resource ID of the target resource')
param targetResourceId string

@description('Resource ID of the Log Analytics Workspace')
param logAnalyticsWorkspaceId string

@description('Log retention in days (30 for Free Trial, 365 for PCI production)')
param retentionDays int = 30

resource diagnosticSettings 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'diag-${targetResourceName}'
  scope: any(targetResourceId)
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logs: [
      {
        categoryGroup: 'allLogs'
        enabled: true
        retentionPolicy: {
          enabled: true
          days: retentionDays
        }
      }
    ]
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
        retentionPolicy: {
          enabled: true
          days: retentionDays
        }
      }
    ]
  }
}

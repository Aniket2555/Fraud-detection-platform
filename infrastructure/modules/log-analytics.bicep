@description('Environment name')
param environment string

@description('Location')
param location string

param projectName string = 'fraud-detection'

var logAnalyticsName = 'log-fraud-${environment}'

var retentionDays = 30                    // Free Trial: 30 days (free tier)
var dailyCapGb = 1                        // Free Trial: 1 GB/day (stay within free 5 GB/month)

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: logAnalyticsName
  location: location
  tags: {
    project: projectName
    environment: environment
    'managed-by': 'bicep'
  }
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: retentionDays
    workspaceCapping: dailyCapGb > 0 ? {
      dailyQuotaGb: dailyCapGb
    } : null
  }
}

output logAnalyticsId string = logAnalytics.id
output logAnalyticsName string = logAnalytics.name

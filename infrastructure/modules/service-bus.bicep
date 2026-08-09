@description('Environment name — dev for Free Trial')
param environment string = 'dev'

@description('Location')
param location string

param projectName string = 'fraud-detection'

var namespaceName = 'sbns-fraud-${environment}'
var topicName = 'sb-topic-fraud-events'

// --- Service Bus Namespace (Standard Tier required for Topics) ---
resource sbNamespace 'Microsoft.ServiceBus/namespaces@2022-10-01-preview' = {
  name: namespaceName
  location: location
  tags: {
    project: projectName
    environment: environment
    'managed-by': 'bicep'
  }
  sku: {
    name: 'Standard'
    tier: 'Standard'
  }
  properties: {
    publicNetworkAccess: 'Enabled'
  }
}

// --- Topic: fraud-events ---
resource sbTopic 'Microsoft.ServiceBus/namespaces/topics@2022-10-01-preview' = {
  parent: sbNamespace
  name: topicName
  properties: {
    defaultMessageTimeToLive: 'P7D' // 7 days TTL
    maxSizeInMegabytes: 1024
    requiresDuplicateDetection: true
    duplicateDetectionHistoryTimeWindow: 'PT10M' // 10-minute dup detection window
    enablePartitioning: false // Standard tier: not partitioned
  }
}

// --- Subscription 1: Step-Up Auth Workflow ---
resource subStepUp 'Microsoft.ServiceBus/namespaces/topics/subscriptions@2022-10-01-preview' = {
  parent: sbTopic
  name: 'sub-stepup-auth'
  properties: {
    maxDeliveryCount: 5
    deadLetteringOnMessageExpiration: true
    enableBatchedOperations: true
    lockDuration: 'PT1M'
  }
}

resource ruleStepUp 'Microsoft.ServiceBus/namespaces/topics/subscriptions/rules@2022-10-01-preview' = {
  parent: subStepUp
  name: 'FilterStepUp'
  properties: {
    filterType: 'CorrelationFilter'
    correlationFilter: {
      properties: {
        action: 'step_up'
      }
    }
  }
}

// --- Subscription 2: Case Management ---
resource subCaseMgmt 'Microsoft.ServiceBus/namespaces/topics/subscriptions@2022-10-01-preview' = {
  parent: sbTopic
  name: 'sub-case-mgmt'
  properties: {
    maxDeliveryCount: 5
    deadLetteringOnMessageExpiration: true
    enableBatchedOperations: true
    lockDuration: 'PT1M'
  }
}

// --- Subscription 3: Compliance Audit Log ---
resource subAuditLog 'Microsoft.ServiceBus/namespaces/topics/subscriptions@2022-10-01-preview' = {
  parent: sbTopic
  name: 'sub-audit-log'
  properties: {
    maxDeliveryCount: 10
    deadLetteringOnMessageExpiration: true
    enableBatchedOperations: true
    lockDuration: 'PT1M'
  }
}

// --- Authorization Rules ---
resource sendAuthRule 'Microsoft.ServiceBus/namespaces/topics/authorizationRules@2022-10-01-preview' = {
  parent: sbTopic
  name: 'publisher-send-rule'
  properties: { rights: ['Send'] }
}

resource listenAuthRule 'Microsoft.ServiceBus/namespaces/topics/authorizationRules@2022-10-01-preview' = {
  parent: sbTopic
  name: 'consumer-listen-rule'
  properties: { rights: ['Listen'] }
}

output namespaceName string = sbNamespace.name
output topicName string = topicName
output sendConnectionString string = sendAuthRule.listKeys().primaryConnectionString
output listenConnectionString string = listenAuthRule.listKeys().primaryConnectionString

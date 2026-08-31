# --- Namespace (Standard tier required for Topics) ---
resource "azurerm_servicebus_namespace" "this" {
  name                = "sbns-fraud-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "Standard"

  public_network_access_enabled = true # Free Trial: no private endpoint
  minimum_tls_version            = "1.2"
  # local_auth_enabled stays at its default (true) -- decision_engine/audit_logger/
  # dlq_monitor and the DLQ replay tooling all still authenticate via SAS
  # connection strings (see docs/execution-log/07-decision-engine.md); only the
  # decision_function identity has been migrated to a managed-identity RBAC role
  # so far. Disabling local auth entirely would break the rest immediately.

  tags = {
    project      = var.project_name
    environment  = var.environment
    "managed-by" = "terraform"
  }
}

# --- Topic: fraud-events ---
resource "azurerm_servicebus_topic" "fraud_events" {
  name         = "sb-topic-fraud-events"
  namespace_id = azurerm_servicebus_namespace.this.id

  default_message_ttl                     = "P7D" # 7 days TTL
  max_size_in_megabytes                   = 1024
  requires_duplicate_detection            = true
  duplicate_detection_history_time_window = "PT10M" # 10-minute dup detection window
  partitioning_enabled                    = false   # Standard tier: not partitioned
}

# --- Subscription 1: Step-Up Auth Workflow (filtered to step_up actions only) ---
resource "azurerm_servicebus_subscription" "step_up" {
  name                                 = "sub-stepup-auth"
  topic_id                             = azurerm_servicebus_topic.fraud_events.id
  max_delivery_count                   = 5
  dead_lettering_on_message_expiration = true
  batched_operations_enabled           = true
  lock_duration                        = "PT1M"
}

resource "azurerm_servicebus_subscription_rule" "filter_step_up" {
  name            = "FilterStepUp"
  subscription_id = azurerm_servicebus_subscription.step_up.id
  filter_type     = "CorrelationFilter"

  correlation_filter {
    properties = {
      action = "step_up"
    }
  }
}

# --- Subscription 2: Case Management (unfiltered -- catches every published decision) ---
resource "azurerm_servicebus_subscription" "case_mgmt" {
  name                                 = "sub-case-mgmt"
  topic_id                             = azurerm_servicebus_topic.fraud_events.id
  max_delivery_count                   = 5
  dead_lettering_on_message_expiration = true
  batched_operations_enabled           = true
  lock_duration                        = "PT1M"
}

# --- Subscription 3: Compliance Audit Log ---
resource "azurerm_servicebus_subscription" "audit_log" {
  name                                 = "sub-audit-log"
  topic_id                             = azurerm_servicebus_topic.fraud_events.id
  max_delivery_count                   = 10
  dead_lettering_on_message_expiration = true
  batched_operations_enabled           = true
  lock_duration                        = "PT1M"
}

# --- Authorization Rules (topic-level, least privilege) ---
resource "azurerm_servicebus_topic_authorization_rule" "publisher_send" {
  name     = "publisher-send-rule"
  topic_id = azurerm_servicebus_topic.fraud_events.id

  send   = true
  listen = false
  manage = false
}

resource "azurerm_servicebus_topic_authorization_rule" "consumer_listen" {
  name     = "consumer-listen-rule"
  topic_id = azurerm_servicebus_topic.fraud_events.id

  send   = false
  listen = true
  manage = false
}

# --- DLQ replay tooling needs both send (to re-publish) and listen (to
# receive from the dead-letter sub-queue) on one connection, but never
# manage -- narrower than the namespace root key. ---
resource "azurerm_servicebus_topic_authorization_rule" "dlq_replay" {
  name     = "dlq-replay-rule"
  topic_id = azurerm_servicebus_topic.fraud_events.id

  send   = true
  listen = true
  manage = false
}

output "namespace_id" {
  value = azurerm_servicebus_namespace.this.id
}

output "namespace_name" {
  value = azurerm_servicebus_namespace.this.name
}

output "topic_name" {
  value = azurerm_servicebus_topic.fraud_events.name
}

output "send_connection_string" {
  value     = azurerm_servicebus_topic_authorization_rule.publisher_send.primary_connection_string
  sensitive = true
}

output "listen_connection_string" {
  value     = azurerm_servicebus_topic_authorization_rule.consumer_listen.primary_connection_string
  sensitive = true
}

output "dlq_replay_connection_string" {
  value     = azurerm_servicebus_topic_authorization_rule.dlq_replay.primary_connection_string
  sensitive = true
}

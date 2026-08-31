output "namespace_id" {
  value = azurerm_eventhub_namespace.this.id
}

output "namespace_name" {
  value = azurerm_eventhub_namespace.this.name
}

output "eventhub_name" {
  value = azurerm_eventhub.transactions.name
}

output "producer_connection_string" {
  value     = azurerm_eventhub_authorization_rule.producer_send.primary_connection_string
  sensitive = true
}

output "consumer_connection_string" {
  value     = azurerm_eventhub_authorization_rule.consumer_listen.primary_connection_string
  sensitive = true
}

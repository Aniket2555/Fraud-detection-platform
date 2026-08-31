output "cosmos_endpoint" {
  value = azurerm_cosmosdb_account.this.endpoint
}

output "cosmos_account_id" {
  value = azurerm_cosmosdb_account.this.id
}

output "database_name" {
  value = azurerm_cosmosdb_gremlin_database.this.name
}

output "graph_name" {
  value = azurerm_cosmosdb_gremlin_graph.entity_graph.name
}

output "primary_key" {
  value     = azurerm_cosmosdb_account.this.primary_key
  sensitive = true
}

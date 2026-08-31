output "resource_group_name" {
  value = module.resource_group.resource_group_name
}

output "storage_account_name" {
  value = module.storage_account.storage_account_name
}

output "key_vault_name" {
  value = module.key_vault.key_vault_name
}

output "databricks_workspace_url" {
  value = module.databricks_workspace.workspace_url
}

output "log_analytics_name" {
  value = module.log_analytics.log_analytics_name
}

output "eventhub_namespace_name" {
  value = module.eventhubs.namespace_name
}

output "cosmos_endpoint" {
  value = module.cosmos_db.cosmos_endpoint
}

output "service_bus_namespace_name" {
  value = module.service_bus.namespace_name
}

output "sql_server_fqdn" {
  value = module.azure_sql.sql_server_fqdn
}

output "app_configuration_endpoint" {
  value = module.app_configuration.config_store_endpoint
}

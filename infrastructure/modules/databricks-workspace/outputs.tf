output "workspace_id" {
  value = azurerm_databricks_workspace.this.id
}

output "workspace_url" {
  value = azurerm_databricks_workspace.this.workspace_url
}

output "workspace_name" {
  value = azurerm_databricks_workspace.this.name
}

output "storage_account_identity_principal_id" {
  description = "Principal ID of the workspace's own managed identity (used for its default/DBFS-root storage account) -- used to grant Databricks RBAC on the lakehouse storage account and Key Vault"
  value       = try(azurerm_databricks_workspace.this.storage_account_identity[0].principal_id, null)
}

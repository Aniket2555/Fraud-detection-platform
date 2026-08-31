output "log_analytics_id" {
  value = azurerm_log_analytics_workspace.this.id
}

output "log_analytics_name" {
  value = azurerm_log_analytics_workspace.this.name
}

output "workspace_id" {
  description = "The workspace's own GUID (distinct from the ARM resource ID) — needed by some diagnostic/agent configs"
  value       = azurerm_log_analytics_workspace.this.workspace_id
}

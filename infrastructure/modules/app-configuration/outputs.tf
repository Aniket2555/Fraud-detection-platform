output "config_store_id" {
  value = azurerm_app_configuration.this.id
}

output "config_store_name" {
  value = azurerm_app_configuration.this.name
}

output "config_store_endpoint" {
  value = azurerm_app_configuration.this.endpoint
}

# Read-only connection string -- sufficient for the Decision Engine Function's
# threshold cache, which only ever calls get_configuration_setting().
output "primary_read_connection_string" {
  value     = azurerm_app_configuration.this.primary_read_key[0].connection_string
  sensitive = true
}

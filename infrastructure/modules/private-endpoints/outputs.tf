output "storage_private_endpoint_id" {
  value = var.enable_private_endpoints ? azurerm_private_endpoint.storage[0].id : null
}

output "key_vault_private_endpoint_id" {
  value = var.enable_private_endpoints ? azurerm_private_endpoint.key_vault[0].id : null
}

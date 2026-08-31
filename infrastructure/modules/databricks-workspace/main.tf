# Free Trial: Premium SKU is required for Unity Catalog + cluster policies, and is
# included as a 14-day trial with the Azure Free Trial. No VNet injection --
# managed (default) VNet only.
resource "azurerm_databricks_workspace" "this" {
  name                = "dbw-fraud-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "premium"

  managed_resource_group_name   = "rg-dbw-fraud-${var.environment}-managed"
  public_network_access_enabled = true # Free Trial: always public

  tags = {
    project      = var.project_name
    environment  = var.environment
    "managed-by" = "terraform"
  }
}

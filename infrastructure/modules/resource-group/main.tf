resource "azurerm_resource_group" "this" {
  name     = "rg-${var.project_name}-${var.environment}"
  location = var.location

  tags = {
    project      = var.project_name
    environment  = var.environment
    owner        = var.owner_email
    "managed-by" = "terraform"
  }
}

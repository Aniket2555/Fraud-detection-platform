resource "azurerm_app_configuration" "this" {
  name                = "appcs-fraud-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "free" # Free Trial: Free tier (1,000 requests/day)

  public_network_access = "Enabled"

  tags = {
    project      = var.project_name
    environment  = var.environment
    "managed-by" = "terraform"
  }
}

# NOTE: writing keys requires the deploying identity to hold a data-plane role
# on the store (e.g. "App Configuration Data Owner"), not just ARM-level
# create permissions -- unlike Key Vault secrets, App Configuration key-values
# are not covered by resource-level RBAC alone. Grant that role to whichever
# identity/service principal runs `terraform apply` before the first run.
resource "azurerm_app_configuration_key" "approve_max_threshold" {
  configuration_store_id = azurerm_app_configuration.this.id
  key                    = "FraudEngine:ApproveMaxThreshold"
  value                  = "0.10"
  tags = {
    description = "Maximum score for auto-approve"
  }
}

resource "azurerm_app_configuration_key" "step_up_max_threshold" {
  configuration_store_id = azurerm_app_configuration.this.id
  key                    = "FraudEngine:StepUpMaxThreshold"
  value                  = "0.60"
  tags = {
    description = "Maximum score for step-up auth"
  }
}

resource "azurerm_app_configuration_key" "block_min_threshold" {
  configuration_store_id = azurerm_app_configuration.this.id
  key                    = "FraudEngine:BlockMinThreshold"
  value                  = "0.90"
  tags = {
    description = "Minimum score for auto-block"
  }
}

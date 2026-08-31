# Key Vault names are globally unique across ALL Azure tenants (like storage
# account DNS names) -- a short random suffix avoids collisions with other
# Azure customers without anyone having to hand-pick a unique name.
resource "random_string" "suffix" {
  length  = 4
  special = false
  upper   = false
}

resource "azurerm_key_vault" "this" {
  name                = "kv-fraud-${var.environment}-${random_string.suffix.result}"
  resource_group_name = var.resource_group_name
  location            = var.location
  tenant_id           = var.tenant_id

  sku_name = "standard" # Free Trial: always Standard (10k free ops/month)

  rbac_authorization_enabled    = true # Use RBAC, not access policies
  soft_delete_retention_days    = 90
  purge_protection_enabled      = false # Free Trial: disabled for easy cleanup
  public_network_access_enabled = true  # Free Trial: no private endpoint

  network_acls {
    default_action = "Allow" # Free Trial: allow all (no VNet)
    bypass         = "AzureServices"
  }

  tags = {
    project      = var.project_name
    environment  = var.environment
    "managed-by" = "terraform"
  }
}

# --- Deployer gets admin role ---
# NOTE: unlike the Bicep version, Terraform doesn't need a manually computed
# deterministic guid() for the role assignment's name -- azurerm_role_assignment
# auto-generates one, and Terraform's own state (not the resource name) is what
# tracks idempotency across re-applies.
resource "azurerm_role_assignment" "deployer_kv_admin" {
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = var.deployer_object_id
}

# --- Placeholder secrets for later phases ---
# Not all secrets exist yet at Phase 0. This ensures the Key Vault structure is
# correct and Databricks secret scope / application config reference the right
# names from day one.
resource "azurerm_key_vault_secret" "placeholders" {
  for_each = toset(var.placeholder_secret_names)

  name         = each.value
  value        = "PLACEHOLDER-TO-BE-SET-IN-PHASE-${each.value}"
  key_vault_id = azurerm_key_vault.this.id
  content_type = "text/plain"

  depends_on = [azurerm_role_assignment.deployer_kv_admin]
}

# --- Diagnostic settings ---
resource "azurerm_monitor_diagnostic_setting" "this" {
  name                       = "kv-fraud-${var.environment}-diagnostics"
  target_resource_id         = azurerm_key_vault.this.id
  log_analytics_workspace_id = var.log_analytics_workspace_id

  enabled_log {
    category = "AuditEvent"
  }

  enabled_metric {
    category = "AllMetrics"
  }
}

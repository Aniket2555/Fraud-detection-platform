# Entra ID RBAC matrix (Implementation-details/Phase_7_implementation_plan.md §7.3).
#
# Unlike the Bicep version, this module doesn't need `existing` resource
# lookups or a deterministic guid()-based name for each role assignment --
# every target resource is created in the same Terraform state (its ID is
# passed straight in as a variable from the root module), and Terraform's
# state itself (not a computed name) is what makes re-applying idempotent.
# `azurerm_role_assignment` auto-generates its own name/GUID when one isn't
# given, so none is set here.
#
# Role names are resolved against the live subscription's role definitions at
# plan/apply time -- a typo fails loudly ("role definition not found") rather
# than silently assigning the wrong role the way an unverified hardcoded GUID
# could.

# Terraform treats an explicit `null` passed into a provider-required
# argument as "not configured" (not as null) -- it errors with "Missing
# required argument" rather than silently accepting it. So each assignment
# below whose principal ID may not exist yet is skipped entirely via `count`
# rather than attempting to pass through a possibly-null/empty value.

# --- Databricks -> ADLS Gen2 Storage Blob Data Contributor ---
# Skipped when null: Unity-Catalog-enabled workspaces (isUcEnabled=true,
# which this one is) don't populate the classic per-workspace
# storage_account_identity -- UC governs storage access via an Access
# Connector + metastore credential instead. Revisit when that's wired up.
resource "azurerm_role_assignment" "databricks_storage" {
  count                = var.databricks_principal_id != null && var.databricks_principal_id != "" ? 1 : 0
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = var.databricks_principal_id
}

# --- Databricks -> Key Vault Secrets User ---
resource "azurerm_role_assignment" "databricks_key_vault" {
  count                = var.databricks_principal_id != null && var.databricks_principal_id != "" ? 1 : 0
  scope                = var.key_vault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = var.databricks_principal_id
}

# --- Decision Function -> Service Bus Data Sender ---
# Skipped until the Decision Function (Phase 5) is actually deployed and its
# principal ID is supplied.
resource "azurerm_role_assignment" "decision_function_service_bus" {
  count                = var.decision_function_principal_id != null && var.decision_function_principal_id != "" ? 1 : 0
  scope                = var.service_bus_namespace_id
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = var.decision_function_principal_id
}

# --- Decision Function -> App Configuration Data Reader ---
resource "azurerm_role_assignment" "decision_function_app_config" {
  count                = var.decision_function_principal_id != null && var.decision_function_principal_id != "" ? 1 : 0
  scope                = var.app_configuration_id
  role_definition_name = "App Configuration Data Reader"
  principal_id         = var.decision_function_principal_id
}

# --- Logic App -> Service Bus Data Receiver ---
# Skipped until the Logic App (Phase 5) is actually deployed and its
# principal ID is supplied.
resource "azurerm_role_assignment" "logic_app_service_bus" {
  count                = var.logic_app_principal_id != null && var.logic_app_principal_id != "" ? 1 : 0
  scope                = var.service_bus_namespace_id
  role_definition_name = "Azure Service Bus Data Receiver"
  principal_id         = var.logic_app_principal_id
}

# --- Logic App -> SQL DB Contributor ---
resource "azurerm_role_assignment" "logic_app_sql" {
  count                = var.logic_app_principal_id != null && var.logic_app_principal_id != "" ? 1 : 0
  scope                = var.sql_database_id
  role_definition_name = "SQL DB Contributor"
  principal_id         = var.logic_app_principal_id
}

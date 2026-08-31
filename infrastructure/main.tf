# ============================================================
# Fraud Detection Platform — Terraform Root Module
# FREE TRIAL VERSION — no VNet by default, no Azure ML
# ============================================================
#
# Unlike the previous Bicep main.bicep (which only ever orchestrated 5 of the
# 13 modules -- RG, Log Analytics, Key Vault, Storage, Databricks -- leaving
# Event Hubs, Cosmos DB, Service Bus, Azure SQL, App Configuration, RBAC, and
# Private Endpoints to be deployed by hand via separate `az deployment group
# create` calls per module), this root module wires ALL of them into one
# graph. That's not a stylistic choice -- Terraform's single-state model is
# what makes "deploy everything with one `terraform apply`" tractable, which
# was exactly the gap the Bicep version had (see Implementation-details/
# Phase_0_implementation_plan.md's "Known Issues": rbac-assignments.bicep was
# never called by main.bicep).
#
# Usage:
#   terraform init -backend-config=backend-dev.conf
#   terraform plan  -var-file=environments/dev.tfvars
#   terraform apply -var-file=environments/dev.tfvars

module "resource_group" {
  source = "./modules/resource-group"

  environment  = var.environment
  location     = var.location
  project_name = var.project_name
  owner_email  = var.owner_email
}

module "log_analytics" {
  source = "./modules/log-analytics"

  environment         = var.environment
  location            = var.location
  resource_group_name = module.resource_group.resource_group_name
  project_name        = var.project_name
}

module "key_vault" {
  source = "./modules/key-vault"

  environment                = var.environment
  location                   = var.location
  resource_group_name        = module.resource_group.resource_group_name
  project_name               = var.project_name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  deployer_object_id         = var.deployer_object_id
  log_analytics_workspace_id = module.log_analytics.log_analytics_id
}

module "storage_account" {
  source = "./modules/storage-account"

  environment         = var.environment
  location            = var.location
  resource_group_name = module.resource_group.resource_group_name
  project_name        = var.project_name
}

module "databricks_workspace" {
  source = "./modules/databricks-workspace"

  environment         = var.environment
  location            = var.location
  resource_group_name = module.resource_group.resource_group_name
  project_name        = var.project_name
}

module "eventhubs" {
  source = "./modules/eventhubs"

  environment         = var.environment
  location            = var.location
  resource_group_name = module.resource_group.resource_group_name
  project_name        = var.project_name
  storage_account_id  = module.storage_account.storage_account_id
}

module "cosmos_db" {
  source = "./modules/cosmos-db"

  environment         = var.environment
  location            = var.location
  resource_group_name = module.resource_group.resource_group_name
  project_name        = var.project_name
}

module "service_bus" {
  source = "./modules/service-bus"

  environment         = var.environment
  location            = var.location
  resource_group_name = module.resource_group.resource_group_name
  project_name        = var.project_name
}

module "azure_sql" {
  source = "./modules/azure-sql"

  environment         = var.environment
  location            = var.location
  resource_group_name = module.resource_group.resource_group_name
  project_name        = var.project_name
  sql_admin_login     = var.sql_admin_login
  sql_admin_password  = var.sql_admin_password
}

module "app_configuration" {
  source = "./modules/app-configuration"

  environment         = var.environment
  location            = var.location
  resource_group_name = module.resource_group.resource_group_name
  project_name        = var.project_name
}

# --- Phase 5: hosting for the 3 Decision Engine Azure Functions ---
module "function_app" {
  source = "./modules/function-app"

  environment                          = var.environment
  location                             = var.location
  resource_group_name                  = module.resource_group.resource_group_name
  project_name                         = var.project_name
  service_bus_send_connection_string   = module.service_bus.send_connection_string
  service_bus_listen_connection_string = module.service_bus.listen_connection_string
  service_bus_topic_name               = module.service_bus.topic_name
  app_config_read_connection_string    = module.app_configuration.primary_read_connection_string
  data_lake_storage_account_name       = module.storage_account.storage_account_name
  data_lake_storage_account_id         = module.storage_account.storage_account_id
}

# --- Phase 5: the 2 Logic App workflows (Case Management, Step-Up Auth) that
# consume from the Service Bus fan-out subscriptions and write case records
# to Azure SQL. See infrastructure/modules/logic-apps/main.tf for why the
# API connections use connection-string auth rather than managed identity.
module "logic_apps" {
  source = "./modules/logic-apps"

  environment                          = var.environment
  location                             = var.location
  resource_group_name                  = module.resource_group.resource_group_name
  project_name                         = var.project_name
  service_bus_listen_connection_string = module.service_bus.listen_connection_string
  sql_server_fqdn                      = module.azure_sql.sql_server_fqdn
  sql_database_name                    = module.azure_sql.sql_database_name
  sql_admin_login                      = var.sql_admin_login
  sql_admin_password                   = var.sql_admin_password
}

# --- Diagnostic settings: every major resource sends logs/metrics to Log
# Analytics. The Bicep version only ever wired this for Key Vault (hand-
# rolled inline) despite Phase 0's own spec claiming every resource did --
# closing that gap here since it's what the generic module was always for.
module "diagnostics" {
  source = "./modules/diagnostic-settings"

  for_each = {
    storage      = module.storage_account.storage_account_id
    databricks   = module.databricks_workspace.workspace_id
    eventhubs    = module.eventhubs.namespace_id
    cosmos_db    = module.cosmos_db.cosmos_account_id
    service_bus  = module.service_bus.namespace_id
    sql_database = module.azure_sql.sql_database_id
    app_config   = module.app_configuration.config_store_id
  }

  target_resource_name       = each.key
  target_resource_id         = each.value
  log_analytics_workspace_id = module.log_analytics.log_analytics_id

  # Storage accounts expose no log categories at the top-level resource --
  # only their sub-resources (blob/queue/table/file services) do, each a
  # separate resource ID; account-level metrics (Transaction, etc.) still work.
  enable_logs = each.key == "storage" ? false : true

  # Databricks workspaces don't support metric export via diagnostic settings.
  enable_metrics = each.key == "databricks" ? false : true
}

module "rbac_assignments" {
  source = "./modules/rbac-assignments"

  databricks_principal_id        = module.databricks_workspace.storage_account_identity_principal_id
  decision_function_principal_id = var.decision_function_principal_id
  logic_app_principal_id         = var.logic_app_principal_id

  storage_account_id       = module.storage_account.storage_account_id
  key_vault_id             = module.key_vault.key_vault_id
  service_bus_namespace_id = module.service_bus.namespace_id
  app_configuration_id     = module.app_configuration.config_store_id
  sql_database_id          = module.azure_sql.sql_database_id
}

module "private_endpoints" {
  source = "./modules/private-endpoints"

  environment                = var.environment
  location                   = var.location
  resource_group_name        = module.resource_group.resource_group_name
  enable_private_endpoints   = var.enable_private_endpoints
  vnet_id                    = var.vnet_id
  private_endpoint_subnet_id = var.private_endpoint_subnet_id
  storage_account_id         = module.storage_account.storage_account_id
  key_vault_id               = module.key_vault.key_vault_id
}

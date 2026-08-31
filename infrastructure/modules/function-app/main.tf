# --- Shared runtime storage account for all 3 Consumption-plan Function Apps ---
# (Consumption-plan Functions require a storage account for trigger/lease
# state; sharing one across the 3 small dev functions keeps this Free-Trial
# footprint minimal rather than provisioning 3 separate accounts.)
resource "random_string" "func_storage_suffix" {
  length  = 6
  special = false
  upper   = false
}

resource "azurerm_storage_account" "functions" {
  name                     = "stfuncfraud${var.environment}${random_string.func_storage_suffix.result}"
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"

  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false
  # shared_access_key_enabled stays at its default (true) -- the Consumption
  # plan Function Apps above authenticate to this account via
  # primary_access_key (AzureWebJobsStorage), a Microsoft-documented
  # requirement for the Functions runtime's trigger/lease state, not a
  # deferred hardening item like the data lake account's key access.

  blob_properties {
    delete_retention_policy {
      days = 7 # Free Trial: shortest retention to save storage, matching the data lake account
    }
  }

  sas_policy {
    expiration_period = "00.01:00:00"
    expiration_action = "Log"
  }

  tags = {
    project      = var.project_name
    environment  = var.environment
    "managed-by" = "terraform"
  }
}

resource "azurerm_service_plan" "functions" {
  name                = "asp-fraud-functions-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  os_type             = "Linux"
  sku_name            = "Y1" # Consumption plan -- first 1,000,000 executions/month free
}

locals {
  common_app_settings = {
    FUNCTIONS_WORKER_RUNTIME = "python"
  }
}

# --- Decision Engine: HTTP-triggered fast path, publishes to Service Bus ---
resource "azurerm_linux_function_app" "decision_engine" {
  name                = "func-fraud-decision-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location

  storage_account_name       = azurerm_storage_account.functions.name
  storage_account_access_key = azurerm_storage_account.functions.primary_access_key
  service_plan_id            = azurerm_service_plan.functions.id

  https_only = true

  identity {
    type = "SystemAssigned"
  }

  site_config {
    application_stack {
      python_version = "3.11"
    }
  }

  app_settings = merge(local.common_app_settings, {
    SERVICE_BUS_CONN_STR   = var.service_bus_send_connection_string
    SERVICE_BUS_TOPIC_NAME = var.service_bus_topic_name
    APP_CONFIG_CONN_STR    = var.app_config_read_connection_string
  })

  # WEBSITE_RUN_FROM_PACKAGE / SCM_DO_BUILD_DURING_DEPLOYMENT / ENABLE_ORYX_BUILD
  # are set by the deployment process (`az functionapp deployment source
  # config-zip`, `az functionapp config appsettings set` -- see
  # docs/execution-log/07-decision-engine.md), not Terraform. Without this,
  # every `terraform plan` after a code deploy shows a diff that would delete
  # them -- including WEBSITE_RUN_FROM_PACKAGE, which points at the currently
  # deployed code package; applying that diff would break the live function.
  lifecycle {
    ignore_changes = [app_settings]
  }

  tags = {
    project      = var.project_name
    environment  = var.environment
    "managed-by" = "terraform"
  }
}

# --- Audit Logger: Service Bus topic trigger, writes to ADLS Gen2 via managed identity ---
resource "azurerm_linux_function_app" "audit_logger" {
  name                = "func-fraud-audit-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location

  storage_account_name       = azurerm_storage_account.functions.name
  storage_account_access_key = azurerm_storage_account.functions.primary_access_key
  service_plan_id            = azurerm_service_plan.functions.id

  https_only = true

  identity {
    type = "SystemAssigned"
  }

  site_config {
    application_stack {
      python_version = "3.11"
    }
  }

  app_settings = merge(local.common_app_settings, {
    SERVICE_BUS_CONN_STR      = var.service_bus_listen_connection_string
    ADLS_STORAGE_ACCOUNT_NAME = var.data_lake_storage_account_name
  })

  lifecycle {
    ignore_changes = [app_settings]
  }

  tags = {
    project      = var.project_name
    environment  = var.environment
    "managed-by" = "terraform"
  }
}

# --- DLQ Monitor: timer-triggered, polls dead-letter queues across subscriptions ---
resource "azurerm_linux_function_app" "dlq_monitor" {
  name                = "func-fraud-dlqmon-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location

  storage_account_name       = azurerm_storage_account.functions.name
  storage_account_access_key = azurerm_storage_account.functions.primary_access_key
  service_plan_id            = azurerm_service_plan.functions.id

  https_only = true

  identity {
    type = "SystemAssigned"
  }

  site_config {
    application_stack {
      python_version = "3.11"
    }
  }

  app_settings = merge(local.common_app_settings, {
    # Needs both listen (receive from DLQ) and manage is NOT required --
    # ServiceBusClient.get_subscription_receiver only needs Listen claims.
    SERVICE_BUS_CONN_STR = var.service_bus_listen_connection_string
  })

  lifecycle {
    ignore_changes = [app_settings]
  }

  tags = {
    project      = var.project_name
    environment  = var.environment
    "managed-by" = "terraform"
  }
}

# --- RBAC: audit_logger's managed identity needs Storage Blob Data Contributor
# on the data lake to write audit records via ManagedIdentityCredential ---
resource "azurerm_role_assignment" "audit_logger_storage" {
  scope                = var.data_lake_storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_linux_function_app.audit_logger.identity[0].principal_id
}

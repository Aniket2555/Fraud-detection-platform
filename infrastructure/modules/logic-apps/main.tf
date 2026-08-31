# Deploys the two Consumption Logic App workflows whose definitions already
# existed as plain JSON under logic-apps/workflows/ but were never wired into
# any Terraform module (Implementation-details/Phase_5_implementation_plan.md's
# "Known Issues"). Triggers/actions are read directly from those JSON files via
# jsondecode(file(...)) instead of being duplicated into HCL, so the deployed
# workflow always matches what's committed there.
#
# Both API connections use connection-string / SQL-auth ("basic") parameter
# sets rather than OAuth or managed identity: those are the only auth types
# that deploy non-interactively (no browser consent flow), matching how every
# other component in this repo (the 3 Functions) authenticates to Service Bus
# and SQL — see infrastructure/modules/function-app/main.tf.

locals {
  case_mgmt_workflow = jsondecode(file("${path.module}/../../../logic-apps/workflows/workflow_case_management.json"))
  stepup_workflow    = jsondecode(file("${path.module}/../../../logic-apps/workflows/workflow_stepup_auth.json"))

  case_mgmt_trigger_name = keys(local.case_mgmt_workflow.triggers)[0]
  stepup_trigger_name    = keys(local.stepup_workflow.triggers)[0]

  connections_param = {
    servicebus = {
      connectionId   = azurerm_api_connection.servicebus.id
      connectionName = azurerm_api_connection.servicebus.name
      id             = data.azurerm_managed_api.servicebus.id
    }
    sql = {
      connectionId   = azurerm_api_connection.sql.id
      connectionName = azurerm_api_connection.sql.name
      id             = data.azurerm_managed_api.sql.id
    }
  }

  common_tags = {
    project      = var.project_name
    environment  = var.environment
    "managed-by" = "terraform"
  }
}

data "azurerm_managed_api" "servicebus" {
  name     = "servicebus"
  location = var.location
}

data "azurerm_managed_api" "sql" {
  name     = "sql"
  location = var.location
}

resource "azurerm_api_connection" "servicebus" {
  name                = "servicebus-fraud-${var.environment}"
  resource_group_name = var.resource_group_name
  managed_api_id      = data.azurerm_managed_api.servicebus.id
  display_name        = "servicebus"

  parameter_values = {
    connectionString = var.service_bus_listen_connection_string
  }

  # Terraform can't read back a write-only connectionString to diff it, and
  # re-applying the same value every plan is a no-op that just adds noise.
  lifecycle {
    ignore_changes = [parameter_values]
  }

  tags = local.common_tags
}

resource "azurerm_api_connection" "sql" {
  name                = "sql-fraud-${var.environment}"
  resource_group_name = var.resource_group_name
  managed_api_id      = data.azurerm_managed_api.sql.id
  display_name        = "sql"

  parameter_values = {
    server   = var.sql_server_fqdn
    database = var.sql_database_name
    username = var.sql_admin_login
    password = var.sql_admin_password
  }

  lifecycle {
    ignore_changes = [parameter_values]
  }

  tags = local.common_tags
}

# --- Workflow 1: Case Management (unfiltered — every published decision) ---
resource "azurerm_logic_app_workflow" "case_management" {
  name                = "logic-fraud-case-mgmt-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name

  workflow_parameters = {
    "$connections" = jsonencode({ defaultValue = {}, type = "Object" })
  }

  parameters = {
    "$connections" = jsonencode(local.connections_param)
  }

  tags = local.common_tags
}

resource "azurerm_logic_app_trigger_custom" "case_management" {
  name         = local.case_mgmt_trigger_name
  logic_app_id = azurerm_logic_app_workflow.case_management.id
  body         = jsonencode(local.case_mgmt_workflow.triggers[local.case_mgmt_trigger_name])
}

# The Logic Apps API rejects an action whose runAfter target doesn't exist in
# the workflow yet (verified live), so these need explicit creation order —
# depends_on only accepts a static list, which rules out a for_each here.
# body still reads from the JSON file so content edits stay in sync.
resource "azurerm_logic_app_action_custom" "case_management_parse" {
  name         = "Parse_Message_JSON"
  logic_app_id = azurerm_logic_app_workflow.case_management.id
  body         = jsonencode(local.case_mgmt_workflow.actions["Parse_Message_JSON"])
}

resource "azurerm_logic_app_action_custom" "case_management_upsert" {
  name         = "Upsert_Case_Record"
  logic_app_id = azurerm_logic_app_workflow.case_management.id
  body         = jsonencode(local.case_mgmt_workflow.actions["Upsert_Case_Record"])

  depends_on = [azurerm_logic_app_action_custom.case_management_parse]
}

# --- Workflow 2: Step-Up Auth (filtered to action == "step_up") ---
resource "azurerm_logic_app_workflow" "stepup_auth" {
  name                = "logic-fraud-stepup-auth-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name

  workflow_parameters = {
    "$connections" = jsonencode({ defaultValue = {}, type = "Object" })
  }

  parameters = {
    "$connections" = jsonencode(local.connections_param)
  }

  tags = local.common_tags
}

resource "azurerm_logic_app_trigger_custom" "stepup_auth" {
  name         = local.stepup_trigger_name
  logic_app_id = azurerm_logic_app_workflow.stepup_auth.id
  body         = jsonencode(local.stepup_workflow.triggers[local.stepup_trigger_name])
}

resource "azurerm_logic_app_action_custom" "stepup_auth_parse" {
  name         = "Parse_Message_JSON"
  logic_app_id = azurerm_logic_app_workflow.stepup_auth.id
  body         = jsonencode(local.stepup_workflow.actions["Parse_Message_JSON"])
}

resource "azurerm_logic_app_action_custom" "stepup_auth_upsert" {
  name         = "Upsert_Case_Record"
  logic_app_id = azurerm_logic_app_workflow.stepup_auth.id
  body         = jsonencode(local.stepup_workflow.actions["Upsert_Case_Record"])

  depends_on = [azurerm_logic_app_action_custom.stepup_auth_parse]
}

resource "azurerm_logic_app_action_custom" "stepup_auth_wait" {
  name         = "Wait_For_Customer_Response"
  logic_app_id = azurerm_logic_app_workflow.stepup_auth.id
  body         = jsonencode(local.stepup_workflow.actions["Wait_For_Customer_Response"])

  depends_on = [azurerm_logic_app_action_custom.stepup_auth_upsert]
}

resource "azurerm_logic_app_action_custom" "stepup_auth_escalate" {
  name         = "Escalate_If_Timeout"
  logic_app_id = azurerm_logic_app_workflow.stepup_auth.id
  body         = jsonencode(local.stepup_workflow.actions["Escalate_If_Timeout"])

  depends_on = [azurerm_logic_app_action_custom.stepup_auth_wait]
}

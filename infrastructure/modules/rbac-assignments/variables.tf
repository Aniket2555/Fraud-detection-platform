variable "databricks_principal_id" {
  description = "Databricks workspace system-assigned managed identity principal ID"
  type        = string
}

variable "decision_function_principal_id" {
  description = "Decision Function system-assigned managed identity principal ID"
  type        = string
}

variable "logic_app_principal_id" {
  description = "Logic App (step-up/review workflow) system-assigned managed identity principal ID"
  type        = string
}

variable "storage_account_id" {
  type = string
}

variable "key_vault_id" {
  type = string
}

variable "service_bus_namespace_id" {
  type = string
}

variable "app_configuration_id" {
  type = string
}

variable "sql_database_id" {
  type = string
}

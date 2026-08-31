variable "environment" {
  description = "Environment name — dev only for Free Trial"
  type        = string
  default     = "dev"
  validation {
    condition     = var.environment == "dev"
    error_message = "Only 'dev' is supported on the Azure Free Trial."
  }
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "centralindia"
}

variable "project_name" {
  type    = string
  default = "fraud-detection"
}

variable "owner_email" {
  description = "Owner email for tagging"
  type        = string
}

variable "deployer_object_id" {
  description = "Object ID of the deployer, for initial Key Vault Administrator access"
  type        = string
}

variable "sql_admin_login" {
  type    = string
  default = "fraudsqladmin"
}

variable "sql_admin_password" {
  description = "SQL Server admin password. Supply via TF_VAR_sql_admin_password env var or -var, never in a committed .tfvars file."
  type        = string
  sensitive   = true
}

variable "enable_private_endpoints" {
  description = "Enable private endpoints (false for Free Trial, true for production upgrade)"
  type        = bool
  default     = false
}

variable "vnet_id" {
  description = "VNet resource ID — only required when enable_private_endpoints = true (Free Trial has no VNet)"
  type        = string
  default     = ""
}

variable "private_endpoint_subnet_id" {
  description = "Private endpoint subnet resource ID — only required when enable_private_endpoints = true"
  type        = string
  default     = ""
}

# --- RBAC: principal IDs for identities NOT managed by this Terraform config ---
# Databricks' own workspace identity IS managed here (wired automatically from
# the databricks-workspace module's output below). The Decision Function and
# Logic App identities are not -- neither is provisioned by this IaC layer
# (no azurerm_linux_function_app / azurerm_logic_app_workflow module exists
# yet; see Implementation-details/Phase_5_implementation_plan.md's "Known
# Issues"). Supply their principal IDs manually once those resources exist,
# the same way the original Bicep module required.
variable "decision_function_principal_id" {
  description = "Decision Function system-assigned managed identity principal ID (from wherever the Function App is actually provisioned)"
  type        = string
  default     = ""
}

variable "logic_app_principal_id" {
  description = "Logic App system-assigned managed identity principal ID (from wherever the Logic App is actually provisioned)"
  type        = string
  default     = ""
}

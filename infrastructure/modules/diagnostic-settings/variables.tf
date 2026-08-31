variable "target_resource_name" {
  description = "Name of the target resource, used to build a readable diagnostic setting name"
  type        = string
}

variable "target_resource_id" {
  description = "Resource ID of the target resource to collect diagnostics from"
  type        = string
}

variable "log_analytics_workspace_id" {
  description = "Resource ID of the Log Analytics workspace"
  type        = string
}

variable "enable_logs" {
  description = "Whether to enable the allLogs category group -- off for resource types that don't support any log categories at this scope (e.g. a storage account's top-level resource; logs there live on its sub-resources instead)"
  type        = bool
  default     = true
}

variable "enable_metrics" {
  description = "Whether to enable AllMetrics -- off for resource types that don't support metric export via diagnostic settings (e.g. Databricks workspaces)"
  type        = bool
  default     = true
}

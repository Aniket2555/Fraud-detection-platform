variable "environment" {
  type = string
}

variable "location" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "project_name" {
  type    = string
  default = "fraud-detection"
}

variable "tenant_id" {
  description = "Azure AD tenant ID"
  type        = string
}

variable "deployer_object_id" {
  description = "Object ID of the deployer for initial Key Vault Administrator access"
  type        = string
}

variable "log_analytics_workspace_id" {
  type = string
}

variable "placeholder_secret_names" {
  description = "Secrets that don't have real values yet -- populated in later phases (Event Hubs in Phase 2, Redis/Cosmos in Phase 3, SQL/Service Bus/App Config in Phase 5)"
  type        = list(string)
  default = [
    "eventhub-conn-str",
    "eventhub-namespace",
    "redis-conn-str",
    "azure-sql-conn-str",
    "cosmos-db-conn-str",
    "service-bus-conn-str",
    "app-config-conn-str",
  ]
}

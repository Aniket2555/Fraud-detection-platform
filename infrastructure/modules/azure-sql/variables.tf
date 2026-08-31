variable "environment" {
  type    = string
  default = "dev"
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

variable "sql_admin_login" {
  description = "SQL Server admin login name"
  type        = string
  default     = "fraudsqladmin"
}

variable "sql_admin_password" {
  description = "SQL Server admin password. Pass via TF_VAR_sql_admin_password or -var, never commit a real value to a .tfvars file."
  type        = string
  sensitive   = true
}

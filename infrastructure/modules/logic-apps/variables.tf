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
  type = string
}

variable "service_bus_listen_connection_string" {
  description = "Topic-level Listen connection string — both workflows only ever read (get + implicit complete) from their subscription's message head"
  type        = string
  sensitive   = true
}

variable "sql_server_fqdn" {
  type = string
}

variable "sql_database_name" {
  type = string
}

variable "sql_admin_login" {
  type = string
}

variable "sql_admin_password" {
  type      = string
  sensitive = true
}

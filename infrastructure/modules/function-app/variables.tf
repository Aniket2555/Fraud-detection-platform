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

variable "service_bus_send_connection_string" {
  type      = string
  sensitive = true
}

variable "service_bus_listen_connection_string" {
  type      = string
  sensitive = true
}

variable "service_bus_topic_name" {
  type = string
}

variable "app_config_read_connection_string" {
  type      = string
  sensitive = true
}

variable "data_lake_storage_account_name" {
  type = string
}

variable "data_lake_storage_account_id" {
  type = string
}

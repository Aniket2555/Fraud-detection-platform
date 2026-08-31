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

variable "storage_account_id" {
  description = "ADLS Gen2 storage account resource ID (Capture destination)"
  type        = string
}

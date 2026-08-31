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

variable "vnet_id" {
  description = "VNet resource ID for private endpoint subnet + DNS zone links"
  type        = string
  default     = ""
}

variable "private_endpoint_subnet_id" {
  description = "Subnet resource ID for private endpoints"
  type        = string
  default     = ""
}

variable "storage_account_id" {
  type = string
}

variable "key_vault_id" {
  type = string
}

variable "enable_private_endpoints" {
  description = "Enable private endpoints (false for Free Trial, true for production)"
  type        = bool
  default     = false
}

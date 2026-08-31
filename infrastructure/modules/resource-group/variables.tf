variable "environment" {
  description = "Environment name — dev only for Free Trial"
  type        = string
  validation {
    condition     = var.environment == "dev"
    error_message = "Only 'dev' is supported on the Azure Free Trial."
  }
}

variable "location" {
  description = "Azure region for the resource group"
  type        = string
  default     = "centralindia"
}

variable "project_name" {
  description = "Project name used in the naming convention"
  type        = string
  default     = "fraud-detection"
}

variable "owner_email" {
  description = "Owner email for tagging / accountability"
  type        = string
}

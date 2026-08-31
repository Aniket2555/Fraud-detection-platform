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

variable "retention_days" {
  description = "Free Trial: 30 days (free tier includes 31)"
  type        = number
  default     = 30
}

variable "daily_cap_gb" {
  description = "Free Trial: 1 GB/day cap to stay within the 5 GB/month free tier"
  type        = number
  default     = 1
}

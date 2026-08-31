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

variable "containers" {
  description = "ADLS Gen2 filesystem containers to provision"
  type        = list(string)
  default = [
    "staging",
    "raw",
    "bronze",
    "silver",
    "gold",
    "quarantine",
    "checkpoints",
    "feature-store",
    "eventhubs-capture",
  ]
}

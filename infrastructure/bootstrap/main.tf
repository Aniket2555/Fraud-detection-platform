# ============================================================
# Terraform State Backend Bootstrap
# ============================================================
# Terraform can't create the storage account its own remote state lives in --
# that's a chicken-and-egg problem, so this is a small, SEPARATE config with
# its own LOCAL state (bootstrap.tfstate, gitignored), run once, by hand,
# before the main config's first `terraform init`.
#
# Usage (one-time, per environment):
#   cd infrastructure/bootstrap
#   terraform init
#   terraform apply -var="environment=dev"
#
# Then use the printed values to fill in ../backend-dev.conf and run the main
# config's `terraform init -backend-config=../backend-dev.conf`.

terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
  # Intentionally local state -- this config bootstraps the remote backend,
  # it can't use it.
}

provider "azurerm" {
  features {}
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "location" {
  type    = string
  default = "centralindia"
}

# Storage account names must be globally unique, 3-24 lowercase alphanumeric
# characters -- a short random suffix avoids collisions with other Azure
# customers without anyone having to hand-pick a unique name.
resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

resource "azurerm_resource_group" "tfstate" {
  name     = "rg-tfstate-fraud-${var.environment}"
  location = var.location

  tags = {
    project      = "fraud-detection"
    environment  = var.environment
    purpose      = "terraform-state"
    "managed-by" = "terraform-bootstrap"
  }
}

resource "azurerm_storage_account" "tfstate" {
  name                = "sttfstate${var.environment}${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.tfstate.name
  location            = azurerm_resource_group.tfstate.location

  account_tier                    = "Standard"
  account_replication_type        = "LRS" # Free Trial: cheapest redundancy; state is small
  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false # No reason Terraform state should ever be blob-public

  blob_properties {
    versioning_enabled = true # Recover a previous state version if something goes wrong
    delete_retention_policy {
      days = 30 # Recover the state file itself if accidentally deleted -- this is the one
                # storage account in the whole platform where that protection is unconditionally
                # worth it, Free Trial or not.
    }
  }

  sas_policy {
    expiration_period = "00.01:00:00"
    expiration_action  = "Log"
  }

  tags = {
    project      = "fraud-detection"
    environment  = var.environment
    purpose      = "terraform-state"
    "managed-by" = "terraform-bootstrap"
  }
}

resource "azurerm_storage_container" "tfstate" {
  name                  = "tfstate"
  storage_account_id    = azurerm_storage_account.tfstate.id
  container_access_type = "private"
}

output "resource_group_name" {
  value = azurerm_resource_group.tfstate.name
}

output "storage_account_name" {
  value = azurerm_storage_account.tfstate.name
}

output "container_name" {
  value = azurerm_storage_container.tfstate.name
}

output "backend_config_snippet" {
  description = "Paste these values into ../backend-<env>.conf"
  value       = <<-EOT
    resource_group_name  = "${azurerm_resource_group.tfstate.name}"
    storage_account_name = "${azurerm_storage_account.tfstate.name}"
    container_name        = "${azurerm_storage_container.tfstate.name}"
    key                   = "fraud-detection-${var.environment}.tfstate"
  EOT
}

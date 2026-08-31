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

  # Partial backend config -- environment-specific values (resource group,
  # storage account, container, state file key) are supplied at `terraform
  # init` time via -backend-config, e.g.:
  #   terraform init -backend-config=backend-dev.conf
  # See infrastructure/bootstrap/ for the one-time step that creates the
  # storage account this backend writes state into (Terraform can't bootstrap
  # its own remote state store).
  backend "azurerm" {}
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
  }
}

data "azurerm_client_config" "current" {}

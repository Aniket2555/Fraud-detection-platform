resource "azurerm_storage_account" "this" {
  name                = "stfraudlake${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location

  account_tier             = "Standard"
  account_replication_type = "LRS" # Free Trial: cheapest redundancy
  account_kind             = "StorageV2"

  is_hns_enabled = true # Hierarchical namespace = ADLS Gen2

  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = true # Free Trial: needed for some Databricks operations
  default_to_oauth_authentication = true
  access_tier                     = "Hot"

  network_rules {
    default_action = "Allow" # Free Trial: no VNet/private endpoints
    bypass         = ["AzureServices"]
  }

  blob_properties {
    delete_retention_policy {
      days = 7 # Free Trial: shortest retention to save storage
    }
    container_delete_retention_policy {
      days = 7
    }
    # No blob versioning -- Delta Lake handles versioning via its transaction log
  }

  sas_policy {
    expiration_period = "00.01:00:00" # 1 day -- free to set, bounds SAS token lifetime
    expiration_action = "Log"
  }

  tags = {
    project      = var.project_name
    environment  = var.environment
    "managed-by" = "terraform"
  }
}

# --- Filesystem containers (ADLS Gen2) ---
# staging: human/Kaggle-API upload landing zone (pl_ingest_ieee_cis copies staging -> raw/ieee-cis/)
resource "azurerm_storage_data_lake_gen2_filesystem" "containers" {
  for_each = toset(var.containers)

  name               = each.value
  storage_account_id = azurerm_storage_account.this.id
}

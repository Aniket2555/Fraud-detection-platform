# Cosmos DB account names are globally unique across ALL Azure tenants -- a
# short random suffix avoids collisions with other Azure customers without
# anyone having to hand-pick a unique name.
resource "random_string" "suffix" {
  length  = 4
  special = false
  upper   = false
}

resource "azurerm_cosmosdb_account" "this" {
  name                = "cosmos-fraud-${var.environment}-${random_string.suffix.result}"
  resource_group_name = var.resource_group_name
  location            = var.location
  offer_type          = "Standard"
  kind                = "GlobalDocumentDB"

  capabilities {
    name = "EnableGremlin"
  }
  capabilities {
    name = "EnableServerless" # Free Trial: Serverless SKU (pay per RU consumed)
  }

  consistency_policy {
    consistency_level = "Session"
  }

  geo_location {
    location          = var.location
    failover_priority = 0
  }

  tags = {
    project      = var.project_name
    environment  = var.environment
    "managed-by" = "terraform"
  }
}

resource "azurerm_cosmosdb_gremlin_database" "this" {
  name                = "graph-fraud-db"
  resource_group_name = var.resource_group_name
  account_name        = azurerm_cosmosdb_account.this.name
}

resource "azurerm_cosmosdb_gremlin_graph" "entity_graph" {
  name                = "entity-graph"
  resource_group_name = var.resource_group_name
  account_name        = azurerm_cosmosdb_account.this.name
  database_name       = azurerm_cosmosdb_gremlin_database.this.name
  partition_key_path  = "/partitionKey"

  index_policy {
    automatic      = true
    indexing_mode  = "consistent"
    included_paths = ["/*"]
    excluded_paths = ["/\"_etag\"/?"]
  }
}

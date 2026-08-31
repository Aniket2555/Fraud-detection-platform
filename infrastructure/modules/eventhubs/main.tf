resource "azurerm_eventhub_namespace" "this" {
  name                = "ehns-fraud-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location

  sku      = "Standard" # Free Trial: Standard ($11/month base)
  capacity = 1          # 1 TU -- sufficient for dev throughput

  auto_inflate_enabled          = false # Free Trial: disabled (cost control)
  public_network_access_enabled = true  # Free Trial: no private endpoints
  local_authentication_enabled  = true  # Allow SAS auth (simpler for Free Trial)

  tags = {
    project      = var.project_name
    environment  = var.environment
    "managed-by" = "terraform"
  }
}

resource "azurerm_eventhub" "transactions" {
  name              = "eh-transactions"
  namespace_id      = azurerm_eventhub_namespace.this.id
  partition_count   = 4 # Free Trial: 4 partitions (Standard tier max: 32)
  message_retention = 1 # Free Trial: minimum retention (free)

  capture_description {
    enabled             = true # Always-on Capture for disaster recovery
    encoding            = "Avro"
    interval_in_seconds = 900       # 15-minute window (minimizes write cost)
    size_limit_in_bytes = 314572800 # 300 MB window
    skip_empty_archives = true      # Don't write empty Avro files

    destination {
      name                = "EventHubArchive.AzureBlockBlob"
      archive_name_format = "{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}"
      blob_container_name = "eventhubs-capture"
      storage_account_id  = var.storage_account_id
    }
  }
}

# --- Consumer Groups ---
# NOTE: "$Default" is not declared here -- Azure creates it automatically with
# every Event Hub, and azurerm_eventhub_consumer_group's name validation
# rejects the literal string "$Default" (only letters/numbers/./-/_ allowed),
# so it can't be brought under Terraform management as a distinct resource
# anyway. Only the custom consumer group needs to be declared.
resource "azurerm_eventhub_consumer_group" "bronze_ingest" {
  name                = "bronze-ingest"
  namespace_name      = azurerm_eventhub_namespace.this.name
  eventhub_name       = azurerm_eventhub.transactions.name
  resource_group_name = var.resource_group_name
}

# --- Authorization Rules (least privilege: separate send-only / listen-only) ---
resource "azurerm_eventhub_authorization_rule" "producer_send" {
  name                = "producer-send-rule"
  namespace_name      = azurerm_eventhub_namespace.this.name
  eventhub_name       = azurerm_eventhub.transactions.name
  resource_group_name = var.resource_group_name

  send   = true
  listen = false
  manage = false
}

resource "azurerm_eventhub_authorization_rule" "consumer_listen" {
  name                = "consumer-listen-rule"
  namespace_name      = azurerm_eventhub_namespace.this.name
  eventhub_name       = azurerm_eventhub.transactions.name
  resource_group_name = var.resource_group_name

  send   = false
  listen = true
  manage = false
}

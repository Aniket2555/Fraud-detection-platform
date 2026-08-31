resource "azurerm_mssql_server" "this" {
  name                = "sql-fraud-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location

  administrator_login           = var.sql_admin_login
  administrator_login_password  = var.sql_admin_password
  version                       = "12.0"
  public_network_access_enabled = true

  tags = {
    project      = var.project_name
    environment  = var.environment
    "managed-by" = "terraform"
  }
}

resource "azurerm_mssql_firewall_rule" "allow_azure_services" {
  name             = "AllowAllWindowsAzureIps"
  server_id        = azurerm_mssql_server.this.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

# --- Serverless General Purpose GP_S_Gen5_1, auto-pause after 60 min idle ---
resource "azurerm_mssql_database" "cases" {
  name      = "sqldb-fraud-cases-${var.environment}"
  server_id = azurerm_mssql_server.this.id

  sku_name                    = "GP_S_Gen5_1"
  auto_pause_delay_in_minutes = 60
  min_capacity                = 0.5
  max_size_gb                 = 32
  zone_redundant              = false

  tags = {
    project      = var.project_name
    environment  = var.environment
    "managed-by" = "terraform"
  }
}

# Free Trial: 30-day retention (free tier) + 1 GB/day cap (stays within the 5 GB/month free tier)
resource "azurerm_log_analytics_workspace" "this" {
  name                = "log-fraud-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "PerGB2018"
  retention_in_days   = var.retention_days
  daily_quota_gb      = var.daily_cap_gb > 0 ? var.daily_cap_gb : null

  tags = {
    project      = var.project_name
    environment  = var.environment
    "managed-by" = "terraform"
  }
}

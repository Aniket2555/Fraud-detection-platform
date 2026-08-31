# Generic, reusable diagnostic-settings module -- attach Log Analytics
# diagnostics to any resource by passing its resource ID.
#
# Not every resource type supports both blocks (e.g. Databricks workspaces
# reject enabled_metric with "Metric export is not enabled"), so both are
# toggleable -- callers turn off what the target resource type doesn't
# support.
resource "azurerm_monitor_diagnostic_setting" "this" {
  name                       = "diag-${var.target_resource_name}"
  target_resource_id         = var.target_resource_id
  log_analytics_workspace_id = var.log_analytics_workspace_id

  dynamic "enabled_log" {
    for_each = var.enable_logs ? [1] : []
    content {
      category_group = "allLogs"
    }
  }

  dynamic "enabled_metric" {
    for_each = var.enable_metrics ? [1] : []
    content {
      category = "AllMetrics"
    }
  }
}

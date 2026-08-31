output "decision_engine_name" {
  value = azurerm_linux_function_app.decision_engine.name
}

output "decision_engine_default_hostname" {
  value = azurerm_linux_function_app.decision_engine.default_hostname
}

output "audit_logger_name" {
  value = azurerm_linux_function_app.audit_logger.name
}

output "dlq_monitor_name" {
  value = azurerm_linux_function_app.dlq_monitor.name
}

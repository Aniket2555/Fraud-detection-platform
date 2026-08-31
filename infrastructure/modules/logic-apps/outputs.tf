output "case_management_workflow_id" {
  value = azurerm_logic_app_workflow.case_management.id
}

output "case_management_workflow_name" {
  value = azurerm_logic_app_workflow.case_management.name
}

output "stepup_auth_workflow_id" {
  value = azurerm_logic_app_workflow.stepup_auth.id
}

output "stepup_auth_workflow_name" {
  value = azurerm_logic_app_workflow.stepup_auth.name
}

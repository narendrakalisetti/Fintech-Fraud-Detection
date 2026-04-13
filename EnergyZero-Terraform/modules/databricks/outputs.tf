output "workspace_id"             { value = azurerm_databricks_workspace.main.id }
output "workspace_url"            { value = "https://${azurerm_databricks_workspace.main.workspace_url}" }
output "databricks_principal_id"  { value = azurerm_databricks_workspace.main.storage_account_identity[0].principal_id }
output "sql_warehouse_endpoint"   {
  description = "Placeholder — configure Databricks SQL Warehouse after workspace deploy"
  value       = "Configure via Databricks UI: SQL Warehouses > Connection Details > JDBC URL"
}

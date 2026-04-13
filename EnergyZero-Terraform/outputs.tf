output "resource_group_name" {
  description = "Name of the main resource group"
  value       = azurerm_resource_group.main.name
}

output "adls_storage_account_name" {
  description = "ADLS Gen2 storage account name"
  value       = module.adls.storage_account_name
}

output "adls_storage_account_id" {
  description = "ADLS Gen2 storage account resource ID"
  value       = module.adls.storage_account_id
}

output "key_vault_id" {
  description = "Azure Key Vault resource ID"
  value       = module.key_vault.key_vault_id
}

output "key_vault_uri" {
  description = "Azure Key Vault URI for application config"
  value       = module.key_vault.key_vault_uri
}

output "adf_id" {
  description = "Azure Data Factory resource ID"
  value       = module.adf.adf_id
}

output "adf_principal_id" {
  description = "ADF managed identity principal ID (for RBAC)"
  value       = module.adf.adf_principal_id
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = module.databricks.workspace_url
}

output "databricks_workspace_id" {
  description = "Databricks workspace resource ID"
  value       = module.databricks.workspace_id
}

output "log_analytics_workspace_id" {
  description = "Log Analytics Workspace ID"
  value       = module.monitoring.workspace_id
}

output "purview_account_id" {
  description = "Microsoft Purview account resource ID"
  value       = module.purview.purview_account_id
}

output "purview_catalog_endpoint" {
  description = "Purview catalog endpoint for data governance queries"
  value       = module.purview.catalog_endpoint
}

# Power BI DirectQuery connection details
output "gold_layer_dfs_endpoint" {
  description = "ADLS Gen2 DFS endpoint for Power BI DirectQuery connection to gold layer"
  value       = "abfss://gold@${module.adls.storage_account_name}.dfs.core.windows.net/"
}

output "databricks_sql_warehouse_endpoint" {
  description = "Databricks SQL warehouse JDBC endpoint for Power BI DirectQuery"
  value       = module.databricks.sql_warehouse_endpoint
}

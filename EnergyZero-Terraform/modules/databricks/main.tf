/*
  Module: Azure Databricks
  Provisions: Workspace with VNet injection, cluster policy (auto-terminate),
              managed identity, diagnostic settings
*/

resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-uks-${var.project_name}-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = var.databricks_sku

  custom_parameters {
    virtual_network_id                                   = var.vnet_id
    public_subnet_name                                   = var.databricks_public_subnet_name
    private_subnet_name                                  = var.databricks_private_subnet_name
    public_subnet_network_security_group_association_id  = var.public_nsg_association_id
    private_subnet_network_security_group_association_id = var.private_nsg_association_id
    no_public_ip                                         = true  # VNet injection — no public IP
  }

  managed_resource_group_name = "rg-uks-${var.project_name}-${var.environment}-databricks-managed"

  tags = var.tags
}

# Diagnostic settings
resource "azurerm_monitor_diagnostic_setting" "databricks" {
  name                       = "diag-dbw-${var.project_name}"
  target_resource_id         = azurerm_databricks_workspace.main.id
  log_analytics_workspace_id = var.log_analytics_workspace_id

  enabled_log { category = "dbfs" }
  enabled_log { category = "clusters" }
  enabled_log { category = "accounts" }
  enabled_log { category = "jobs" }
  enabled_log { category = "notebook" }
  enabled_log { category = "ssh" }
  enabled_log { category = "workspace" }
}

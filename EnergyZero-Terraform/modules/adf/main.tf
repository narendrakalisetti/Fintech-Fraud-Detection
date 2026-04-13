/*
  Module: Azure Data Factory
  Provisions: ADF workspace, managed identity, linked services,
              diagnostic settings
*/

resource "azurerm_data_factory" "main" {
  name                = "adf-uks-${var.project_name}-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }

  # Managed virtual network — all traffic stays within Azure
  managed_virtual_network_enabled = true

  tags = var.tags
}

# Linked Service: ADLS Gen2 via managed identity
resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "adls" {
  name                 = "ls_adls_${replace(var.project_name, "-", "_")}"
  data_factory_id      = azurerm_data_factory.main.id
  url                  = "https://${var.adls_storage_account_name}.dfs.core.windows.net"
  use_managed_identity = true  # No SAS keys or connection strings
}

# Linked Service: Key Vault (secrets referenced at runtime)
resource "azurerm_data_factory_linked_service_key_vault" "kv" {
  name            = "ls_keyvault_${replace(var.project_name, "-", "_")}"
  data_factory_id = azurerm_data_factory.main.id
  key_vault_id    = var.key_vault_id
}

# Diagnostic settings
resource "azurerm_monitor_diagnostic_setting" "adf" {
  name                       = "diag-adf-${var.project_name}"
  target_resource_id         = azurerm_data_factory.main.id
  log_analytics_workspace_id = var.log_analytics_workspace_id

  enabled_log { category = "ActivityRuns" }
  enabled_log { category = "PipelineRuns" }
  enabled_log { category = "TriggerRuns" }
  metric { category = "AllMetrics" }
}

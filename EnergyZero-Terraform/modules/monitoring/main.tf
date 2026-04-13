/*
  Module: Monitoring
  Provisions: Log Analytics Workspace, alert rules, action groups
*/

resource "azurerm_log_analytics_workspace" "main" {
  name                = "law-uks-${var.project_name}-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "PerGB2018"
  retention_in_days   = var.retention_days  # >= 90 for UK GDPR Art. 30

  tags = var.tags
}

# Action group for alerts
resource "azurerm_monitor_action_group" "data_engineering" {
  name                = "ag-de-${var.project_name}-${var.environment}"
  resource_group_name = var.resource_group_name
  short_name          = "DataEng"

  email_receiver {
    name          = "DataEngineeringTeam"
    email_address = var.alert_email
  }

  tags = var.tags
}

# Alert: ADF pipeline failure
resource "azurerm_monitor_scheduled_query_rules_alert_v2" "adf_failure" {
  name                = "alert-adf-failure-${var.project_name}"
  resource_group_name = var.resource_group_name
  location            = var.location
  display_name        = "ADF Pipeline Failure"
  description         = "Fires when any ADF pipeline run fails"
  severity            = 1
  enabled             = true

  scopes                  = [azurerm_log_analytics_workspace.main.id]
  evaluation_frequency    = "PT5M"
  window_duration         = "PT5M"

  criteria {
    query                   = <<-QUERY
      AzureDiagnostics
      | where ResourceType == "FACTORIES/PIPELINERUNS"
      | where status_s == "Failed"
      | summarize count() by bin(TimeGenerated, 5m)
      | where count_ > 0
    QUERY
    time_aggregation_method = "Count"
    threshold               = 0
    operator                = "GreaterThan"
  }

  action {
    action_groups = [azurerm_monitor_action_group.data_engineering.id]
  }

  tags = var.tags
}

# Alert: Databricks job failure
resource "azurerm_monitor_scheduled_query_rules_alert_v2" "databricks_failure" {
  name                = "alert-dbx-failure-${var.project_name}"
  resource_group_name = var.resource_group_name
  location            = var.location
  display_name        = "Databricks Job Failure"
  description         = "Fires when a Databricks job run fails"
  severity            = 1
  enabled             = true

  scopes               = [azurerm_log_analytics_workspace.main.id]
  evaluation_frequency = "PT5M"
  window_duration      = "PT5M"

  criteria {
    query = <<-QUERY
      AzureDiagnostics
      | where ResourceType == "DATABRICKS/WORKSPACES"
      | where Category == "jobs"
      | where OperationName == "runFailed"
      | summarize count() by bin(TimeGenerated, 5m)
      | where count_ > 0
    QUERY
    time_aggregation_method = "Count"
    threshold               = 0
    operator                = "GreaterThan"
  }

  action {
    action_groups = [azurerm_monitor_action_group.data_engineering.id]
  }

  tags = var.tags
}

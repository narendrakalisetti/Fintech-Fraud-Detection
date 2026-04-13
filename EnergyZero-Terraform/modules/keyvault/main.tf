/*
  Module: Azure Key Vault
  Provisions: Key Vault with CMK, RBAC, private endpoint, soft-delete
*/

resource "azurerm_key_vault" "main" {
  name                = "kv-uks-${var.project_name}-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  tenant_id           = var.tenant_id
  sku_name            = "premium"  # Premium required for HSM-backed CMK

  # RBAC model (not legacy Access Policies)
  enable_rbac_authorization = true

  # GDPR Art. 32 — protection against accidental deletion
  soft_delete_retention_days = 90
  purge_protection_enabled   = true

  # Network: deny all public access
  public_network_access_enabled = false

  network_acls {
    bypass         = ["AzureServices"]
    default_action = "Deny"
    ip_rules       = var.allowed_ip_ranges
  }

  tags = var.tags
}

# Private endpoint for Key Vault
resource "azurerm_private_endpoint" "key_vault" {
  name                = "pe-kv-${var.project_name}-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  subnet_id           = var.private_endpoint_subnet_id

  private_service_connection {
    name                           = "psc-kv"
    private_connection_resource_id = azurerm_key_vault.main.id
    subresource_names              = ["vault"]
    is_manual_connection           = false
  }

  tags = var.tags
}

# Diagnostic settings
resource "azurerm_monitor_diagnostic_setting" "key_vault" {
  name                       = "diag-kv-${var.project_name}"
  target_resource_id         = azurerm_key_vault.main.id
  log_analytics_workspace_id = var.log_analytics_workspace_id

  enabled_log { category = "AuditEvent" }
  enabled_log { category = "AzurePolicyEvaluationDetails" }
  metric { category = "AllMetrics" }
}

# CMK key for ADLS encryption
resource "azurerm_key_vault_key" "cmk" {
  name         = "cmk-adls-${var.project_name}"
  key_vault_id = azurerm_key_vault.main.id
  key_type     = "RSA-HSM"
  key_size     = 4096

  key_opts = ["decrypt", "encrypt", "sign", "unwrapKey", "verify", "wrapKey"]

  rotation_policy {
    automatic {
      time_before_expiry = "P30D"
    }
    expire_after         = "P365D"
    notify_before_expiry = "P30D"
  }

  depends_on = [azurerm_key_vault.main]
}

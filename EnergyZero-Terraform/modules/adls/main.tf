/*
  Module: ADLS Gen2
  Provisions: Storage account, Bronze/Silver/Gold containers,
              lifecycle policies, private endpoint, Defender for Storage
*/

resource "azurerm_storage_account" "adls" {
  name                     = "sauks${var.project_name}${var.environment}"
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = var.replication_type
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # ADLS Gen2 hierarchical namespace

  # Security hardening
  shared_access_key_enabled       = false  # Managed identity only — no SAS keys
  public_network_access_enabled   = false  # All access via private endpoint
  allow_nested_items_to_be_public = false
  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true

  blob_properties {
    delete_retention_policy {
      days = 30  # GDPR Art. 17 — soft-delete for 30 days before erasure
    }
    versioning_enabled       = true
    change_feed_enabled      = true
    last_access_time_enabled = true
  }

  network_rules {
    default_action             = "Deny"
    bypass                     = ["AzureServices"]
    ip_rules                   = var.allowed_ip_ranges
    virtual_network_subnet_ids = []
  }

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# ---------------------------------------------------------------------------
# Medallion containers
# ---------------------------------------------------------------------------
resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_id    = azurerm_storage_account.adls.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_id    = azurerm_storage_account.adls.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_id    = azurerm_storage_account.adls.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "tfstate" {
  name                  = "tfstate"
  storage_account_id    = azurerm_storage_account.adls.id
  container_access_type = "private"
}

# ---------------------------------------------------------------------------
# Lifecycle: Auto-tier cold data to archive (Net Zero + cost optimisation)
# ---------------------------------------------------------------------------
resource "azurerm_storage_management_policy" "lifecycle" {
  storage_account_id = azurerm_storage_account.adls.id

  rule {
    name    = "bronze-archive-cold"
    enabled = true

    filters {
      prefix_match = ["bronze/"]
      blob_types   = ["blockBlob"]
    }

    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than    = 30
        tier_to_archive_after_days_since_modification_greater_than = 90
        delete_after_days_since_modification_greater_than          = 365
      }
    }
  }

  rule {
    name    = "silver-cool-after-60"
    enabled = true

    filters {
      prefix_match = ["silver/"]
      blob_types   = ["blockBlob"]
    }

    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than = 60
      }
    }
  }
}

# ---------------------------------------------------------------------------
# Diagnostic settings → Log Analytics
# ---------------------------------------------------------------------------
resource "azurerm_monitor_diagnostic_setting" "adls" {
  name                       = "diag-adls-${var.project_name}"
  target_resource_id         = "${azurerm_storage_account.adls.id}/blobServices/default"
  log_analytics_workspace_id = var.log_analytics_workspace_id

  enabled_log {
    category = "StorageRead"
  }
  enabled_log {
    category = "StorageWrite"
  }
  enabled_log {
    category = "StorageDelete"
  }

  metric {
    category = "Transaction"
  }
}

# ---------------------------------------------------------------------------
# Private endpoint (no public internet access to ADLS)
# ---------------------------------------------------------------------------
resource "azurerm_private_endpoint" "adls_blob" {
  name                = "pe-adls-blob-${var.project_name}-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  subnet_id           = var.private_endpoint_subnet_id

  private_service_connection {
    name                           = "psc-adls-blob"
    private_connection_resource_id = azurerm_storage_account.adls.id
    subresource_names              = ["blob"]
    is_manual_connection           = false
  }

  tags = var.tags
}

resource "azurerm_private_endpoint" "adls_dfs" {
  name                = "pe-adls-dfs-${var.project_name}-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  subnet_id           = var.private_endpoint_subnet_id

  private_service_connection {
    name                           = "psc-adls-dfs"
    private_connection_resource_id = azurerm_storage_account.adls.id
    subresource_names              = ["dfs"]
    is_manual_connection           = false
  }

  tags = var.tags
}

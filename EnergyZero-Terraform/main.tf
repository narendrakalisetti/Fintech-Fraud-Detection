/*
=============================================================================
EnergyZero – Azure Data Engineering Platform
=============================================================================
Author      : Narendra Kalisetti
Project     : EnergyZero
Region      : UK South (uksouth)
Compliance  : UK GDPR | ISO 27001 | CIS Azure Benchmark | Net Zero 2050
Description : Root Terraform module. Provisions the full EnergyZero data
              engineering platform: remote state, networking, ADLS Gen2
              (medallion), Key Vault (CMK), ADF, Databricks, Purview,
              and monitoring.
GDPR        : All PII-adjacent data (customer energy consumption linked to
              account IDs) is pseudonymised at the silver layer using SHA-256.
              Raw customer identifiers never leave the bronze container.
              Data Controller: EnergyZero Ltd (UK ICO registration: ZB123456).
Net Zero    : Deployed exclusively in Azure UK South — 100% renewable energy.
              Databricks clusters auto-terminate after 20 minutes idle.
              ADLS lifecycle policies archive cold data automatically.
=============================================================================
*/

terraform {
  required_version = ">= 1.9.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Remote state with locking — MANDATORY for team environments.
  # Bootstrap with: bash scripts/bootstrap_state.sh
  backend "azurerm" {
    resource_group_name  = "rg-uks-energyzero-tfstate"
    storage_account_name = "sauksenergyzerotfstate"
    container_name       = "tfstate"
    key                  = "prod/energyzero-de.tfstate"
    # State locking is automatic via Azure Blob Storage lease mechanism.
    # Concurrent applies will fail-fast rather than corrupt state.
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = false # GDPR: never force-delete secrets
      recover_soft_deleted_key_vaults = true  # Recover on redeploy after soft-delete
    }
    resource_group {
      prevent_deletion_if_contains_resources = true
    }
  }
}

# ---------------------------------------------------------------------------
# Current subscription context
# ---------------------------------------------------------------------------
data "azurerm_client_config" "current" {}
data "azurerm_subscription" "current" {}

# ---------------------------------------------------------------------------
# Resource Group
# ---------------------------------------------------------------------------
resource "azurerm_resource_group" "main" {
  name     = "rg-uks-${var.project_name}-${var.environment}"
  location = var.location
  tags     = local.common_tags
}

# ---------------------------------------------------------------------------
# Module: Networking (VNet, subnets, private endpoints)
# ---------------------------------------------------------------------------
module "networking" {
  source              = "./modules/networking"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  project_name        = var.project_name
  environment         = var.environment
  tags                = local.common_tags
}

# ---------------------------------------------------------------------------
# Module: Monitoring (Log Analytics Workspace + Alerts)
# ---------------------------------------------------------------------------
module "monitoring" {
  source              = "./modules/monitoring"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  project_name        = var.project_name
  environment         = var.environment
  tags                = local.common_tags
}

# ---------------------------------------------------------------------------
# Module: ADLS Gen2 (Bronze / Silver / Gold medallion)
# ---------------------------------------------------------------------------
module "adls" {
  source                     = "./modules/adls"
  resource_group_name        = azurerm_resource_group.main.name
  location                   = var.location
  project_name               = var.project_name
  environment                = var.environment
  allowed_ip_ranges          = var.allowed_ip_ranges
  private_endpoint_subnet_id = module.networking.private_endpoint_subnet_id
  log_analytics_workspace_id = module.monitoring.workspace_id
  tags                       = local.common_tags
}

# ---------------------------------------------------------------------------
# Module: Azure Key Vault (secrets, CMK, RBAC)
# ---------------------------------------------------------------------------
module "key_vault" {
  source                     = "./modules/keyvault"
  resource_group_name        = azurerm_resource_group.main.name
  location                   = var.location
  project_name               = var.project_name
  environment                = var.environment
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  object_id                  = data.azurerm_client_config.current.object_id
  allowed_ip_ranges          = var.allowed_ip_ranges
  private_endpoint_subnet_id = module.networking.private_endpoint_subnet_id
  log_analytics_workspace_id = module.monitoring.workspace_id
  tags                       = local.common_tags
}

# ---------------------------------------------------------------------------
# Module: Azure Data Factory
# ---------------------------------------------------------------------------
module "adf" {
  source                     = "./modules/adf"
  resource_group_name        = azurerm_resource_group.main.name
  location                   = var.location
  project_name               = var.project_name
  environment                = var.environment
  key_vault_id               = module.key_vault.key_vault_id
  adls_storage_account_name  = module.adls.storage_account_name
  adls_storage_account_id    = module.adls.storage_account_id
  log_analytics_workspace_id = module.monitoring.workspace_id
  tags                       = local.common_tags
}

# ---------------------------------------------------------------------------
# Module: Databricks Workspace
# ---------------------------------------------------------------------------
module "databricks" {
  source                        = "./modules/databricks"
  resource_group_name           = azurerm_resource_group.main.name
  location                      = var.location
  project_name                  = var.project_name
  environment                   = var.environment
  databricks_subnet_id          = module.networking.databricks_public_subnet_id
  databricks_private_subnet_id  = module.networking.databricks_private_subnet_id
  key_vault_id                  = module.key_vault.key_vault_id
  adls_storage_account_id       = module.adls.storage_account_id
  log_analytics_workspace_id    = module.monitoring.workspace_id
  tags                          = local.common_tags
}

# ---------------------------------------------------------------------------
# Module: Microsoft Purview (Data Catalogue + Lineage + Classification)
# ---------------------------------------------------------------------------
module "purview" {
  source              = "./modules/purview"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  project_name        = var.project_name
  environment         = var.environment
  adls_storage_id     = module.adls.storage_account_id
  adf_id              = module.adf.adf_id
  tags                = local.common_tags
}

# ---------------------------------------------------------------------------
# RBAC: ADF managed identity → ADLS Gen2
# ---------------------------------------------------------------------------
resource "azurerm_role_assignment" "adf_adls_contributor" {
  scope                = module.adls.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = module.adf.adf_principal_id
  description          = "ADF reads/writes all medallion layers via managed identity"
}

# RBAC: ADF → Key Vault (read secrets only — least privilege)
resource "azurerm_role_assignment" "adf_kv_secrets_user" {
  scope                = module.key_vault.key_vault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = module.adf.adf_principal_id
  description          = "ADF retrieves connection strings from KV at runtime"
}

# RBAC: Databricks → ADLS Gen2
resource "azurerm_role_assignment" "databricks_adls_contributor" {
  scope                = module.adls.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = module.databricks.databricks_principal_id
  description          = "Databricks reads bronze, writes silver and gold via managed identity"
}

# RBAC: Databricks → Key Vault (read secrets)
resource "azurerm_role_assignment" "databricks_kv_secrets_user" {
  scope                = module.key_vault.key_vault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = module.databricks.databricks_principal_id
}

# RBAC: Purview → ADLS Gen2 (read for scanning)
resource "azurerm_role_assignment" "purview_adls_reader" {
  scope                = module.adls.storage_account_id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = module.purview.purview_principal_id
  description          = "Purview scans ADLS for data catalogue and lineage"
}

# ---------------------------------------------------------------------------
# Microsoft Defender for Storage
# ---------------------------------------------------------------------------
resource "azurerm_security_center_storage_defender" "adls" {
  storage_account_id                          = module.adls.storage_account_id
  malware_scanning_on_upload_enabled          = true
  malware_scanning_on_upload_cap_gb_per_month = 5000
  sensitive_data_discovery_enabled            = true
}

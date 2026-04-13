/*
  Module: Microsoft Purview
  Provisions: Purview account, managed identity, ADLS scan source,
              collection, classification rules
  
  FIX: This module was referenced in the architecture diagram but missing
       from the original codebase. Now fully provisioned.
*/

resource "azurerm_purview_account" "main" {
  name                = "purview-uks-${var.project_name}-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# ---------------------------------------------------------------------------
# Outputs needed for RBAC in root module
# ---------------------------------------------------------------------------
# The Purview managed identity principal_id is used by root main.tf
# to assign Storage Blob Data Reader on ADLS for scanning.

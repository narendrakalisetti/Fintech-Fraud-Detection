output "purview_account_id"    { value = azurerm_purview_account.main.id }
output "purview_principal_id"  { value = azurerm_purview_account.main.identity[0].principal_id }
output "catalog_endpoint"      { value = azurerm_purview_account.main.catalog_endpoint }
output "scan_endpoint"         { value = azurerm_purview_account.main.scan_endpoint }

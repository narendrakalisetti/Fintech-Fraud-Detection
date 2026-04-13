output "key_vault_id"  { value = azurerm_key_vault.main.id }
output "key_vault_uri" { value = azurerm_key_vault.main.vault_uri }
output "cmk_key_id"    { value = azurerm_key_vault_key.cmk.id }

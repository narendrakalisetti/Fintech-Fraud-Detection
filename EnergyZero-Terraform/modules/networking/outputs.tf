output "vnet_id"                        { value = azurerm_virtual_network.main.id }
output "private_endpoint_subnet_id"     { value = azurerm_subnet.private_endpoint.id }
output "databricks_public_subnet_id"    { value = azurerm_subnet.databricks_public.id }
output "databricks_private_subnet_id"   { value = azurerm_subnet.databricks_private.id }
output "databricks_public_subnet_name"  { value = azurerm_subnet.databricks_public.name }
output "databricks_private_subnet_name" { value = azurerm_subnet.databricks_private.name }
output "public_nsg_association_id"      { value = azurerm_subnet_network_security_group_association.databricks_public.id }
output "private_nsg_association_id"     { value = azurerm_subnet_network_security_group_association.databricks_private.id }

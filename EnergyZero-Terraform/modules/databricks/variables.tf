variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "project_name" { type = string }
variable "environment" { type = string }
variable "databricks_subnet_id" { type = string }
variable "databricks_private_subnet_id" { type = string }
variable "key_vault_id" { type = string }
variable "adls_storage_account_id" { type = string }
variable "log_analytics_workspace_id" { type = string }
variable "databricks_sku" { type = string; default = "premium" }
variable "vnet_id" { type = string; default = "" }
variable "databricks_public_subnet_name" { type = string; default = "snet-databricks-public" }
variable "databricks_private_subnet_name" { type = string; default = "snet-databricks-private" }
variable "public_nsg_association_id" { type = string; default = "" }
variable "private_nsg_association_id" { type = string; default = "" }
variable "tags" { type = map(string); default = {} }

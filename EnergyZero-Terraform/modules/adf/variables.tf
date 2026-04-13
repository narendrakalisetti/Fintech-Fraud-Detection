variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "project_name" { type = string }
variable "environment" { type = string }
variable "key_vault_id" { type = string }
variable "adls_storage_account_name" { type = string }
variable "adls_storage_account_id" { type = string }
variable "log_analytics_workspace_id" { type = string }
variable "tags" { type = map(string); default = {} }

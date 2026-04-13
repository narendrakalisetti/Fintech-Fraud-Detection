variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "project_name" { type = string }
variable "environment" { type = string }
variable "tenant_id" { type = string }
variable "object_id" { type = string }
variable "allowed_ip_ranges" { type = list(string); default = [] }
variable "private_endpoint_subnet_id" { type = string }
variable "log_analytics_workspace_id" { type = string }
variable "tags" { type = map(string); default = {} }

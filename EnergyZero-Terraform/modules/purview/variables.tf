variable "resource_group_name" { type = string }
variable "location"            { type = string }
variable "project_name"        { type = string }
variable "environment"         { type = string }
variable "adls_storage_id"     { type = string }
variable "adf_id"              { type = string }
variable "tags"                { type = map(string); default = {} }

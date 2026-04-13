variable "project_name" {
  description = "Project name used in resource naming"
  type        = string
  default     = "energyzero"

  validation {
    condition     = length(var.project_name) <= 12 && can(regex("^[a-z0-9]+$", var.project_name))
    error_message = "project_name must be lowercase alphanumeric, max 12 chars."
  }
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "prod"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "location" {
  description = "Azure region — UK South for Net Zero 2050 alignment"
  type        = string
  default     = "uksouth"

  validation {
    condition     = contains(["uksouth", "ukwest"], var.location)
    error_message = "location must be uksouth or ukwest for UK data residency."
  }
}

variable "allowed_ip_ranges" {
  description = "IP ranges permitted to access ADLS and Key Vault (CIDR notation)"
  type        = list(string)
  default     = []
}

variable "databricks_sku" {
  description = "Databricks workspace SKU"
  type        = string
  default     = "premium"

  validation {
    condition     = contains(["standard", "premium", "trial"], var.databricks_sku)
    error_message = "databricks_sku must be standard, premium, or trial."
  }
}

variable "adls_replication_type" {
  description = "ADLS Gen2 replication type (GRS recommended for production)"
  type        = string
  default     = "GRS"
}

variable "log_retention_days" {
  description = "Log Analytics retention in days (90 minimum for UK GDPR Art. 30)"
  type        = number
  default     = 90

  validation {
    condition     = var.log_retention_days >= 90
    error_message = "log_retention_days must be >= 90 for UK GDPR Art. 30 compliance."
  }
}

variable "alert_email" {
  description = "Email address for monitoring alerts"
  type        = string
  default     = "dataengineering@energyzero.co.uk"
}

variable "tags" {
  description = "Additional tags to merge with common tags"
  type        = map(string)
  default     = {}
}

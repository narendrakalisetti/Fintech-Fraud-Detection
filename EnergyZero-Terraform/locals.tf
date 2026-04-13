locals {
  common_tags = merge(
    {
      Project        = "EnergyZero"
      Environment    = var.environment
      ManagedBy      = "Terraform"
      Owner          = "Data Engineering"
      CostCentre     = "DE-001"
      DataController = "EnergyZero Ltd"
      IcoRegistration = "ZB123456"
      Compliance     = "UK-GDPR|ISO27001|NetZero2050"
      Region         = var.location
      CreatedBy      = "Narendra Kalisetti"
    },
    var.tags
  )

  # Naming convention: <type>-<region>-<project>-<env>
  name_prefix = "${var.project_name}-${var.environment}"

  # Storage account name — no hyphens, max 24 chars
  storage_account_name = "sauks${var.project_name}${var.environment}"

  # Terraform state storage
  tfstate_storage_name = "sauks${var.project_name}tfstate"
}

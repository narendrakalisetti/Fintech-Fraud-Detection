project_name          = "energyzero"
environment           = "dev"
location              = "uksouth"
adls_replication_type = "LRS"       # LRS in dev — cheaper, no need for GRS
databricks_sku        = "standard"  # Standard tier in dev — no premium features needed
log_retention_days    = 30          # Shorter retention in dev
alert_email           = "narendra.dev@energyzero.co.uk"
allowed_ip_ranges     = ["your.office.ip/32"]  # Replace with actual dev IP

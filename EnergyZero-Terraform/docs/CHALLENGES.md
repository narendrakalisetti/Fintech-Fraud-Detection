# EnergyZero – Engineering Challenges & Solutions

## 1. ADF Managed Identity RBAC Propagation Delay

**Problem:** After assigning `Storage Blob Data Contributor` to the ADF managed identity via Terraform, the first pipeline run failed with HTTP 403. Azure RBAC changes can take up to 10 minutes to propagate globally across regions.

**Solution:** Added a 10-minute `sleep` step in `scripts/deploy_adf_pipelines.sh` after Terraform apply, before triggering any pipelines. Also added the RBAC assignment to the `depends_on` chain so Terraform waits for it before moving on.

**Lesson:** Never trigger pipelines in the same CI step as RBAC assignments. Always insert a propagation buffer.

---

## 2. Key Vault Soft-Delete Conflict on Redeploy

**Problem:** After `terraform destroy` in dev, redeploying failed because the Key Vault name was soft-deleted but not purged. Azure Key Vault names are globally unique and soft-deleted vaults hold the name for 90 days.

**Solution:** Added `recover_soft_deleted_key_vaults = true` in the AzureRM provider `features` block. Documented manual purge command for when recovery is not wanted:

```bash
az keyvault purge --name kv-uks-energyzero-dev --location uksouth
```

**Lesson:** Key Vault name collisions on redeploy are one of the most common Terraform/Azure gotchas. Always use `recover_soft_deleted_key_vaults = true` in non-production or add the purge to your destroy script.

---

## 3. ADLS Gen2 HNS and Shared Access Key Conflict

**Problem:** Setting `shared_access_key_enabled = false` (security best practice) broke the initial ADF linked service which was using a connection string (SAS key-based auth). The ADF pipeline started failing with `AuthorizationPermissionMismatch`.

**Solution:** Migrated the ADF linked service from connection string authentication to managed identity authentication (`use_managed_identity = true`). This is also the more secure pattern — no credentials stored anywhere.

**Lesson:** When hardening storage security (`shared_access_key_enabled = false`), all dependent services must be migrated to managed identity or service principal auth first. Sequence matters.

---

## 4. Terraform State Lock Contention in CI

**Problem:** Two concurrent GitHub Actions runs (e.g. a PR push and a direct commit) triggered simultaneous `terraform plan` executions, causing an Azure Blob Storage state lock conflict. The second run failed with `Error acquiring the state lock`.

**Solution:** Added `concurrency` groups to the GitHub Actions workflow:

```yaml
concurrency:
  group: terraform-${{ github.ref }}
  cancel-in-progress: false
```

`cancel-in-progress: false` ensures in-flight applies are not interrupted mid-run, which could corrupt state.

**Lesson:** Always use GitHub Actions concurrency groups for any workflow that writes to shared state.

---

## 5. Purview Managed Identity Requires Explicit RBAC Propagation

**Problem:** Purview scan of ADLS failed with `403 Forbidden` despite the `Storage Blob Data Reader` RBAC assignment being present in Terraform state.

**Solution:** Added an explicit `depends_on` in the Purview RBAC assignment referencing the ADLS module output, plus a 5-minute wait in the smoke test before triggering a Purview scan.

---

## 6. Delta OPTIMIZE and VACUUM Permissions

**Problem:** Running `OPTIMIZE` and `VACUUM` on Delta tables in ADLS required the Databricks cluster service principal to have `Storage Blob Data Owner` (not just `Contributor`) because VACUUM deletes files.

**Solution:** Upgraded the Databricks RBAC role from `Storage Blob Data Contributor` to `Storage Blob Data Owner` specifically for the gold layer container. Bronze and silver remain `Contributor`.

---

## 7. AzureRM Provider v4.x Breaking Changes

**Problem:** Upgrading from AzureRM `~> 3.110` to `~> 4.0` introduced breaking changes — specifically, `azurerm_storage_container` now requires `storage_account_id` instead of `storage_account_name`, and several `azurerm_monitor_diagnostic_setting` arguments changed.

**Solution:** Ran `terraform plan` against the 4.x provider in a dev environment first, reviewed all breaking changes in the [AzureRM v4 upgrade guide](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/guides/4.0-upgrade-guide), and updated all affected resources before applying to prod.

**Lesson:** Always test major provider version upgrades in dev. The AzureRM 3.x → 4.x migration is non-trivial.

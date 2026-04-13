# Contributing to EnergyZero-Terraform

## Getting Started

1. Fork the repo and clone locally
2. Install pre-commit hooks: `pre-commit install`
3. Copy dev tfvars: `cp environments/dev/terraform.tfvars terraform.tfvars`
4. Run: `terraform init -backend=false && terraform validate`

## Pull Request Checklist

- [ ] `terraform fmt -recursive` passes
- [ ] `terraform validate` passes
- [ ] `tflint` passes with no errors
- [ ] `checkov` security scan reviewed
- [ ] PySpark tests pass: `pytest tests/ -v`
- [ ] CHANGELOG.md updated

## Branching Strategy

- `main` — production-ready, protected
- `feature/*` — new features
- `fix/*` — bug fixes

All PRs require at least one reviewer approval before merge.

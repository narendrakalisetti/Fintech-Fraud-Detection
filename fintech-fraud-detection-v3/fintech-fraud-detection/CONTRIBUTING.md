# Contributing to ClearPay Fraud Detection

## Branching Strategy
- `main` — production. CI runs full test suite + security scan on every PR.
- `develop` — integration branch.
- `feature/*` — feature branches, always from `develop`.

## Before Committing
```bash
pip install pre-commit
pre-commit install
```
Hooks run black, flake8, and bandit automatically.

## Pull Request Checklist
- [ ] All tests pass: `pytest tests/ -v --cov=src --cov-fail-under=85`
- [ ] Black formatting: `black --check src/ tests/`
- [ ] Flake8 linting: `flake8 src/ tests/ --max-line-length=120`
- [ ] Bandit security: `bandit -r src/ -ll`
- [ ] No raw PII (IBAN, account numbers) in code, tests, or sample data
- [ ] GDPR impact assessed for any new data fields
- [ ] Risk rule changes reviewed by Compliance team

## GDPR Code Review Rule
Any PR that touches `hash_pii_columns`, `filter_high_value_transactions`, or the risk
scoring engine requires sign-off from the Data Protection Officer (DPO) before merge.

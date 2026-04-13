# Contributing to Fintech-Fraud-Detection

## Getting Started

1. Fork the repo and clone locally
2. Install dependencies: `pip install -r requirements-dev.txt`
3. Install pre-commit hooks: `pre-commit install`
4. Generate test data: `python scripts/generate_sample_data.py --rows 10000 --seed 42`
5. Run tests: `pytest tests/ -v --cov=src`

## Pull Request Checklist

- [ ] `black` formatting passes
- [ ] `ruff` linting passes
- [ ] `bandit` security scan reviewed
- [ ] All tests pass: `pytest tests/ -v`
- [ ] Coverage >= 85%: `pytest --cov=src --cov-fail-under=85`
- [ ] CHANGELOG.md updated

## Writing Tests

All new src functions must have unit tests in `tests/`. Use the session-scoped `spark` fixture from `conftest.py`. Tests should run in under 30 seconds (use `local[2]` master).

## Branching

- `main` — production, protected
- `feature/*` — new features
- `fix/*` — bug fixes
- `compliance/*` — regulatory changes (MLR, GDPR)

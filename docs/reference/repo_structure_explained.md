# Repo Structure Explained

This project uses a layered repository because each layer solves a different problem.

- `docs/` explains the project
- `config/` stores settings
- `sample_data/` organizes raw domains
- `sql/` stores reusable SQL
- `src/` stores reusable Python logic
- `notebooks/` stores Databricks workflow steps
- `tests/` prepares for validation
- `.github/workflows/` stores repository automation checks

A strong engineering repository should make those responsibilities visible.

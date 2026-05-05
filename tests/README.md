# Test Suite Documentation

This directory contains automated tests for the Vattenfall Capstone Project to ensure project structure, configuration, and data quality.

## Test Files Overview

### `test_repo_structure.py`
Validates the repository structure and organization:
* Verifies required directories exist (config, docs, notebooks, sample_data, sql, src, tests)
* Checks that source code follows proper Python package structure
* Ensures key files like README.md and SQL scripts are present
* Validates naming conventions (snake_case for Python files, .yml for configs)

### `test_config_presence.py`
Validates configuration files:
* Confirms all config files exist (project_config.yml, paths_config.yml, tables_config.yml)
* Verifies config files are valid YAML
* Checks that config files contain expected keys and structure
* Validates that config_loader utility exists

### `test_sample_data.py`
Validates sample data files:
* Verifies all sample data directories exist (energy_prices, weather, grid_telemetry, reference)
* Confirms each directory contains sample files
* Performs basic structure validation on CSV files
* Ensures files are not empty

### `conftest.py`
Shared pytest fixtures:
* `project_root`: Returns project root directory
* `config_dir`: Returns config directory path
* `sample_data_dir`: Returns sample_data directory path
* `load_config`: Factory fixture to load YAML files
* `project_config`: Pre-loaded project configuration
* `paths_config`: Pre-loaded paths configuration
* `tables_config`: Pre-loaded tables configuration

## Running Tests

### Prerequisites
Install pytest and PyYAML:
```bash
pip install pytest PyYAML
```

### Run All Tests
From the project root directory:
```bash
pytest tests/
```

### Run Specific Test File
```bash
pytest tests/test_repo_structure.py
```

### Run with Verbose Output
```bash
pytest tests/ -v
```

### Run and Show Print Statements
```bash
pytest tests/ -s
```

### Run Tests with Coverage
```bash
pip install pytest-cov
pytest tests/ --cov=src --cov-report=html
```

## Test Organization

Tests follow pytest conventions:
* Test files are named `test_*.py`
* Test classes are named `Test*`
* Test functions are named `test_*`
* Fixtures are defined in `conftest.py` for reusability

## Adding New Tests

When adding new tests:
1. Create a new file following the `test_*.py` naming convention
2. Import pytest and required modules
3. Use fixtures from `conftest.py` for common setup
4. Write descriptive test names that explain what is being tested
5. Add docstrings to test functions
6. Update this README with information about new test files

## Test Categories

**Structure Tests**: Validate project organization
**Configuration Tests**: Ensure configs are valid and complete
**Data Tests**: Verify sample data presence and quality

## Continuous Integration

These tests can be integrated into CI/CD pipelines:
* Run on every commit
* Run before deployments
* Run as part of pull request validation

## Troubleshooting

If tests fail:
1. Read the error message carefully
2. Check if required files/directories exist
3. Verify configuration files are valid YAML
4. Ensure Python path is set correctly
5. Check that all dependencies are installed

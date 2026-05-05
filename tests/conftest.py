"""
Shared pytest fixtures and configuration for the test suite.

This file provides reusable fixtures for all test modules.
"""

import pytest
import yaml
from pathlib import Path


@pytest.fixture(scope="session")
def project_root():
    """Return the project root directory."""
    # Handle both pytest execution and Databricks notebook execution
    try:
        return Path(__file__).parent.parent
    except NameError:
        # Fallback for Databricks/notebook environment where __file__ doesn't exist
        return Path("/Workspace/Users/gharbi@startsteps.org/vattenfall-capstone-project")


@pytest.fixture(scope="session")
def config_dir(project_root):
    """Return the config directory path."""
    return project_root / "config"


@pytest.fixture(scope="session")
def sample_data_dir(project_root):
    """Return the sample_data directory path."""
    return project_root / "sample_data"


@pytest.fixture(scope="session")
def load_config():
    """Factory fixture to load any YAML config file."""
    def _load_config(config_path):
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    return _load_config


@pytest.fixture(scope="session")
def project_config(config_dir, load_config):
    """Load and return project_config.yml."""
    return load_config(config_dir / "project_config.yml")


@pytest.fixture(scope="session")
def paths_config(config_dir, load_config):
    """Load and return paths_config.yml."""
    return load_config(config_dir / "paths_config.yml")


@pytest.fixture(scope="session")
def tables_config(config_dir, load_config):
    """Load and return tables_config.yml."""
    return load_config(config_dir / "tables_config.yml")

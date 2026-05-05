"""
Tests for validating configuration files presence and structure.

This test suite ensures that all configuration files exist and contain
the expected keys and structure.
"""

import os
import pytest
import yaml
from pathlib import Path


# Get the project root directory (parent of tests folder)
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"


class TestConfigFilesPresence:
    """Test suite for verifying configuration files exist."""
    
    def test_config_directory_exists(self):
        """Verify config directory exists."""
        assert CONFIG_DIR.exists(), "config directory should exist"
        assert CONFIG_DIR.is_dir(), "config should be a directory"
    
    def test_project_config_exists(self):
        """Verify project_config.yml exists."""
        config_path = CONFIG_DIR / "project_config.yml"
        assert config_path.exists(), "project_config.yml should exist"
    
    def test_paths_config_exists(self):
        """Verify paths_config.yml exists."""
        config_path = CONFIG_DIR / "paths_config.yml"
        assert config_path.exists(), "paths_config.yml should exist"
    
    def test_tables_config_exists(self):
        """Verify tables_config.yml exists."""
        config_path = CONFIG_DIR / "tables_config.yml"
        assert config_path.exists(), "tables_config.yml should exist"


class TestConfigFileValidity:
    """Test suite for verifying configuration files are valid YAML."""
    
    def test_project_config_is_valid_yaml(self):
        """Verify project_config.yml is valid YAML."""
        config_path = CONFIG_DIR / "project_config.yml"
        
        with open(config_path, 'r') as f:
            try:
                config = yaml.safe_load(f)
                assert config is not None, "project_config.yml should not be empty"
            except yaml.YAMLError as e:
                pytest.fail(f"project_config.yml is not valid YAML: {e}")
    
    def test_paths_config_is_valid_yaml(self):
        """Verify paths_config.yml is valid YAML."""
        config_path = CONFIG_DIR / "paths_config.yml"
        
        with open(config_path, 'r') as f:
            try:
                config = yaml.safe_load(f)
                assert config is not None, "paths_config.yml should not be empty"
            except yaml.YAMLError as e:
                pytest.fail(f"paths_config.yml is not valid YAML: {e}")
    
    def test_tables_config_is_valid_yaml(self):
        """Verify tables_config.yml is valid YAML."""
        config_path = CONFIG_DIR / "tables_config.yml"
        
        with open(config_path, 'r') as f:
            try:
                config = yaml.safe_load(f)
                assert config is not None, "tables_config.yml should not be empty"
            except yaml.YAMLError as e:
                pytest.fail(f"tables_config.yml is not valid YAML: {e}")


class TestProjectConfigStructure:
    """Test suite for validating project_config.yml structure."""
    
    @pytest.fixture
    def project_config(self):
        """Load project configuration."""
        config_path = CONFIG_DIR / "project_config.yml"
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def test_has_project_name(self, project_config):
        """Verify project_config contains project_name."""
        assert "project_name" in project_config, "project_config should have 'project_name' key"
        assert isinstance(project_config["project_name"], str), "project_name should be a string"
    
    def test_has_catalog_info(self, project_config):
        """Verify project_config contains catalog information."""
        expected_keys = ["catalog", "schemas", "volumes"]
        
        for key in expected_keys:
            assert key in project_config, f"project_config should have '{key}' key"


class TestPathsConfigStructure:
    """Test suite for validating paths_config.yml structure."""
    
    @pytest.fixture
    def paths_config(self):
        """Load paths configuration."""
        config_path = CONFIG_DIR / "paths_config.yml"
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def test_has_data_paths(self, paths_config):
        """Verify paths_config contains data source paths."""
        expected_categories = ["market_prices", "weather", "grid_events", "reference"]
        
        for category in expected_categories:
            assert category in paths_config["landing_paths"], f"paths_config should have '{category}' section"


class TestTablesConfigStructure:
    """Test suite for validating tables_config.yml structure."""
    
    @pytest.fixture
    def tables_config(self):
        """Load tables configuration."""
        config_path = CONFIG_DIR / "tables_config.yml"
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def test_has_table_definitions(self, tables_config):
        """Verify tables_config contains table definitions."""
        assert "tables" in tables_config or "bronze" in tables_config or len(tables_config) > 0, \
            "tables_config should define tables"
    
    def test_table_definitions_are_dict(self, tables_config):
        """Verify table definitions follow expected structure."""
        assert isinstance(tables_config, dict), "tables_config should be a dictionary"


class TestConfigLoaderUtil:
    """Test suite for config_loader utility functions."""
    
    def test_config_loader_module_exists(self):
        """Verify config_loader.py exists in src/utils."""
        config_loader_path = PROJECT_ROOT / "src" / "utils" / "config_loader.py"
        assert config_loader_path.exists(), "src/utils/config_loader.py should exist"
    
    def test_config_loader_not_empty(self):
        """Verify config_loader.py is not empty."""
        config_loader_path = PROJECT_ROOT / "src" / "utils" / "config_loader.py"
        assert config_loader_path.stat().st_size > 0, "config_loader.py should have content"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

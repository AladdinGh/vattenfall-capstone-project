"""
Tests for validating the Vattenfall Capstone Project repository structure.

This test suite ensures that all required directories and key files are present
in the project structure.
"""

import os
import pytest
from pathlib import Path


# Get the project root directory (parent of tests folder)
# Handle both pytest execution and Databricks notebook execution
try:
    PROJECT_ROOT = Path(__file__).parent.parent
except NameError:
    # Fallback for Databricks/notebook environment where __file__ doesn't exist
    PROJECT_ROOT = Path("/Workspace/Users/gharbi@startsteps.org/vattenfall-capstone-project")


class TestRepositoryStructure:
    """Test suite for verifying repository structure and organization."""
    
    def test_root_readme_exists(self):
        """Verify that README.md exists at project root."""
        readme_path = PROJECT_ROOT / "README.md"
        assert readme_path.exists(), "README.md should exist at project root"
        assert readme_path.stat().st_size > 0, "README.md should not be empty"
    
    def test_required_directories_exist(self):
        """Verify that all required directories are present."""
        required_dirs = [
            "config",
            "docs",
            "notebooks",
            "sample_data",
            "sql",
            "src",
            "tests",
        ]
        
        for dir_name in required_dirs:
            dir_path = PROJECT_ROOT / dir_name
            assert dir_path.exists(), f"Directory '{dir_name}' should exist"
            assert dir_path.is_dir(), f"'{dir_name}' should be a directory"
    
    def test_config_directory_structure(self):
        """Verify config directory contains expected files."""
        config_files = [
            "project_config.yml",
            "paths_config.yml",
            "tables_config.yml",
        ]
        
        for config_file in config_files:
            config_path = PROJECT_ROOT / "config" / config_file
            assert config_path.exists(), f"Config file '{config_file}' should exist"
    
    def test_src_directory_structure(self):
        """Verify src directory has proper Python package structure."""
        src_subdirs = ["utils", "transforms", "udfs", "validation"]
        
        for subdir in src_subdirs:
            subdir_path = PROJECT_ROOT / "src" / subdir
            assert subdir_path.exists(), f"Source directory 'src/{subdir}' should exist"
            
            init_file = subdir_path / "__init__.py"
            assert init_file.exists(), f"'src/{subdir}/__init__.py' should exist for package"
    
    def test_sample_data_directories_exist(self):
        """Verify sample_data has subdirectories for each data source."""
        data_sources = [
            "energy_prices",
            "weather",
            "grid_telemetry",
            "reference",
        ]
        
        for source in data_sources:
            source_path = PROJECT_ROOT / "sample_data" / source
            assert source_path.exists(), f"Sample data directory '{source}' should exist"
    
    def test_notebooks_directory_structure(self):
        """Verify notebooks directory has layer-specific subdirectories."""
        # Check if notebooks directory exists first
        notebooks_dir = PROJECT_ROOT / "notebooks"
        assert notebooks_dir.exists(), "notebooks directory should exist"
        
        # Look for common patterns: bronze/silver/gold OR 02_bronze/03_silver/04_gold
        subdirs = [d.name for d in notebooks_dir.iterdir() if d.is_dir()]
        
        # At least one layer directory should exist
        expected_patterns = ["bronze", "silver", "gold", "02_bronze", "03_silver", "04_gold"]
        has_layer = any(layer in subdirs for layer in expected_patterns)
        
        assert has_layer, f"notebooks should contain at least one layer directory. Found: {subdirs}"
    
    def test_sql_scripts_exist(self):
        """Verify key SQL scripts are present."""
        sql_files = [
            "01_uc_setup.sql",
            "02_day1_bronze_validation.sql",
        ]
        
        for sql_file in sql_files:
            sql_path = PROJECT_ROOT / "sql" / sql_file
            assert sql_path.exists(), f"SQL file '{sql_file}' should exist"
    
    def test_docs_directory_has_content(self):
        """Verify docs directory contains documentation files."""
        docs_path = PROJECT_ROOT / "docs"
        doc_files = list(docs_path.glob("*.md"))
        
        assert len(doc_files) > 0, "docs directory should contain markdown files"


class TestFileNamingConventions:
    """Test suite for verifying consistent naming conventions."""
    
    def test_python_files_use_snake_case(self):
        """Verify Python files follow snake_case naming convention."""
        src_path = PROJECT_ROOT / "src"
        
        for py_file in src_path.rglob("*.py"):
            filename = py_file.stem
            # Skip __init__ files
            if filename == "__init__":
                continue
            
            # Check for snake_case (lowercase with underscores)
            assert filename.islower(), f"Python file '{py_file.name}' should be lowercase"
            assert " " not in filename, f"Python file '{py_file.name}' should not contain spaces"
    
    def test_config_files_use_yaml_extension(self):
        """Verify config files use .yml extension."""
        config_path = PROJECT_ROOT / "config"
        config_files = list(config_path.glob("*"))
        
        for config_file in config_files:
            if config_file.is_file():
                assert config_file.suffix in [".yml", ".yaml"], \
                    f"Config file '{config_file.name}' should use .yml or .yaml extension"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

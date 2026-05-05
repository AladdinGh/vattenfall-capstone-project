"""
Tests for validating sample data files.

This test suite ensures that sample data files exist and have expected structure.
"""

import pytest
import csv
import json
from pathlib import Path


@pytest.fixture
def sample_data_dir(project_root):
    """Return the sample_data directory path."""
    return project_root / "sample_data"


class TestSampleDataDirectories:
    """Test suite for verifying sample data directory structure."""
    
    def test_energy_prices_directory_exists(self, sample_data_dir):
        """Verify energy_prices directory exists."""
        energy_dir = sample_data_dir / "energy_prices"
        assert energy_dir.exists(), "energy_prices directory should exist"
        assert energy_dir.is_dir(), "energy_prices should be a directory"
    
    def test_weather_directory_exists(self, sample_data_dir):
        """Verify weather directory exists."""
        weather_dir = sample_data_dir / "weather"
        assert weather_dir.exists(), "weather directory should exist"
        assert weather_dir.is_dir(), "weather should be a directory"
    
    def test_grid_telemetry_directory_exists(self, sample_data_dir):
        """Verify grid_telemetry directory exists."""
        telemetry_dir = sample_data_dir / "grid_telemetry"
        assert telemetry_dir.exists(), "grid_telemetry directory should exist"
        assert telemetry_dir.is_dir(), "grid_telemetry should be a directory"
    
    def test_reference_directory_exists(self, sample_data_dir):
        """Verify reference directory exists."""
        reference_dir = sample_data_dir / "reference"
        assert reference_dir.exists(), "reference directory should exist"
        assert reference_dir.is_dir(), "reference should be a directory"


class TestEnergyPricesSampleData:
    """Test suite for energy prices sample data."""
    
    def test_has_sample_files(self, sample_data_dir):
        """Verify energy_prices has sample data files."""
        energy_dir = sample_data_dir / "energy_prices"
        
        # Check for CSV or JSON files
        csv_files = list(energy_dir.glob("*.csv"))
        json_files = list(energy_dir.glob("*.json"))
        
        assert len(csv_files) + len(json_files) > 0, \
            "energy_prices should contain at least one sample data file"


class TestWeatherSampleData:
    """Test suite for weather sample data."""
    
    def test_has_sample_files(self, sample_data_dir):
        """Verify weather has sample data files."""
        weather_dir = sample_data_dir / "weather"
        
        # Check for CSV or JSON files
        csv_files = list(weather_dir.glob("*.csv"))
        json_files = list(weather_dir.glob("*.json"))
        
        assert len(csv_files) + len(json_files) > 0, \
            "weather should contain at least one sample data file"
    
    def test_weather_csv_structure(self, sample_data_dir):
        """Verify weather CSV files have proper structure."""
        weather_dir = sample_data_dir / "weather"
        csv_files = list(weather_dir.glob("*.csv"))
        
        if len(csv_files) > 0:
            # Test first CSV file
            with open(csv_files[0], 'r') as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames
                
                # Verify it has headers
                assert headers is not None, f"{csv_files[0].name} should have column headers"
                
                # Try to read at least one row
                rows = list(reader)
                assert len(rows) > 0, f"{csv_files[0].name} should have at least one data row"


class TestGridTelemetrySampleData:
    """Test suite for grid telemetry sample data."""
    
    def test_has_sample_files(self, sample_data_dir):
        """Verify grid_telemetry has sample data files."""
        telemetry_dir = sample_data_dir / "grid_telemetry"
        
        # Check for various file types
        csv_files = list(telemetry_dir.glob("*.csv"))
        json_files = list(telemetry_dir.glob("*.json"))
        parquet_files = list(telemetry_dir.glob("*.parquet"))
        
        assert len(csv_files) + len(json_files) + len(parquet_files) > 0, \
            "grid_telemetry should contain at least one sample data file"


class TestReferenceSampleData:
    """Test suite for reference data."""
    
    def test_has_sample_files(self, sample_data_dir):
        """Verify reference has sample data files."""
        reference_dir = sample_data_dir / "reference"
        
        # Check for CSV or JSON files
        csv_files = list(reference_dir.glob("*.csv"))
        json_files = list(reference_dir.glob("*.json"))
        
        assert len(csv_files) + len(json_files) > 0, \
            "reference should contain at least one sample data file"


class TestSampleDataQuality:
    """Test suite for basic data quality checks."""
    
    def test_csv_files_not_empty(self, sample_data_dir):
        """Verify CSV files are not empty."""
        csv_files = list(sample_data_dir.rglob("*.csv"))
        
        for csv_file in csv_files:
            assert csv_file.stat().st_size > 0, f"{csv_file.name} should not be empty"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

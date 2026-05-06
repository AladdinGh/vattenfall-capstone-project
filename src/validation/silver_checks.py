# Silver Layer Data Quality Validation

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Tuple


class SilverDataValidator:
    """Data quality validator for silver layer tables."""
    
    def __init__(self, df: DataFrame, table_name: str):
        self.df = df
        self.table_name = table_name
        self.results = {}
    
    def check_completeness(self, required_columns: List[str]) -> Dict:
        """
        Check completeness of required columns.
        
        Returns:
            Dictionary with completeness metrics
        """
        completeness = {}
        total_rows = self.df.count()
        
        for col in required_columns:
            if col in self.df.columns:
                null_count = self.df.filter(F.col(col).isNull()).count()
                completeness[col] = {
                    "null_count": null_count,
                    "completeness_pct": ((total_rows - null_count) / total_rows * 100) if total_rows > 0 else 0
                }
            else:
                completeness[col] = {"error": "Column not found"}
        
        self.results["completeness"] = completeness
        return completeness
    
    def check_duplicates(self, key_columns: List[str]) -> Dict:
        """
        Check for duplicate records based on key columns.
        
        Returns:
            Dictionary with duplicate metrics
        """
        total_rows = self.df.count()
        distinct_rows = self.df.select(key_columns).distinct().count()
        duplicate_count = total_rows - distinct_rows
        
        result = {
            "total_rows": total_rows,
            "distinct_rows": distinct_rows,
            "duplicate_count": duplicate_count,
            "duplicate_pct": (duplicate_count / total_rows * 100) if total_rows > 0 else 0
        }
        
        self.results["duplicates"] = result
        return result
    
    def check_value_ranges(self, range_checks: Dict[str, Tuple[float, float]]) -> Dict:
        """
        Check if numeric values are within expected ranges.
        
        Args:
            range_checks: Dict of {column: (min_value, max_value)}
        
        Returns:
            Dictionary with range violation counts
        """
        range_results = {}
        
        for col, (min_val, max_val) in range_checks.items():
            if col in self.df.columns:
                out_of_range = self.df.filter(
                    (F.col(col) < min_val) | (F.col(col) > max_val)
                ).count()
                
                range_results[col] = {
                    "expected_range": f"[{min_val}, {max_val}]",
                    "violations": out_of_range,
                    "violation_pct": (out_of_range / self.df.count() * 100) if self.df.count() > 0 else 0
                }
            else:
                range_results[col] = {"error": "Column not found"}
        
        self.results["value_ranges"] = range_results
        return range_results
    
    def check_timeliness(self, timestamp_col: str, max_age_hours: int = 24) -> Dict:
        """
        Check data timeliness - how recent is the data.
        
        Args:
            timestamp_col: Name of timestamp column
            max_age_hours: Maximum acceptable data age in hours
        
        Returns:
            Dictionary with timeliness metrics
        """
        if timestamp_col not in self.df.columns:
            return {"error": "Timestamp column not found"}
        
        latest_timestamp = self.df.agg(F.max(timestamp_col)).collect()[0][0]
        oldest_timestamp = self.df.agg(F.min(timestamp_col)).collect()[0][0]
        
        from datetime import datetime, timedelta
        now = datetime.now()
        
        if latest_timestamp:
            # Convert to datetime if needed
            if isinstance(latest_timestamp, str):
                latest_timestamp = datetime.fromisoformat(latest_timestamp)
            
            age_hours = (now - latest_timestamp).total_seconds() / 3600
            is_stale = age_hours > max_age_hours
        else:
            age_hours = None
            is_stale = None
        
        result = {
            "latest_timestamp": str(latest_timestamp),
            "oldest_timestamp": str(oldest_timestamp),
            "data_age_hours": age_hours,
            "is_stale": is_stale,
            "max_age_threshold_hours": max_age_hours
        }
        
        self.results["timeliness"] = result
        return result
    
    def check_referential_integrity(self, foreign_key: str, 
                                   reference_df: DataFrame, 
                                   reference_key: str) -> Dict:
        """
        Check referential integrity between tables.
        
        Args:
            foreign_key: Foreign key column in current table
            reference_df: Reference DataFrame
            reference_key: Primary key column in reference table
        
        Returns:
            Dictionary with referential integrity metrics
        """
        # Get distinct foreign keys
        fk_values = self.df.select(foreign_key).distinct()
        
        # Get distinct reference keys
        ref_values = reference_df.select(reference_key).distinct()
        
        # Find orphaned records
        orphaned = fk_values.join(ref_values, fk_values[foreign_key] == ref_values[reference_key], "left_anti")
        orphaned_count = orphaned.count()
        
        total_distinct_fk = fk_values.count()
        
        result = {
            "foreign_key": foreign_key,
            "reference_key": reference_key,
            "orphaned_keys": orphaned_count,
            "total_distinct_keys": total_distinct_fk,
            "integrity_pct": ((total_distinct_fk - orphaned_count) / total_distinct_fk * 100) if total_distinct_fk > 0 else 100
        }
        
        self.results["referential_integrity"] = result
        return result
    
    def generate_report(self) -> str:
        """
        Generate comprehensive data quality report.
        
        Returns:
            Formatted string report
        """
        report_lines = []
        report_lines.append("=" * 70)
        report_lines.append(f"DATA QUALITY REPORT: {self.table_name}")
        report_lines.append("=" * 70)
        
        for check_type, results in self.results.items():
            report_lines.append(f"\n{check_type.upper()}:")
            report_lines.append("-" * 70)
            
            if isinstance(results, dict):
                for key, value in results.items():
                    report_lines.append(f"  {key}: {value}")
            else:
                report_lines.append(f"  {results}")
        
        report_lines.append("=" * 70)
        
        return "\n".join(report_lines)


def validate_market_prices(df: DataFrame) -> Dict:
    """
    Validate market prices silver table.
    """
    validator = SilverDataValidator(df, "silver_market_prices")
    
    # Required columns check
    required_cols = ["timestamp", "market_zone", "price_eur_mwh"]
    validator.check_completeness(required_cols)
    
    # Duplicate check
    validator.check_duplicates(["timestamp", "market_zone"])
    
    # Value range checks
    range_checks = {
        "price_eur_mwh": (0, 1000),  # Reasonable price range
        "price_24h_avg": (0, 1000),
        "hour": (0, 23)
    }
    validator.check_value_ranges(range_checks)
    
    # Timeliness check
    validator.check_timeliness("timestamp", max_age_hours=48)
    
    return validator.results


def validate_weather_data(df: DataFrame) -> Dict:
    """
    Validate weather silver table.
    """
    validator = SilverDataValidator(df, "silver_weather")
    
    # Use correct column names that match weather schema
    required_cols = ["timestamp", "region_normalized", "temperature_c", "wind_speed_ms"]
    validator.check_completeness(required_cols)
    
    # Check for duplicates using region_original (the city name)
    validator.check_duplicates(["timestamp", "region_original"])
    
    # Value range checks with correct column names
    range_checks = {
        "temperature_c": (-50, 60),
        "wind_speed_ms": (0, 100),
        "cloud_cover_percent": (0, 100),
        "precipitation_mm": (0, 500)
    }
    validator.check_value_ranges(range_checks)
    
    validator.check_timeliness("timestamp", max_age_hours=48)
    
    return validator.results


def validate_grid_events(df: DataFrame) -> Dict:
    """
    Validate grid events silver table.
    """
    validator = SilverDataValidator(df, "silver_grid_events")
    
    required_cols = ["event_id", "timestamp", "event_category", "severity_score"]
    validator.check_completeness(required_cols)
    
    validator.check_duplicates(["event_id"])
    
    range_checks = {
        "severity_score": (0, 100),
        "voltage_kv": (0, 500),
        "frequency_hz": (49, 51)
    }
    validator.check_value_ranges(range_checks)
    
    validator.check_timeliness("timestamp", max_age_hours=72)
    
    return validator.results


def run_all_validations(df_prices: DataFrame, 
                       df_weather: DataFrame, 
                       df_events: DataFrame) -> None:
    """
    Run all validations and print reports.
    """
    print("\n" + "=" * 70)
    print("RUNNING SILVER LAYER VALIDATIONS")
    print("=" * 70 + "\n")
    
    # Validate market prices
    print("\nValidating Market Prices...")
    price_results = validate_market_prices(df_prices)
    validator = SilverDataValidator(df_prices, "silver_market_prices")
    validator.results = price_results
    print(validator.generate_report())
    
    # Validate weather
    print("\nValidating Weather Data...")
    weather_results = validate_weather_data(df_weather)
    validator = SilverDataValidator(df_weather, "silver_weather")
    validator.results = weather_results
    print(validator.generate_report())
    
    # Validate grid events
    print("\nValidating Grid Events...")
    event_results = validate_grid_events(df_events)
    validator = SilverDataValidator(df_events, "silver_grid_events")
    validator.results = event_results
    print(validator.generate_report())
    
    print("\n✅ All validations complete")

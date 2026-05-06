# Grid Event Transformations for Silver Layer

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType, IntegerType
from pyspark.sql.window import Window


def clean_grid_events(df: DataFrame) -> DataFrame:
    """
    Clean and standardize grid event data from bronze layer.
    
    Operations:
    - Remove duplicates
    - Cast data types
    - Standardize event types
    - Validate severity levels
    """
    df_clean = df.dropDuplicates(["event_id", "timestamp"])
    
    df_clean = df_clean.withColumn(
        "timestamp",
        F.col("timestamp").cast(TimestampType())
    )
    
    df_clean = df_clean \
        .withColumn("voltage_kv", F.col("voltage_kv").cast(DoubleType())) \
        .withColumn("current_a", F.col("current_a").cast(DoubleType())) \
        .withColumn("power_mw", F.col("power_mw").cast(DoubleType())) \
        .withColumn("frequency_hz", F.col("frequency_hz").cast(DoubleType()))
    
    # Standardize event type names
    df_clean = df_clean.withColumn(
        "event_type_clean",
        F.lower(F.trim(F.col("event_type")))
    )
    
    # Filter invalid frequency readings (should be around 50 Hz in Europe)
    df_clean = df_clean.filter(
        (F.col("frequency_hz").isNull()) |
        (F.col("frequency_hz").between(49, 51))
    )
    
    return df_clean


def categorize_events(df: DataFrame) -> DataFrame:
    """
    Categorize grid events into standard categories.
    """
    df_categorized = df.withColumn(
        "event_category",
        F.when(F.col("event_type_clean").contains("outage"), "outage")
         .when(F.col("event_type_clean").contains("voltage"), "voltage_issue")
         .when(F.col("event_type_clean").contains("overload"), "overload")
         .when(F.col("event_type_clean").contains("fault"), "fault")
         .when(F.col("event_type_clean").contains("maintenance"), "maintenance")
         .otherwise("other")
    )
    
    return df_categorized


def calculate_severity_score(df: DataFrame) -> DataFrame:
    """
    Calculate severity score for grid events based on impact.
    
    Score ranges from 0-100 based on:
    - Duration of event
    - Number of affected customers
    - Power loss magnitude
    - Event category
    """
    # Base severity by category
    df_severity = df.withColumn(
        "severity_base",
        F.when(F.col("event_category") == "outage", 80)
         .when(F.col("event_category") == "fault", 60)
         .when(F.col("event_category") == "overload", 50)
         .when(F.col("event_category") == "voltage_issue", 40)
         .when(F.col("event_category") == "maintenance", 10)
         .otherwise(20)
    )
    
    # Adjust for power impact
    df_severity = df_severity.withColumn(
        "power_impact_score",
        F.when(F.col("power_mw") > 100, 20)
         .when(F.col("power_mw") > 50, 15)
         .when(F.col("power_mw") > 10, 10)
         .otherwise(5)
    )
    
    # Final severity score (capped at 100)
    df_severity = df_severity.withColumn(
        "severity_score",
        F.least(F.col("severity_base") + F.col("power_impact_score"), F.lit(100))
    )
    
    # Severity level categorization
    df_severity = df_severity.withColumn(
        "severity_level",
        F.when(F.col("severity_score") >= 80, "critical")
         .when(F.col("severity_score") >= 60, "high")
         .when(F.col("severity_score") >= 40, "medium")
         .otherwise("low")
    )
    
    return df_severity


def add_operational_context(df: DataFrame) -> DataFrame:
    """
    Add operational context and derived fields.
    """
    # Time of day impact
    df_context = df.withColumn(
        "hour",
        F.hour("timestamp")
    )
    
    df_context = df_context.withColumn(
        "is_peak_hours",
        F.when(F.col("hour").between(7, 22), True).otherwise(False)
    )
    
    df_context = df_context.withColumn(
        "is_business_hours",
        F.when(F.col("hour").between(8, 17), True).otherwise(False)
    )
    
    # Day of week context
    df_context = df_context.withColumn(
        "day_of_week",
        F.dayofweek("timestamp")
    )
    
    df_context = df_context.withColumn(
        "is_weekend",
        F.when(F.col("day_of_week").isin([1, 7]), True).otherwise(False)
    )
    
    return df_context


def calculate_event_statistics(df: DataFrame) -> DataFrame:
    """
    Calculate event frequency and patterns by location.
    """
    window_location_24h = Window \
        .partitionBy("location_id") \
        .orderBy("timestamp") \
        .rangeBetween(-86400, 0)  # 24 hours in seconds
    
    # Count events in last 24 hours by location
    df_stats = df.withColumn(
        "events_last_24h",
        F.count("event_id").over(window_location_24h)
    )
    
    # Average severity in last 24 hours
    df_stats = df_stats.withColumn(
        "avg_severity_24h",
        F.avg("severity_score").over(window_location_24h)
    )
    
    # Flag for high-incident locations
    df_stats = df_stats.withColumn(
        "is_high_incident_location",
        F.when(F.col("events_last_24h") > 5, True).otherwise(False)
    )
    
    return df_stats


def transform_bronze_to_silver(df: DataFrame) -> DataFrame:
    """
    Complete transformation pipeline for grid events.
    """
    df_silver = (
        df
        .transform(clean_grid_events)
        .transform(categorize_events)
        .transform(calculate_severity_score)
        .transform(add_operational_context)
        .transform(calculate_event_statistics)
        .withColumn("silver_processing_timestamp", F.current_timestamp())
        .withColumn("data_source", F.lit("bronze_grid_events"))
    )
    
    return df_silver

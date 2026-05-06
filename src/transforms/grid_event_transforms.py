# Grid Event Transformations for Silver Layer
# Version 2.0 - Updated for actual bronze schema

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType, IntegerType, StringType
from pyspark.sql.window import Window


def clean_grid_events(df: DataFrame) -> DataFrame:
    """
    Clean and standardize grid event data from bronze layer.
    
    Operations:
    - Remove duplicates by event_id and timestamp
    - Cast duration_minutes and affected_customers to proper types
    - Filter invalid duration (< 0 or > 10080 minutes = 1 week)
    - Filter invalid affected_customers (< 0)
    - Remove null critical fields
    """
    # Remove duplicates
    df_clean = df.dropDuplicates(["event_id", "timestamp"])
    
    # Ensure timestamp is proper type
    df_clean = df_clean.withColumn(
        "timestamp",
        F.col("timestamp").cast(TimestampType())
    )
    
    # Cast numeric fields
    df_clean = df_clean \
        .withColumn("duration_minutes", F.col("duration_minutes").cast(IntegerType())) \
        .withColumn("affected_customers", F.col("affected_customers").cast(IntegerType()))
    
    # Filter invalid records
    df_clean = df_clean.filter(
        (F.col("timestamp").isNotNull()) &
        (F.col("event_id").isNotNull()) &
        (F.col("duration_minutes").isNotNull()) &
        (F.col("duration_minutes") >= 0) &
        (F.col("duration_minutes") <= 10080) &  # Max 1 week
        (F.col("affected_customers").isNotNull()) &
        (F.col("affected_customers") >= 0)
    )
    
    return df_clean


def create_event_day(df: DataFrame) -> DataFrame:
    """
    Create event_day field from timestamp (similar to report_day in other silver tables).
    """
    df_with_day = df.withColumn(
        "event_day",
        F.to_date(F.col("timestamp"))
    )
    
    return df_with_day


def normalize_severity(df: DataFrame) -> DataFrame:
    """
    Normalize severity levels to standard 4-level system.
    
    Maps all variations to: critical, high, medium, low
    Handles mixed case: CRITICAL, Critical, critical, etc.
    """
    df_normalized = df.withColumn(
        "severity_normalized",
        F.when(F.lower(F.trim(F.col("severity"))).isin(["critical", "crit"]), "critical")
         .when(F.lower(F.trim(F.col("severity"))).isin(["high"]), "high")
         .when(F.lower(F.trim(F.col("severity"))).isin(["medium", "med", "moderate"]), "medium")
         .when(F.lower(F.trim(F.col("severity"))).isin(["low", "minor"]), "low")
         .otherwise("medium")  # Default to medium if unknown
    )
    
    return df_normalized


def normalize_event_type(df: DataFrame) -> DataFrame:
    """
    Normalize event types to standard categories.
    
    Maps variations to standard types:
    - equipment_failure
    - planned_maintenance
    - unplanned_outage
    - voltage_fluctuation
    - weather_damage
    - overload
    - other
    """
    df_normalized = df.withColumn(
        "event_type_clean",
        F.lower(F.trim(F.col("event_type")))
    )
    
    df_normalized = df_normalized.withColumn(
        "event_type_normalized",
        F.when(F.col("event_type_clean").isin(["equipment_failure", "failure", "fault"]), "equipment_failure")
         .when(F.col("event_type_clean").isin(["planned_maintenance", "maintenance", "scheduled"]), "planned_maintenance")
         .when(F.col("event_type_clean").isin(["unplanned_outage", "outage", "blackout"]), "unplanned_outage")
         .when(F.col("event_type_clean").isin(["voltage_fluctuation", "fluctuation", "voltage"]), "voltage_fluctuation")
         .when(F.col("event_type_clean").isin(["weather_damage", "storm", "weather"]), "weather_damage")
         .when(F.col("event_type_clean").isin(["overload", "overcurrent"]), "overload")
         .when(F.col("event_type_clean").isin(["alarm"]), "alarm")
         .otherwise("other")
    )
    
    # Map to broader categories
    df_normalized = df_normalized.withColumn(
        "event_category",
        F.when(F.col("event_type_normalized").isin(["unplanned_outage"]), "outage")
         .when(F.col("event_type_normalized").isin(["voltage_fluctuation", "alarm"]), "voltage_issue")
         .when(F.col("event_type_normalized") == "overload", "overload")
         .when(F.col("event_type_normalized") == "equipment_failure", "fault")
         .when(F.col("event_type_normalized") == "planned_maintenance", "maintenance")
         .when(F.col("event_type_normalized") == "weather_damage", "weather")
         .otherwise("other")
    )
    
    return df_normalized


def normalize_region(df: DataFrame) -> DataFrame:
    """
    Normalize region names from cities to country codes.
    """
    df_normalized = df.withColumn(
        "region_normalized",
        F.when(F.col("region").isin(["Copenhagen", "Aarhus"]), "DK")
         .when(F.col("region").isin(["Oslo", "Bergen", "Trondheim", "Stavanger"]), "NO")
         .when(F.col("region").isin(["Stockholm", "Gothenburg", "Malmo", "Uppsala"]), "SE")
         .when(F.col("region").isin(["Helsinki", "Tampere", "Turku", "Oulu"]), "FI")
         .otherwise(F.col("region"))
    )
    
    return df_normalized


def add_temporal_features(df: DataFrame) -> DataFrame:
    """
    Add temporal features for time-based analysis.
    """
    df_temporal = df \
        .withColumn("year", F.year("timestamp")) \
        .withColumn("month", F.month("timestamp")) \
        .withColumn("day", F.dayofmonth("timestamp")) \
        .withColumn("hour", F.hour("timestamp")) \
        .withColumn("day_of_week", F.dayofweek("timestamp")) \
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0))
    
    # Business context
    df_temporal = df_temporal \
        .withColumn("is_business_hours", 
                    F.when(F.col("hour").between(8, 17), 1).otherwise(0)) \
        .withColumn("is_peak_hours",
                    F.when(F.col("hour").between(7, 22), 1).otherwise(0))
    
    # Hour category
    df_temporal = df_temporal.withColumn(
        "hour_category",
        F.when(F.col("hour").between(0, 5), "night")
         .when(F.col("hour").between(6, 11), "morning")
         .when(F.col("hour").between(12, 17), "afternoon")
         .when(F.col("hour").between(18, 21), "evening")
         .otherwise("late_night")
    )
    
    return df_temporal


def calculate_impact_metrics(df: DataFrame) -> DataFrame:
    """
    Calculate impact metrics based on duration and affected customers.
    """
    # Impact score (0-100) based on duration and affected customers
    df_impact = df.withColumn(
        "duration_score",
        F.when(F.col("duration_minutes") > 480, 40)  # > 8 hours
         .when(F.col("duration_minutes") > 240, 30)  # > 4 hours
         .when(F.col("duration_minutes") > 60, 20)   # > 1 hour
         .otherwise(10)
    )
    
    df_impact = df_impact.withColumn(
        "customer_impact_score",
        F.when(F.col("affected_customers") > 5000, 40)
         .when(F.col("affected_customers") > 1000, 30)
         .when(F.col("affected_customers") > 100, 20)
         .otherwise(10)
    )
    
    df_impact = df_impact.withColumn(
        "total_impact_score",
        F.col("duration_score") + F.col("customer_impact_score")
    )
    
    # Customer-hours lost (duration * affected customers)
    df_impact = df_impact.withColumn(
        "customer_hours_lost",
        (F.col("duration_minutes") / 60.0) * F.col("affected_customers")
    )
    
    # Duration category
    df_impact = df_impact.withColumn(
        "duration_category",
        F.when(F.col("duration_minutes") <= 15, "very_short")
         .when(F.col("duration_minutes") <= 60, "short")
         .when(F.col("duration_minutes") <= 240, "medium")
         .when(F.col("duration_minutes") <= 480, "long")
         .otherwise("very_long")
    )
    
    # Affected customers category
    df_impact = df_impact.withColumn(
        "affected_customers_category",
        F.when(F.col("affected_customers") <= 10, "minimal")
         .when(F.col("affected_customers") <= 100, "low")
         .when(F.col("affected_customers") <= 1000, "moderate")
         .when(F.col("affected_customers") <= 5000, "high")
         .otherwise("critical")
    )
    
    return df_impact


def calculate_event_statistics(df: DataFrame) -> DataFrame:
    """
    Calculate rolling event statistics by substation.
    """
    # Window: last 24 hours by substation
    window_24h = Window \
        .partitionBy("substation_id") \
        .orderBy(F.col("timestamp").cast("long")) \
        .rangeBetween(-86400, 0)
    
    # Count events in last 24 hours
    df_stats = df.withColumn(
        "events_last_24h",
        F.count("event_id").over(window_24h) - 1  # Exclude current event
    )
    
    # Average duration in last 24 hours
    df_stats = df_stats.withColumn(
        "avg_duration_24h",
        F.avg("duration_minutes").over(window_24h)
    )
    
    # Total affected customers in last 24 hours
    df_stats = df_stats.withColumn(
        "total_affected_24h",
        F.sum("affected_customers").over(window_24h)
    )
    
    # Flag for high-incident substations
    df_stats = df_stats.withColumn(
        "is_high_incident_substation",
        F.when(F.col("events_last_24h") >= 3, 1).otherwise(0)
    )
    
    return df_stats


def add_severity_band_udf(df: DataFrame) -> DataFrame:
    """
    Apply severity band classifier UDF.
    
    This is the meaningful UDF requirement - classifies events into severity bands
    based on multiple factors: normalized severity, duration, affected customers, and event type.
    
    Bands: critical_priority, high_priority, medium_priority, low_priority, minimal
    """
    from pyspark.sql.functions import udf
    
    def classify_severity_band(severity: str, duration: int, affected_customers: int, 
                              event_type: str) -> str:
        """
        Classify event into severity band based on multiple factors.
        
        Priority factors:
        1. Critical severity + high impact = critical_priority
        2. Long duration or many affected customers = high_priority
        3. Medium impact = medium_priority
        4. Low impact = low_priority
        5. Minimal impact = minimal
        """
        # Handle None values
        severity = severity or "medium"
        duration = duration or 0
        affected_customers = affected_customers or 0
        event_type = event_type or "other"
        
        # Critical priority: critical severity + significant impact
        if severity == "critical" and (duration > 240 or affected_customers > 1000):
            return "critical_priority"
        
        # High priority: high severity or major outages
        if severity == "critical" or \
           (severity == "high" and duration > 120) or \
           affected_customers > 5000 or \
           (event_type == "unplanned_outage" and affected_customers > 1000):
            return "high_priority"
        
        # Medium priority: medium severity with notable impact
        if severity in ["high", "medium"] and duration > 30:
            return "medium_priority"
        
        # Low priority: low severity or minor impact
        if severity == "low" or (duration <= 30 and affected_customers <= 100):
            return "low_priority"
        
        # Minimal: planned maintenance or very low impact
        if event_type == "planned_maintenance" and severity == "low":
            return "minimal"
        
        # Default to medium priority
        return "medium_priority"
    
    # Register and apply UDF
    severity_band_udf = udf(classify_severity_band, StringType())
    
    df_with_band = df.withColumn(
        "severity_band",
        severity_band_udf(
            F.col("severity_normalized"),
            F.col("duration_minutes"),
            F.col("affected_customers"),
            F.col("event_type_normalized")
        )
    )
    
    return df_with_band


def calculate_data_quality_score(df: DataFrame) -> DataFrame:
    """
    Calculate data quality score for each record.
    """
    df_quality = df.withColumn(
        "quality_checks_passed",
        (
            F.when(F.col("event_id").isNotNull(), 1).otherwise(0) +
            F.when(F.col("timestamp").isNotNull(), 1).otherwise(0) +
            F.when(F.col("substation_id").isNotNull(), 1).otherwise(0) +
            F.when(F.col("region").isNotNull(), 1).otherwise(0) +
            F.when(F.col("duration_minutes").isNotNull(), 1).otherwise(0) +
            F.when(F.col("affected_customers").isNotNull(), 1).otherwise(0) +
            F.when(F.col("severity").isNotNull(), 1).otherwise(0) +
            F.when(F.col("event_type").isNotNull(), 1).otherwise(0)
        )
    )
    
    df_quality = df_quality.withColumn(
        "data_quality_score",
        (F.col("quality_checks_passed") / 8.0 * 100).cast(IntegerType())
    )
    
    return df_quality


def enrich_with_metadata(df: DataFrame) -> DataFrame:
    """
    Add metadata fields to track processing.
    """
    df_enriched = df \
        .withColumn("silver_processing_timestamp", F.current_timestamp()) \
        .withColumn("data_source", F.lit("bronze_grid_events")) \
        .withColumn("silver_version", F.lit("2.0"))
    
    return df_enriched


def transform_bronze_to_silver(df: DataFrame) -> DataFrame:
    """
    Complete transformation pipeline for grid events.
    
    Applies all transformations in sequence:
    1. Clean data and validate
    2. Create event_day field
    3. Normalize severity levels
    4. Normalize event types
    5. Normalize region names
    6. Add temporal features
    7. Calculate impact metrics
    8. Calculate event statistics
    9. Apply severity band UDF
    10. Calculate data quality score
    11. Enrich with metadata
    """
    df_silver = (
        df
        .transform(clean_grid_events)
        .transform(create_event_day)
        .transform(normalize_severity)
        .transform(normalize_event_type)
        .transform(normalize_region)
        .transform(add_temporal_features)
        .transform(calculate_impact_metrics)
        .transform(calculate_event_statistics)
        .transform(add_severity_band_udf)
        .transform(calculate_data_quality_score)
        .transform(enrich_with_metadata)
    )
    
    return df_silver

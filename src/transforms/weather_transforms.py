# Weather Transformations for Silver Layer

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType, DateType, StringType
from pyspark.sql.window import Window


def clean_weather_data(df: DataFrame) -> DataFrame:
    """
    Clean and standardize weather data from bronze layer.
    
    Operations:
    - Remove duplicates based on timestamp and region
    - Cast all measurement types to proper numeric types
    - Filter out impossible/invalid weather values
    - Handle duplicate column names (temperature_celsius vs temperature_c)
    """
    # Remove duplicates
    df_clean = df.dropDuplicates(["timestamp", "region"])
    
    # Cast timestamp
    df_clean = df_clean.withColumn(
        "timestamp",
        F.col("timestamp").cast(TimestampType())
    )
    
    # Handle duplicate temperature columns - use temperature_celsius, rename to temperature_c
    if "temperature_celsius" in df_clean.columns:
        df_clean = df_clean.withColumn(
            "temperature_c",
            F.coalesce(
                F.col("temperature_celsius").cast(DoubleType()),
                F.col("temperature_c").cast(DoubleType())
            )
        ).drop("temperature_celsius")
    else:
        df_clean = df_clean.withColumn(
            "temperature_c",
            F.col("temperature_c").cast(DoubleType())
        )
    
    # Cast all measurement types to DoubleType
    df_clean = df_clean \
        .withColumn("wind_speed_ms", F.col("wind_speed_ms").cast(DoubleType())) \
        .withColumn("cloud_cover_percent", F.col("cloud_cover_percent").cast(DoubleType())) \
        .withColumn("precipitation_mm", F.col("precipitation_mm").cast(DoubleType()))
    
    # Handle wind_speed_kmh if it exists (convert to m/s)
    if "wind_speed_kmh" in df_clean.columns:
        df_clean = df_clean.withColumn(
            "wind_speed_ms",
            F.coalesce(
                F.col("wind_speed_ms"),
                F.col("wind_speed_kmh") / 3.6  # Convert km/h to m/s
            )
        ).drop("wind_speed_kmh")
    
    # Filter impossible weather values
    df_clean = df_clean.filter(
        (F.col("timestamp").isNotNull()) &
        (F.col("region").isNotNull()) &
        (F.col("temperature_c").isNotNull()) &
        (F.col("temperature_c").between(-50, 60)) &  # Realistic temperature range
        (F.col("wind_speed_ms").isNotNull()) &
        (F.col("wind_speed_ms").between(0, 100)) &  # 0-360 km/h (0-100 m/s)
        (F.col("cloud_cover_percent").between(0, 100)) &
        (F.col("precipitation_mm") >= 0)  # Can't have negative precipitation
    )
    
    # Flag extreme weather conditions
    df_clean = df_clean.withColumn(
        "is_extreme_temp",
        F.when(
            (F.col("temperature_c") < -20) | (F.col("temperature_c") > 40),
            True
        ).otherwise(False)
    )
    
    df_clean = df_clean.withColumn(
        "is_high_wind",
        F.when(F.col("wind_speed_ms") > 20, True).otherwise(False)  # > 72 km/h
    )
    
    return df_clean


def create_report_day(df: DataFrame) -> DataFrame:
    """
    Create day-level date field for daily aggregations.
    
    The report_day is the date component of the timestamp.
    """
    df_with_day = df.withColumn(
        "report_day",
        F.to_date(F.col("timestamp"))
    )
    
    return df_with_day


def normalize_region(df: DataFrame) -> DataFrame:
    """
    Normalize region/city names to standardized country codes.
    
    Maps:
    - Danish cities (Copenhagen, Aarhus) -> DK
    - Norwegian cities (Oslo, Bergen, Trondheim) -> NO
    - Swedish cities (Stockholm, Gothenburg, Malmö, Uppsala) -> SE
    - Finnish cities (Helsinki, Tampere, Turku) -> FI
    """
    df_normalized = df.withColumn(
        "region_original",
        F.col("region")
    )
    
    df_normalized = df_normalized.withColumn(
        "region_normalized",
        F.when(
            F.lower(F.col("region")).isin(["copenhagen", "aarhus"]),
            "DK"
        ).when(
            F.lower(F.col("region")).isin(["oslo", "bergen", "trondheim"]),
            "NO"
        ).when(
            F.lower(F.col("region")).isin(["stockholm", "gothenburg", "malmö", "malmo", "uppsala"]),
            "SE"
        ).when(
            F.lower(F.col("region")).isin(["helsinki", "tampere", "turku"]),
            "FI"
        ).otherwise("UNKNOWN")
    )
    
    return df_normalized


def normalize_alert_level(df: DataFrame) -> DataFrame:
    """
    Normalize weather alert levels to standardized categories.
    
    Standardized alert levels:
    - none: No alerts
    - low: Minor weather advisory
    - moderate: Weather warning
    - high: Severe weather warning
    - critical: Extreme weather alert
    """
    # Check if weather_alert_level column exists
    if "weather_alert_level" not in df.columns:
        df = df.withColumn("weather_alert_level", F.lit(None).cast(StringType()))
    
    df_normalized = df.withColumn(
        "alert_level_normalized",
        F.when(
            F.col("weather_alert_level").isNull(),
            "none"
        ).when(
            F.lower(F.trim(F.col("weather_alert_level"))).isin(["green", "low", "minor", "advisory"]),
            "low"
        ).when(
            F.lower(F.trim(F.col("weather_alert_level"))).isin(["yellow", "moderate", "warning", "medium"]),
            "moderate"
        ).when(
            F.lower(F.trim(F.col("weather_alert_level"))).isin(["orange", "high", "severe"]),
            "high"
        ).when(
            F.lower(F.trim(F.col("weather_alert_level"))).isin(["red", "critical", "extreme", "dangerous"]),
            "critical"
        ).otherwise("none")
    )
    
    return df_normalized


def add_temporal_features(df: DataFrame) -> DataFrame:
    """
    Add temporal features for time-series analysis.
    
    Features:
    - year, month, day, hour
    - day_of_week, week_of_year
    - is_weekend, is_daylight_hours
    - season classification
    """
    df_temporal = df \
        .withColumn("year", F.year("timestamp")) \
        .withColumn("month", F.month("timestamp")) \
        .withColumn("day", F.dayofmonth("timestamp")) \
        .withColumn("hour", F.hour("timestamp")) \
        .withColumn("day_of_week", F.dayofweek("timestamp")) \
        .withColumn("week_of_year", F.weekofyear("timestamp"))
    
    df_temporal = df_temporal.withColumn(
        "is_weekend",
        F.when(F.col("day_of_week").isin([1, 7]), True).otherwise(False)
    )
    
    # Daylight hours (approximate: 6am-8pm)
    df_temporal = df_temporal.withColumn(
        "is_daylight_hours",
        F.when(
            (F.col("hour") >= 6) & (F.col("hour") <= 20),
            True
        ).otherwise(False)
    )
    
    # Season classification (Northern Hemisphere)
    df_temporal = df_temporal.withColumn(
        "season",
        F.when(F.col("month").isin([12, 1, 2]), "winter")
         .when(F.col("month").isin([3, 4, 5]), "spring")
         .when(F.col("month").isin([6, 7, 8]), "summer")
         .otherwise("autumn")
    )
    
    return df_temporal


def add_weather_indicators(df: DataFrame) -> DataFrame:
    """
    Add reusable weather indicators for energy forecasting.
    
    Indicators:
    - Temperature categories (freezing, cold, mild, warm, hot)
    - Wind power potential (based on wind speed cubed)
    - Heating/cooling demand scores
    - Precipitation intensity classification
    - Weather comfort index
    - Renewable energy favorability
    """
    # Temperature categories
    df_indicators = df.withColumn(
        "temp_category",
        F.when(F.col("temperature_c") < 0, "freezing")
         .when(F.col("temperature_c") < 10, "cold")
         .when(F.col("temperature_c") < 20, "mild")
         .when(F.col("temperature_c") < 30, "warm")
         .otherwise("hot")
    )
    
    # Wind power potential (simplified Betz limit calculation)
    # Power ∝ v³, only effective between 3-25 m/s
    df_indicators = df_indicators.withColumn(
        "wind_power_potential",
        F.when(
            F.col("wind_speed_ms").between(3, 25),
            F.pow(F.col("wind_speed_ms"), 3) * 0.5
        ).otherwise(0.0)
    )
    
    # Wind speed category
    df_indicators = df_indicators.withColumn(
        "wind_category",
        F.when(F.col("wind_speed_ms") < 5, "calm")
         .when(F.col("wind_speed_ms") < 10, "moderate")
         .when(F.col("wind_speed_ms") < 15, "fresh")
         .when(F.col("wind_speed_ms") < 20, "strong")
         .otherwise("storm")
    )
    
    # Heating demand score (degree-days concept)
    # Higher score = more heating needed (base temperature 18°C)
    df_indicators = df_indicators.withColumn(
        "heating_demand_score",
        F.when(
            F.col("temperature_c") < 18,
            18 - F.col("temperature_c")
        ).otherwise(0)
    )
    
    # Cooling demand score (base temperature 22°C)
    df_indicators = df_indicators.withColumn(
        "cooling_demand_score",
        F.when(
            F.col("temperature_c") > 22,
            F.col("temperature_c") - 22
        ).otherwise(0)
    )
    
    # Precipitation intensity classification
    df_indicators = df_indicators.withColumn(
        "precipitation_intensity",
        F.when(F.col("precipitation_mm") == 0, "none")
         .when(F.col("precipitation_mm") < 2.5, "light")
         .when(F.col("precipitation_mm") < 10, "moderate")
         .when(F.col("precipitation_mm") < 50, "heavy")
         .otherwise("extreme")
    )
    
    # Weather discomfort index (simplified)
    # Combination of temperature extremes and precipitation
    df_indicators = df_indicators.withColumn(
        "weather_discomfort_index",
        F.when(
            (F.col("temperature_c") < 0) | (F.col("temperature_c") > 30),
            F.abs(F.col("temperature_c") - 15) + F.col("precipitation_mm") * 2
        ).otherwise(
            F.abs(F.col("temperature_c") - 20) + F.col("precipitation_mm")
        )
    )
    
    # Renewable energy favorability
    # Good for wind (5-20 m/s) OR solar (low cloud cover)
    df_indicators = df_indicators.withColumn(
        "renewable_energy_favorable",
        F.when(
            (F.col("wind_speed_ms").between(5, 20)) |
            (F.col("cloud_cover_percent") < 30),
            True
        ).otherwise(False)
    )
    
    return df_indicators


def calculate_weather_statistics(df: DataFrame) -> DataFrame:
    """
    Calculate rolling statistics for weather patterns.
    
    Statistics:
    - Hour-over-hour changes (temperature, wind speed)
    - 24-hour rolling averages and standard deviations
    - Volatility metrics
    """
    window_region = Window.partitionBy("region_normalized").orderBy("timestamp")
    
    # Temperature change from previous hour
    df_stats = df.withColumn(
        "temp_change_hourly",
        F.col("temperature_c") - F.lag("temperature_c", 1).over(window_region)
    )
    
    # Wind speed change
    df_stats = df_stats.withColumn(
        "wind_change_hourly",
        F.col("wind_speed_ms") - F.lag("wind_speed_ms", 1).over(window_region)
    )
    
    # 24-hour rolling window
    window_24h = Window.partitionBy("region_normalized") \
        .orderBy("timestamp") \
        .rowsBetween(-23, 0)
    
    df_stats = df_stats \
        .withColumn("temp_24h_avg", F.avg("temperature_c").over(window_24h)) \
        .withColumn("temp_24h_stddev", F.stddev("temperature_c").over(window_24h)) \
        .withColumn("wind_24h_avg", F.avg("wind_speed_ms").over(window_24h)) \
        .withColumn("wind_24h_stddev", F.stddev("wind_speed_ms").over(window_24h)) \
        .withColumn("precipitation_24h_total", F.sum("precipitation_mm").over(window_24h))
    
    # Temperature volatility score
    df_stats = df_stats.withColumn(
        "temp_volatility_score",
        F.when(
            (F.col("temp_24h_avg").isNotNull()) & (F.col("temp_24h_stddev").isNotNull()),
            F.col("temp_24h_stddev")
        ).otherwise(None)
    )
    
    return df_stats


def enrich_with_metadata(df: DataFrame) -> DataFrame:
    """
    Add metadata and audit columns for silver layer.
    
    Metadata:
    - silver_processing_timestamp
    - data_source
    - silver_version
    - data_quality_score
    """
    df_enriched = df \
        .withColumn("silver_processing_timestamp", F.current_timestamp()) \
        .withColumn("data_source", F.lit("bronze_weather")) \
        .withColumn("silver_version", F.lit("v1.0"))
    
    # Data quality score
    df_enriched = df_enriched.withColumn(
        "data_quality_score",
        F.when(F.col("is_extreme_temp") == True, 0.8)
         .when(F.col("is_high_wind") == True, 0.8)
         .when(F.col("_rescued_data").isNotNull(), 0.5)
         .otherwise(1.0)
    )
    
    return df_enriched


def transform_bronze_to_silver(df: DataFrame) -> DataFrame:
    """
    Complete transformation pipeline from bronze to silver layer for weather data.
    
    Pipeline steps:
    1. Clean and validate measurements
    2. Create report_day
    3. Normalize region (cities to country codes)
    4. Normalize alert levels
    5. Add temporal features
    6. Add weather indicators
    7. Calculate statistics
    8. Enrich with metadata
    
    Returns:
    DataFrame: Transformed silver layer weather data
    """
    df_silver = (
        df
        .transform(clean_weather_data)
        .transform(create_report_day)
        .transform(normalize_region)
        .transform(normalize_alert_level)
        .transform(add_temporal_features)
        .transform(add_weather_indicators)
        .transform(calculate_weather_statistics)
        .transform(enrich_with_metadata)
    )
    
    return df_silver

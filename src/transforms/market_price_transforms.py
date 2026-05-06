# Market Price Transformations for Silver Layer

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType, DateType, StringType
from pyspark.sql.window import Window


def clean_market_prices(df: DataFrame) -> DataFrame:
    """
    Clean and standardize market price data from bronze layer.
    
    Operations:
    - Remove duplicate records
    - Cast data types correctly (timestamp, price_eur_mwh, volume_mwh)
    - Handle null values
    - Validate price ranges
    - Filter out invalid records
    """
    # Remove duplicates
    df_clean = df.dropDuplicates(["timestamp", "market_zone"])
    
    # Cast timestamp
    df_clean = df_clean.withColumn(
        "timestamp",
        F.col("timestamp").cast(TimestampType())
    )
    
    # Cast numeric columns
    df_clean = df_clean.withColumn(
        "price_eur_mwh",
        F.col("price_eur_mwh").cast(DoubleType())
    )
    
    df_clean = df_clean.withColumn(
        "volume_mwh",
        F.col("volume_mwh").cast(DoubleType())
    )
    
    # Filter invalid records (null timestamp, null market_zone, null or negative prices)
    df_clean = df_clean.filter(
        (F.col("timestamp").isNotNull()) &
        (F.col("market_zone").isNotNull()) &
        (F.col("price_eur_mwh").isNotNull()) &
        (F.col("price_eur_mwh") >= 0)
    )
    
    # Flag price outliers
    df_clean = df_clean.withColumn(
        "is_price_outlier",
        F.when(
            (F.col("price_eur_mwh") > 500) | (F.col("price_eur_mwh") < -100),
            True
        ).otherwise(False)
    )
    
    return df_clean


def create_report_day(df: DataFrame) -> DataFrame:
    """
    Create report_day column from timestamp for daily aggregations.
    
    The report_day is the date component of the timestamp, useful for:
    - Daily aggregations
    - Partitioning strategies
    - Time-based filtering
    """
    df_with_day = df.withColumn(
        "report_day",
        F.to_date(F.col("timestamp"))
    )
    
    return df_with_day


def normalize_market_type(df: DataFrame) -> DataFrame:
    """
    Normalize market_type values to standardized categories.
    
    Standardized categories:
    - day_ahead: Day-ahead market
    - intraday: Intraday market
    - balancing: Balancing/regulation market
    - unknown: Unknown or invalid market type
    """
    df_normalized = df.withColumn(
        "market_type_normalized",
        F.when(
            F.lower(F.trim(F.col("market_type"))).isin(["day_ahead", "day-ahead", "da"]),
            "day_ahead"
        ).when(
            F.lower(F.trim(F.col("market_type"))).isin(["intraday", "intra-day", "id"]),
            "intraday"
        ).when(
            F.lower(F.trim(F.col("market_type"))).isin(["balancing", "regulation", "bal"]),
            "balancing"
        ).otherwise("unknown")
    )
    
    return df_normalized


def normalize_region(df: DataFrame) -> DataFrame:
    """
    Extract and normalize region from market_zone.
    
    Examples:
    - DK1, DK2 -> DK (Denmark)
    - SE1, SE2, SE3, SE4 -> SE (Sweden)
    - NO1, NO2, NO3, NO4, NO5 -> NO (Norway)
    - FI -> FI (Finland)
    
    Also adds a zone_number column for zones with numeric suffixes.
    """
    # Extract region (country code) from market_zone
    df_with_region = df.withColumn(
        "region_normalized",
        F.regexp_extract(F.col("market_zone"), r"^([A-Z]+)", 1)
    )
    
    # Extract zone number if present
    df_with_region = df_with_region.withColumn(
        "zone_number",
        F.regexp_extract(F.col("market_zone"), r"([0-9]+)$", 1)
    )
    
    # Convert empty zone_number to null
    df_with_region = df_with_region.withColumn(
        "zone_number",
        F.when(F.col("zone_number") == "", None)
         .otherwise(F.col("zone_number").cast("integer"))
    )
    
    return df_with_region


def add_temporal_features(df: DataFrame) -> DataFrame:
    """
    Add temporal features for time-series analysis.
    
    Features:
    - year, month, day, hour
    - day_of_week (1=Sunday, 7=Saturday)
    - week_of_year
    - is_weekend (boolean)
    - hour_category (peak 7-22, off_peak)
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
    
    df_temporal = df_temporal.withColumn(
        "hour_category",
        F.when(
            (F.col("hour") >= 7) & (F.col("hour") <= 22),
            "peak"
        ).otherwise("off_peak")
    )
    
    return df_temporal


def calculate_price_statistics(df: DataFrame) -> DataFrame:
    """
    Calculate rolling statistics and derived price metrics.
    
    Metrics:
    - price_change_hourly: Hour-over-hour price change
    - price_change_pct: Percentage change
    - price_24h_avg: 24-hour rolling average
    - price_24h_stddev: 24-hour rolling standard deviation
    - price_volatility_score: Normalized volatility metric
    """
    window_zone = Window.partitionBy("market_zone").orderBy("timestamp")
    
    # Hour-over-hour price change
    df_stats = df.withColumn(
        "price_change_hourly",
        F.col("price_eur_mwh") - F.lag("price_eur_mwh", 1).over(window_zone)
    )
    
    # Percentage change
    df_stats = df_stats.withColumn(
        "price_change_pct",
        F.when(
            F.lag("price_eur_mwh", 1).over(window_zone) != 0,
            (F.col("price_change_hourly") / F.lag("price_eur_mwh", 1).over(window_zone)) * 100
        ).otherwise(None)
    )
    
    # 24-hour rolling window
    window_24h = Window.partitionBy("market_zone") \
        .orderBy("timestamp") \
        .rowsBetween(-23, 0)
    
    df_stats = df_stats.withColumn(
        "price_24h_avg",
        F.avg("price_eur_mwh").over(window_24h)
    )
    
    df_stats = df_stats.withColumn(
        "price_24h_stddev",
        F.stddev("price_eur_mwh").over(window_24h)
    )
    
    # Volatility score (coefficient of variation)
    df_stats = df_stats.withColumn(
        "price_volatility_score",
        F.when(
            (F.col("price_24h_avg").isNotNull()) & (F.col("price_24h_avg") != 0),
            (F.col("price_24h_stddev") / F.col("price_24h_avg")) * 100
        ).otherwise(None)
    )
    
    return df_stats


def add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Add useful derived columns for business analysis.
    
    Columns:
    - price_per_volume: Price efficiency metric (EUR/MWh normalized by volume)
    - is_high_price: Flag for prices above 75th percentile
    - is_low_price: Flag for prices below 25th percentile
    - price_category: categorical price level (very_low, low, medium, high, very_high)
    """
    # Price efficiency (only if volume is available and > 0)
    df_derived = df.withColumn(
        "price_per_volume",
        F.when(
            (F.col("volume_mwh").isNotNull()) & (F.col("volume_mwh") > 0),
            F.col("price_eur_mwh") / F.col("volume_mwh")
        ).otherwise(None)
    )
    
    # Price level flags (using 24h average as baseline)
    df_derived = df_derived.withColumn(
        "is_high_price",
        F.when(
            (F.col("price_24h_avg").isNotNull()) &
            (F.col("price_eur_mwh") > F.col("price_24h_avg") * 1.25),
            True
        ).otherwise(False)
    )
    
    df_derived = df_derived.withColumn(
        "is_low_price",
        F.when(
            (F.col("price_24h_avg").isNotNull()) &
            (F.col("price_eur_mwh") < F.col("price_24h_avg") * 0.75),
            True
        ).otherwise(False)
    )
    
    # Categorical price level
    df_derived = df_derived.withColumn(
        "price_category",
        F.when(F.col("price_eur_mwh") < 20, "very_low")
         .when(F.col("price_eur_mwh") < 50, "low")
         .when(F.col("price_eur_mwh") < 100, "medium")
         .when(F.col("price_eur_mwh") < 200, "high")
         .otherwise("very_high")
    )
    
    return df_derived


def enrich_with_metadata(df: DataFrame) -> DataFrame:
    """
    Add metadata and audit columns for silver layer.
    
    Metadata:
    - silver_processing_timestamp: When record was processed
    - data_source: Source table name
    - silver_version: Version of transformation logic
    - data_quality_score: Overall quality score (0.0-1.0)
    """
    df_enriched = df \
        .withColumn("silver_processing_timestamp", F.current_timestamp()) \
        .withColumn("data_source", F.lit("bronze_market_prices")) \
        .withColumn("silver_version", F.lit("v2.0"))
    
    # Data quality score based on completeness and outliers
    df_enriched = df_enriched.withColumn(
        "data_quality_score",
        F.when(F.col("is_price_outlier") == True, 0.7)
         .when(F.col("_rescued_data").isNotNull(), 0.5)
         .when(F.col("volume_mwh").isNull(), 0.8)
         .otherwise(1.0)
    )
    
    return df_enriched


def transform_bronze_to_silver(df: DataFrame) -> DataFrame:
    """
    Complete transformation pipeline from bronze to silver layer.
    
    Pipeline steps:
    1. Clean and validate data
    2. Create report_day
    3. Normalize market_type
    4. Normalize region
    5. Add temporal features
    6. Calculate price statistics
    7. Add derived columns
    8. Enrich with metadata
    
    Returns:
    DataFrame: Transformed silver layer data
    """
    df_silver = (
        df
        .transform(clean_market_prices)
        .transform(create_report_day)
        .transform(normalize_market_type)
        .transform(normalize_region)
        .transform(add_temporal_features)
        .transform(calculate_price_statistics)
        .transform(add_derived_columns)
        .transform(enrich_with_metadata)
    )
    
    return df_silver

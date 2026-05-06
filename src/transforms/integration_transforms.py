# Integration Transformations for Cross-Domain Analytics

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def join_prices_with_weather(df_prices: DataFrame, df_weather: DataFrame) -> DataFrame:
    """
    Join market prices with weather data by timestamp and location.
    
    Uses temporal join with 1-hour tolerance for matching.
    """
    # Create hour-level timestamp for joining
    df_prices_hourly = df_prices.withColumn(
        "timestamp_hour",
        F.date_trunc("hour", F.col("timestamp"))
    )
    
    df_weather_hourly = df_weather.withColumn(
        "timestamp_hour",
        F.date_trunc("hour", F.col("timestamp"))
    )
    
    # Join on hour-level timestamp
    df_joined = df_prices_hourly.join(
        df_weather_hourly,
        on="timestamp_hour",
        how="left"
    )
    
    return df_joined


def join_with_grid_events(df_base: DataFrame, df_events: DataFrame, 
                          window_hours: int = 24) -> DataFrame:
    """
    Enrich base dataset with grid event information.
    
    Args:
        df_base: Base DataFrame (prices or weather)
        df_events: Grid events DataFrame
        window_hours: Time window for event aggregation (default 24h)
    """
    # Create broadcast join key
    df_base = df_base.withColumn(
        "join_hour",
        F.date_trunc("hour", F.col("timestamp"))
    )
    
    df_events = df_events.withColumn(
        "join_hour",
        F.date_trunc("hour", F.col("timestamp"))
    )
    
    # Aggregate events by hour and location
    df_events_agg = df_events.groupBy("join_hour", "location_id").agg(
        F.count("event_id").alias("event_count"),
        F.max("severity_score").alias("max_severity"),
        F.avg("severity_score").alias("avg_severity"),
        F.countDistinct("event_category").alias("distinct_event_types")
    )
    
    # Join with base data
    df_joined = df_base.join(
        df_events_agg,
        on=["join_hour", "location_id"],
        how="left"
    )
    
    # Fill nulls for hours with no events
    df_joined = df_joined \
        .fillna(0, subset=["event_count", "max_severity", "avg_severity", "distinct_event_types"])
    
    # Add event impact flag
    df_joined = df_joined.withColumn(
        "has_grid_events",
        F.when(F.col("event_count") > 0, True).otherwise(False)
    )
    
    return df_joined


def create_integrated_fact_table(df_prices: DataFrame, 
                                 df_weather: DataFrame, 
                                 df_events: DataFrame) -> DataFrame:
    """
    Create comprehensive integrated fact table joining all data sources.
    
    This is the primary integration function for analytics.
    """
    # Join prices with weather
    df_integrated = join_prices_with_weather(df_prices, df_weather)
    
    # Enrich with grid events
    df_integrated = join_with_grid_events(df_integrated, df_events)
    
    # Add integration metadata
    df_integrated = df_integrated \
        .withColumn("integrated_at", F.current_timestamp()) \
        .withColumn("integration_version", F.lit("v1.0"))
    
    return df_integrated


def calculate_correlation_features(df: DataFrame) -> DataFrame:
    """
    Calculate correlation features between price, weather, and events.
    """
    # Price-temperature correlation indicator
    df_corr = df.withColumn(
        "temp_impact_flag",
        F.when(
            (F.col("temperature_c") < 0) & (F.col("price_eur_mwh") > F.col("price_24h_avg")),
            "cold_high_price"
        ).when(
            (F.col("temperature_c") > 25) & (F.col("price_eur_mwh") > F.col("price_24h_avg")),
            "hot_high_price"
        ).otherwise("normal")
    )
    
    # Wind generation vs price relationship
    df_corr = df_corr.withColumn(
        "wind_price_relationship",
        F.when(
            (F.col("wind_power_potential") > 100) & 
            (F.col("price_eur_mwh") < F.col("price_24h_avg")),
            "high_wind_low_price"
        ).when(
            (F.col("wind_power_potential") < 50) & 
            (F.col("price_eur_mwh") > F.col("price_24h_avg")),
            "low_wind_high_price"
        ).otherwise("normal")
    )
    
    # Event impact on prices
    df_corr = df_corr.withColumn(
        "event_price_impact",
        F.when(
            (F.col("has_grid_events")) & 
            (F.col("max_severity") >= 60) &
            (F.col("price_eur_mwh") > F.col("price_24h_avg")),
            "event_caused_spike"
        ).otherwise("no_impact")
    )
    
    return df_corr


def aggregate_daily_summary(df: DataFrame) -> DataFrame:
    """
    Create daily summary aggregations across all data sources.
    """
    df_daily = df.groupBy(
        F.date_trunc("day", F.col("timestamp")).alias("date"),
        "market_zone",
        "location_id"
    ).agg(
        # Price metrics
        F.avg("price_eur_mwh").alias("avg_price"),
        F.min("price_eur_mwh").alias("min_price"),
        F.max("price_eur_mwh").alias("max_price"),
        F.stddev("price_eur_mwh").alias("price_volatility"),
        
        # Weather metrics
        F.avg("temperature_c").alias("avg_temperature"),
        F.avg("wind_speed_ms").alias("avg_wind_speed"),
        F.sum("solar_generation_potential").alias("total_solar_potential"),
        
        # Event metrics
        F.sum("event_count").alias("total_events"),
        F.max("max_severity").alias("max_event_severity"),
        
        # Counts
        F.count("*").alias("record_count")
    )
    
    return df_daily


def create_gold_analytics_view(df: DataFrame) -> DataFrame:
    """
    Create final analytics-ready view with business KPIs.
    """
    df_gold = df \
        .transform(calculate_correlation_features) \
        .withColumn("report_date", F.date_trunc("day", F.col("timestamp"))) \
        .withColumn("report_hour", F.hour("timestamp"))
    
    # Add business classification
    df_gold = df_gold.withColumn(
        "market_condition",
        F.when(
            (F.col("price_eur_mwh") > F.col("price_24h_avg") * 1.2) &
            (F.col("price_24h_stddev") > 50),
            "volatile_high"
        ).when(
            (F.col("price_eur_mwh") < F.col("price_24h_avg") * 0.8) &
            (F.col("price_24h_stddev") > 50),
            "volatile_low"
        ).when(
            F.col("price_24h_stddev") < 20,
            "stable"
        ).otherwise("normal")
    )
    
    return df_gold

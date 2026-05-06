# Weather Transformations for Silver Layer

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType, IntegerType
from pyspark.sql.window import Window


def clean_weather_data(df: DataFrame) -> DataFrame:
    """
    Clean and standardize weather data from bronze layer.
    
    Operations:
    - Remove duplicates
    - Cast data types
    - Handle null values and outliers
    - Validate sensor readings
    """
    df_clean = df.dropDuplicates(["timestamp", "location_id"])
    
    df_clean = df_clean.withColumn(
        "timestamp",
        F.col("timestamp").cast(TimestampType())
    )
    
    df_clean = df_clean \
        .withColumn("temperature_c", F.col("temperature_c").cast(DoubleType())) \
        .withColumn("wind_speed_ms", F.col("wind_speed_ms").cast(DoubleType())) \
        .withColumn("humidity_pct", F.col("humidity_pct").cast(DoubleType())) \
        .withColumn("cloud_cover_pct", F.col("cloud_cover_pct").cast(DoubleType())) \
        .withColumn("solar_irradiance_wm2", F.col("solar_irradiance_wm2").cast(DoubleType()))
    
    # Filter outliers and invalid readings
    df_clean = df_clean.filter(
        (F.col("temperature_c").between(-50, 60)) &
        (F.col("wind_speed_ms").between(0, 100)) &
        (F.col("humidity_pct").between(0, 100)) &
        (F.col("cloud_cover_pct").between(0, 100)) &
        (F.col("solar_irradiance_wm2") >= 0)
    )
    
    return df_clean


def add_weather_features(df: DataFrame) -> DataFrame:
    """
    Add derived weather features for energy forecasting.
    """
    # Temperature categories
    df_features = df.withColumn(
        "temp_category",
        F.when(F.col("temperature_c") < 0, "freezing")
         .when(F.col("temperature_c") < 10, "cold")
         .when(F.col("temperature_c") < 20, "mild")
         .when(F.col("temperature_c") < 30, "warm")
         .otherwise("hot")
    )
    
    # Wind power potential (simplified calculation)
    df_features = df_features.withColumn(
        "wind_power_potential",
        F.when(
            F.col("wind_speed_ms").between(3, 25),
            F.pow(F.col("wind_speed_ms"), 3) * 0.5
        ).otherwise(0.0)
    )
    
    # Solar generation potential
    df_features = df_features.withColumn(
        "solar_generation_potential",
        F.col("solar_irradiance_wm2") * (1 - F.col("cloud_cover_pct") / 100) * 0.2
    )
    
    # Heating/cooling demand indicator
    df_features = df_features.withColumn(
        "heating_demand_score",
        F.when(F.col("temperature_c") < 18, 18 - F.col("temperature_c")).otherwise(0)
    )
    
    df_features = df_features.withColumn(
        "cooling_demand_score",
        F.when(F.col("temperature_c") > 22, F.col("temperature_c") - 22).otherwise(0)
    )
    
    return df_features


def calculate_weather_statistics(df: DataFrame) -> DataFrame:
    """
    Calculate rolling statistics for weather patterns.
    """
    window_location = Window.partitionBy("location_id").orderBy("timestamp")
    
    # Temperature change from previous reading
    df_stats = df.withColumn(
        "temp_change",
        F.col("temperature_c") - F.lag("temperature_c", 1).over(window_location)
    )
    
    # 24-hour rolling averages
    window_24h = Window.partitionBy("location_id") \
        .orderBy("timestamp") \
        .rowsBetween(-23, 0)
    
    df_stats = df_stats \
        .withColumn("temp_24h_avg", F.avg("temperature_c").over(window_24h)) \
        .withColumn("wind_24h_avg", F.avg("wind_speed_ms").over(window_24h)) \
        .withColumn("solar_24h_avg", F.avg("solar_irradiance_wm2").over(window_24h))
    
    return df_stats


def add_temporal_context(df: DataFrame) -> DataFrame:
    """
    Add temporal context for weather patterns.
    """
    df_temporal = df \
        .withColumn("year", F.year("timestamp")) \
        .withColumn("month", F.month("timestamp")) \
        .withColumn("day", F.dayofmonth("timestamp")) \
        .withColumn("hour", F.hour("timestamp")) \
        .withColumn("day_of_week", F.dayofweek("timestamp"))
    
    # Season classification
    df_temporal = df_temporal.withColumn(
        "season",
        F.when(F.col("month").isin([12, 1, 2]), "winter")
         .when(F.col("month").isin([3, 4, 5]), "spring")
         .when(F.col("month").isin([6, 7, 8]), "summer")
         .otherwise("autumn")
    )
    
    return df_temporal


def transform_bronze_to_silver(df: DataFrame) -> DataFrame:
    """
    Complete transformation pipeline for weather data.
    """
    df_silver = (
        df
        .transform(clean_weather_data)
        .transform(add_weather_features)
        .transform(calculate_weather_statistics)
        .transform(add_temporal_context)
        .withColumn("silver_processing_timestamp", F.current_timestamp())
        .withColumn("data_source", F.lit("bronze_weather"))
    )
    
    return df_silver

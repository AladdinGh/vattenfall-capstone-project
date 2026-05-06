# Asset Reference Transformations for Silver Layer
# Version 1.0 - Stable lookup structure for asset reference data

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DateType
from datetime import datetime


def validate_and_clean_substations(df: DataFrame) -> DataFrame:
    """
    Validate and clean substation reference data.
    
    Operations:
    - Remove duplicates by substation_id
    - Filter null critical fields
    - Standardize substation_id format (ensure uppercase)
    - Trim whitespace from string fields
    """
    # Remove duplicates
    df_clean = df.dropDuplicates(["substation_id"])
    
    # Filter null critical fields
    df_clean = df_clean.filter(
        (F.col("substation_id").isNotNull()) &
        (F.col("region_code").isNotNull()) &
        (F.col("voltage_kv").isNotNull())
    )
    
    # Standardize substation_id to uppercase
    df_clean = df_clean.withColumn(
        "substation_id",
        F.upper(F.trim(F.col("substation_id")))
    )
    
    # Trim whitespace from string fields
    df_clean = df_clean \
        .withColumn("substation_name", F.trim(F.col("substation_name"))) \
        .withColumn("region_code", F.upper(F.trim(F.col("region_code"))))
    
    return df_clean


def extract_country_code(df: DataFrame) -> DataFrame:
    """
    Extract country code from region_code.
    
    Maps region codes (FI-01, DK-01, NO-02, SE-03) to country codes (FI, DK, NO, SE).
    """
    df_with_country = df.withColumn(
        "country_code",
        F.when(F.col("region_code").startswith("FI"), "FI")
         .when(F.col("region_code").startswith("DK"), "DK")
         .when(F.col("region_code").startswith("NO"), "NO")
         .when(F.col("region_code").startswith("SE"), "SE")
         .otherwise(None)
    )
    
    # Extract numeric zone from region_code (FI-01 → 01)
    df_with_country = df_with_country.withColumn(
        "region_zone",
        F.regexp_extract(F.col("region_code"), r"-(\d+)$", 1)
    )
    
    return df_with_country


def classify_voltage_level(df: DataFrame) -> DataFrame:
    """
    Classify substations by voltage level.
    
    Categories:
    - extra_high: >= 400 kV
    - high: 220-399 kV
    - medium: 132-219 kV
    - low: < 132 kV
    """
    df_classified = df.withColumn(
        "voltage_level",
        F.when(F.col("voltage_kv") >= 400, "extra_high")
         .when(F.col("voltage_kv") >= 220, "high")
         .when(F.col("voltage_kv") >= 132, "medium")
         .otherwise("low")
    )
    
    return df_classified


def classify_capacity(df: DataFrame) -> DataFrame:
    """
    Classify substations by capacity.
    
    Categories:
    - very_large: > 600 MVA
    - large: 400-600 MVA
    - medium: 200-399 MVA
    - small: < 200 MVA
    """
    df_classified = df.withColumn(
        "capacity_category",
        F.when(F.col("capacity_mva") > 600, "very_large")
         .when(F.col("capacity_mva") >= 400, "large")
         .when(F.col("capacity_mva") >= 200, "medium")
         .otherwise("small")
    )
    
    return df_classified


def calculate_asset_age(df: DataFrame, current_year: int = None) -> DataFrame:
    """
    Calculate asset age from commissioned_year.
    
    Args:
        current_year: Reference year for age calculation (defaults to current year)
    """
    if current_year is None:
        current_year = datetime.now().year
    
    df_with_age = df \
        .withColumn("asset_age_years", F.lit(current_year) - F.col("commissioned_year")) \
        .withColumn(
            "asset_age_category",
            F.when(F.col("asset_age_years") < 10, "new")
             .when(F.col("asset_age_years") < 20, "modern")
             .when(F.col("asset_age_years") < 30, "mature")
             .when(F.col("asset_age_years") < 40, "aging")
             .otherwise("old")
        )
    
    return df_with_age


def enrich_with_region_details(df_substations: DataFrame, df_regions: DataFrame) -> DataFrame:
    """
    Enrich substations with region details from ref_regions.
    
    Left join to preserve all substations even if region details are missing.
    """
    # Select and rename columns from regions to avoid conflicts
    df_regions_clean = df_regions.select(
        F.col("region_code").alias("region_code_lookup"),
        F.col("region_name"),
        F.col("country").alias("country_name"),
        F.col("population").alias("region_population"),
        F.col("area_km2").alias("region_area_km2")
    )
    
    # Left join on region_code
    df_enriched = df_substations.join(
        df_regions_clean,
        df_substations["region_code"] == df_regions_clean["region_code_lookup"],
        "left"
    ).drop("region_code_lookup")
    
    # Fill missing country_name from country_code if needed
    df_enriched = df_enriched.withColumn(
        "country_name",
        F.coalesce(
            F.col("country_name"),
            F.when(F.col("country_code") == "FI", "Finland")
             .when(F.col("country_code") == "DK", "Denmark")
             .when(F.col("country_code") == "NO", "Norway")
             .when(F.col("country_code") == "SE", "Sweden")
             .otherwise(None)
        )
    )
    
    return df_enriched


def create_join_keys(df: DataFrame) -> DataFrame:
    """
    Create reliable join keys for use in fact tables.
    
    Keys created:
    - asset_key: Primary key (substation_id)
    - region_key: For region-based joins (region_code)
    - country_key: For country-based aggregations (country_code)
    """
    df_with_keys = df \
        .withColumn("asset_key", F.col("substation_id")) \
        .withColumn("region_key", F.col("region_code")) \
        .withColumn("country_key", F.col("country_code"))
    
    return df_with_keys


def add_data_quality_indicators(df: DataFrame) -> DataFrame:
    """
    Add data quality indicators for each record.
    """
    # Count non-null critical fields
    df_quality = df.withColumn(
        "quality_checks_passed",
        (
            F.when(F.col("substation_id").isNotNull(), 1).otherwise(0) +
            F.when(F.col("substation_name").isNotNull(), 1).otherwise(0) +
            F.when(F.col("region_code").isNotNull(), 1).otherwise(0) +
            F.when(F.col("country_code").isNotNull(), 1).otherwise(0) +
            F.when(F.col("voltage_kv").isNotNull(), 1).otherwise(0) +
            F.when(F.col("capacity_mva").isNotNull(), 1).otherwise(0) +
            F.when(F.col("commissioned_year").isNotNull(), 1).otherwise(0) +
            F.when(F.col("region_name").isNotNull(), 1).otherwise(0)
        )
    )
    
    df_quality = df_quality.withColumn(
        "data_quality_score",
        (F.col("quality_checks_passed") / 8.0 * 100).cast(IntegerType())
    )
    
    # Flag for complete records (all critical fields present)
    df_quality = df_quality.withColumn(
        "is_complete_record",
        F.when(F.col("quality_checks_passed") == 8, 1).otherwise(0)
    )
    
    return df_quality


def add_operational_status(df: DataFrame) -> DataFrame:
    """
    Add operational status indicators based on asset age and characteristics.
    """
    df_status = df.withColumn(
        "requires_modernization",
        F.when(
            (F.col("asset_age_years") > 35) & 
            (F.col("voltage_level").isin(["medium", "low"])),
            1
        ).otherwise(0)
    )
    
    df_status = df_status.withColumn(
        "high_priority_asset",
        F.when(
            (F.col("voltage_level") == "extra_high") |
            (F.col("capacity_category") == "very_large"),
            1
        ).otherwise(0)
    )
    
    return df_status


def enrich_with_metadata(df: DataFrame) -> DataFrame:
    """
    Add metadata fields to track processing.
    """
    df_enriched = df \
        .withColumn("silver_processing_timestamp", F.current_timestamp()) \
        .withColumn("data_source", F.lit("ref_substations")) \
        .withColumn("silver_version", F.lit("1.0")) \
        .withColumn("reference_type", F.lit("substation"))
    
    return df_enriched


def transform_to_silver_asset_reference(df_substations: DataFrame, df_regions: DataFrame) -> DataFrame:
    """
    Complete transformation pipeline for asset reference data.
    
    Applies all transformations in sequence:
    1. Validate and clean substations
    2. Extract country code
    3. Classify voltage level
    4. Classify capacity
    5. Calculate asset age
    6. Enrich with region details
    7. Create join keys
    8. Add data quality indicators
    9. Add operational status
    10. Enrich with metadata
    """
    df_silver = (
        df_substations
        .transform(validate_and_clean_substations)
        .transform(extract_country_code)
        .transform(classify_voltage_level)
        .transform(classify_capacity)
        .transform(calculate_asset_age)
        .transform(lambda df: enrich_with_region_details(df, df_regions))
        .transform(create_join_keys)
        .transform(add_data_quality_indicators)
        .transform(add_operational_status)
        .transform(enrich_with_metadata)
    )
    
    return df_silver

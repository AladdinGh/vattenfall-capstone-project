# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Market Prices - Bronze Layer Ingestion
# MAGIC
# MAGIC **Business Domain:** Wholesale Energy Trading
# MAGIC
# MAGIC **Purpose:** Ingest raw market price data from landing zone to bronze Delta table using Auto Loader.
# MAGIC
# MAGIC **Data Source:** CSV files in `/Volumes/vattenfall_dev/raw/landing/market_prices/`
# MAGIC
# MAGIC **Target Table:** `vattenfall_dev.raw.bronze_market_prices`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Configuration

# COMMAND ----------

import yaml

# Load project configuration
config_path = "/Workspace/Users/gharbi@startsteps.org/vattenfall-capstone-project/config/project_config.yml"

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# Extract configuration values
catalog = config["catalog"]
raw_schema = config["schemas"]["raw"]
landing_volume = config["volumes"]["landing"]
checkpoint_volume = config["volumes"]["checkpoints"]

# Table and path configuration
bronze_table = f"{catalog}.{raw_schema}.bronze_market_prices"
landing_path = f"/Volumes/{catalog}/{raw_schema}/{landing_volume}/market_prices/"
checkpoint_path = f"/Volumes/{catalog}/{raw_schema}/{checkpoint_volume}/market_prices/"

print("=" * 70)
print("CONFIGURATION LOADED")
print("=" * 70)
print(f"Bronze Table:      {bronze_table}")
print(f"Landing Path:      {landing_path}")
print(f"Checkpoint Path:   {checkpoint_path}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configure Auto Loader

# COMMAND ----------

# copy file as AL can not get files unless they are in UC
dbutils.fs.cp(
    "file:/Workspace/Users/gharbi@startsteps.org/vattenfall-capstone-project/sample_data/market_prices/",
    "/Volumes/vattenfall_dev/raw/landing/market_prices/",
    recurse=True
)

# COMMAND ----------

# Auto Loader streaming read
df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option("header", "true")
    .option("inferSchema", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .load(landing_path)
)

print("✅ Auto Loader configured")
df_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write to Bronze Delta Table

# COMMAND ----------

# Write stream to bronze table with checkpoint
query = (
    df_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(bronze_table)
)

query.awaitTermination()
print("✅ Market prices data ingested to bronze layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate Bronze Table

# COMMAND ----------

from pyspark.sql import functions as F

df_bronze = spark.table(bronze_table)

print("=" * 70)
print("BRONZE TABLE VALIDATION")
print("=" * 70)
print(f"Table:       {bronze_table}")
print(f"Row Count:   {df_bronze.count():,}")
print(f"Columns:     {len(df_bronze.columns)}")
print("=" * 70)

display(df_bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Quality Checks

# COMMAND ----------

print("Distinct Market Zones:")
df_bronze.select("market_zone").distinct().orderBy("market_zone").show()

print("\nDate Range:")
df_bronze.select(
    F.min("timestamp").alias("earliest"),
    F.max("timestamp").alias("latest")
).show()

print("\nPrice Statistics (EUR/MWh):")
df_bronze.select(
    F.min("price_eur_mwh").alias("min"),
    F.avg("price_eur_mwh").alias("avg"),
    F.max("price_eur_mwh").alias("max")
).show()

rescued_count = df_bronze.filter(F.col("_rescued_data").isNotNull()).count()
print(f"\nRescued data rows: {rescued_count}")

print("✅ Bronze ingestion complete")

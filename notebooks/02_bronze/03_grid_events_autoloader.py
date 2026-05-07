# Databricks notebook source
# MAGIC %md
# MAGIC # Grid Events - Bronze Layer Ingestion
# MAGIC
# MAGIC **Business Domain:** Operational Reliability
# MAGIC
# MAGIC **Purpose:** Ingest raw grid event data from landing zone to bronze Delta table using Auto Loader.
# MAGIC
# MAGIC **Data Source:** CSV files in `/Volumes/vattenfall_dev/raw/landing/grid_events/`
# MAGIC
# MAGIC **Target Table:** `vattenfall_dev.raw.bronze_grid_events`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Configuration

# COMMAND ----------

import yaml

config_path = "/Workspace/Users/gharbi@startsteps.org/vattenfall-capstone-project/config/project_config.yml"

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

catalog = config["catalog"]
raw_schema = config["schemas"]["raw"]
landing_volume = config["volumes"]["landing"]
checkpoint_volume = config["volumes"]["checkpoints"]

bronze_table = f"{catalog}.{raw_schema}.bronze_grid_events"
landing_path = f"/Volumes/{catalog}/{raw_schema}/{landing_volume}/grid_events/"
checkpoint_path = f"/Volumes/{catalog}/{raw_schema}/{checkpoint_volume}/grid_events/"

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
print("✅ Grid events data ingested to bronze layer")

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

display(df_bronze.limit(168))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Quality Checks

# COMMAND ----------

print("Event Types:")
df_bronze.groupBy("event_type").count().orderBy("count", ascending=False).show()

print("\nSeverity Distribution:")
df_bronze.groupBy("severity").count().orderBy("severity").show()

print("\nDate Range:")
df_bronze.select(
    F.min("timestamp").alias("earliest"),
    F.max("timestamp").alias("latest")
).show()

print("\nAffected Customers Statistics:")
df_bronze.select(
    F.sum("affected_customers").alias("total_affected"),
    F.avg("affected_customers").alias("avg_affected"),
    F.max("affected_customers").alias("max_affected")
).show()

print("\nDuration Statistics (minutes):")
df_bronze.select(
    F.min("duration_minutes").alias("min_duration"),
    F.avg("duration_minutes").alias("avg_duration"),
    F.max("duration_minutes").alias("max_duration")
).show()

rescued_count = df_bronze.filter(F.col("_rescued_data").isNotNull()).count()
print(f"\nRescued data rows: {rescued_count}")

print("✅ Bronze ingestion complete")

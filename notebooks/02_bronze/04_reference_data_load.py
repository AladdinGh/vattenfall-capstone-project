# Databricks notebook source
# MAGIC %md
# MAGIC # Reference Data - Bronze Layer Loading
# MAGIC
# MAGIC **Business Domain:** Master Data
# MAGIC
# MAGIC **Purpose:** Load reference/lookup tables from landing zone to bronze Delta tables.
# MAGIC
# MAGIC **Data Source:** CSV files in `/Volumes/vattenfall_dev/raw/landing/reference/`
# MAGIC
# MAGIC **Target Tables:** Multiple reference tables in `vattenfall_dev.raw` schema

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

landing_path = f"/Volumes/{catalog}/{raw_schema}/{landing_volume}/reference/"

print("=" * 70)
print("CONFIGURATION LOADED")
print("=" * 70)
print(f"Catalog:         {catalog}")
print(f"Schema:          {raw_schema}")
print(f"Landing Path:    {landing_path}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Reference Tables
# MAGIC
# MAGIC Reference data is typically small and static, so we use batch loading instead of streaming.

# COMMAND ----------

# Define reference tables to load
reference_tables = [
    "regions",
    "substations",
    "equipment_types",
    "market_zones",
    "facility_types"
]

# Load each reference table
for table_name in reference_tables:
    print(f"\n{'='*70}")
    print(f"Loading: {table_name}")
    print(f"{'='*70}")
    
    # Read CSV
    file_path = f"{landing_path}{table_name}.csv"
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
    
    # Target table
    target_table = f"{catalog}.{raw_schema}.ref_{table_name}"
    
    # Write to Delta table (overwrite mode for reference data)
    df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    
    # Validation
    row_count = df.count()
    print(f"✅ Loaded {row_count:,} rows to {target_table}")
    display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate All Reference Tables

# COMMAND ----------

print("=" * 70)
print("REFERENCE TABLES SUMMARY")
print("=" * 70)

for table_name in reference_tables:
    target_table = f"{catalog}.{raw_schema}.ref_{table_name}"
    df = spark.table(target_table)
    print(f"{target_table:<50} {df.count():>10,} rows")

print("=" * 70)
print("✅ All reference data loaded successfully")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Reference Data Preview

# COMMAND ----------

# Show sample data from each reference table
for table_name in reference_tables:
    target_table = f"{catalog}.{raw_schema}.ref_{table_name}"
    print(f"\n{'='*70}")
    print(f"Table: {target_table}")
    print(f"{'='*70}")
    display(spark.table(target_table).limit(10))

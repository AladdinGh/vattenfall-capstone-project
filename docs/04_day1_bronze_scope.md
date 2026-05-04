# Day 1 Bronze Scope

## What is Day 1?

Day 1 is about **project setup and bronze layer ingestion**. You are not building the full pipeline yet. You are establishing the foundation that the rest of the week will build on.

## Core Objectives

### 1. Repository Structure ✅
- Create a well-organized GitHub repository
- Include proper README, .gitignore, folder structure
- Add meaningful documentation in `docs/`

### 2. Unity Catalog Setup ✅
- Create catalog `vattenfall_dev`
- Create schemas: `raw`, `refined`, `analytics`
- Create volumes: `landing`, `checkpoints`

### 3. Configuration Layer ✅
- Create `config/project_config.yml` with catalog, schemas, tables, paths
- Make config values reusable across notebooks

### 4. Sample Data ✅
- Organize sample CSV files by domain:
  - `sample_data/market_prices/`
  - `sample_data/weather/`
  - `sample_data/grid_events/`
  - `sample_data/reference/`

### 5. Bronze Ingestion (Auto Loader) ✅
**Minimum:** Implement at least **ONE** working Auto Loader bronze ingestion.

**Recommended:** All four domains:
- Market prices
- Weather
- Grid events
- Reference data

**What "working" means:**
- Auto Loader reads CSV files from landing volume
- Writes to bronze Delta table in `vattenfall_dev.raw` schema
- Uses checkpoints for incremental processing
- Handles schema evolution with `_rescued_data`

### 6. Bronze Validation ✅
- SQL queries to validate bronze tables exist
- Row count checks
- Sample data inspection
- Schema verification

## What is NOT Day 1 Scope

❌ **Silver layer** - Data cleaning and validation (Day 2-3)  
❌ **Gold layer** - Business aggregations (Day 3-4)  
❌ **Dashboards** - Visualization (Day 4-5)  
❌ **Workflows** - Job orchestration (Day 5)  
❌ **Advanced governance** - Row/column filters, masking (Day 5)

## Success Criteria

At the end of Day 1, you should have:

1. ✅ A well-structured GitHub repository with documentation
2. ✅ Unity Catalog with catalog, schemas, and volumes created
3. ✅ `config/project_config.yml` with reusable settings
4. ✅ Sample CSV files organized by domain
5. ✅ At least **one working Auto Loader bronze ingestion**
6. ✅ SQL validation queries confirming bronze data exists

## Recommended Notebook Flow

### Setup Phase
1. `01_setup/01_project_overview.py` - Document the project
2. `01_setup/02_uc_setup.py` - Create catalog, schemas, volumes
3. `01_setup/03_landing_and_checkpoint_setup.py` - Prepare volumes

### Ingestion Phase
4. `02_bronze/01_market_prices_autoloader.py` - Ingest market prices
5. `02_bronze/02_weather_autoloader.py` - Ingest weather data
6. `02_bronze/03_grid_events_autoloader.py` - Ingest grid events
7. `02_bronze/04_reference_data_load.py` - Load reference tables

### Validation Phase
8. `02_bronze/05_bronze_validation.py` - Validate all bronze tables

## Key Patterns to Demonstrate

### Auto Loader Pattern
```python
df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option("header", "true")
    .load(landing_path)
)

query = (
    df_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(bronze_table)
)
```

### Config Loading Pattern
```python
import yaml

with open("config/project_config.yml", "r") as f:
    config = yaml.safe_load(f)

catalog = config["catalog"]
bronze_table = f"{catalog}.{config['schemas']['raw']}.{config['tables']['bronze_market_prices']}"
```

## Time Management

**Expected time:** 4-6 hours

- Repository setup: 1 hour
- Unity Catalog setup: 30 minutes
- Config and sample data: 1 hour
- First Auto Loader notebook: 1-2 hours
- Additional Auto Loaders: 1-2 hours
- Validation: 30 minutes

## Getting Unstuck

**If you're stuck:**
1. Review the documentation in `docs/`
2. Check your `config/project_config.yml` for typos
3. Verify Unity Catalog objects exist with `SHOW CATALOGS`, `SHOW SCHEMAS`
4. Check Auto Loader checkpoint path permissions
5. Look for `_rescued_data` in bronze tables (indicates schema issues)
6. Ask instructors for help

## What Makes a Strong Day 1 Submission

**Strong:**
- Clear README with business context and architecture
- Well-organized repo structure
- Meaningful documentation
- Working Auto Loader for multiple domains
- Clean, readable notebook code
- Validation queries confirming success

**Weak:**
- Empty or minimal README
- Flat file structure
- No documentation
- Only one notebook with hardcoded values
- No validation of results

Remember: Day 1 is about **demonstrating engineering discipline**, not just getting code to run.

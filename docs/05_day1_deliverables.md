# Day 1 Deliverables — Checklist

Use this checklist to verify you've completed Day 1 requirements.

## ✅ Repository & Documentation

- [ ] GitHub repository created with descriptive name
- [ ] Root `README.md` with:
  - [ ] Project overview (Vattenfall energy trading & grid management)
  - [ ] Business domains explained (market prices, weather, grid events, reference)
  - [ ] Unity Catalog structure (catalog, schemas, volumes)
  - [ ] Quick start instructions
- [ ] `.gitignore` file (ignores checkpoints, cache, credentials)
- [ ] Top-level folder structure created:
  - [ ] `config/`
  - [ ] `docs/`
  - [ ] `sample_data/`
  - [ ] `sql/`
  - [ ] `src/`
  - [ ] `notebooks/`
  - [ ] `tests/`
  - [ ] `.github/workflows/`

## ✅ Documentation (`docs/`)

- [ ] `00_how_to_use_this_repo.md` - Navigation guide
- [ ] `01_business_context.md` - Vattenfall business case
- [ ] `02_architecture_overview.md` - Technical architecture
- [ ] `03_repo_structure.md` - Folder explanations
- [ ] `04_day1_bronze_scope.md` - Day 1 focus
- [ ] `05_day1_deliverables.md` - This checklist

## ✅ Configuration (`config/`)

- [ ] `project_config.yml` created with:
  - [ ] Catalog name (e.g., `vattenfall_dev`)
  - [ ] Schemas: `raw`, `refined`, `analytics`
  - [ ] Volumes: `landing`, `checkpoints`
  - [ ] Bronze table names for each domain
  - [ ] Landing paths for each domain
  - [ ] Checkpoint paths for each domain

## ✅ Sample Data (`sample_data/`)

- [ ] Domain folders created:
  - [ ] `market_prices/`
  - [ ] `weather/`
  - [ ] `grid_events/`
  - [ ] `reference/`
- [ ] At least **3-5 sample CSV files per domain**
- [ ] Files have realistic column names and data

## ✅ SQL (`sql/`)

- [ ] `01_uc_setup.sql` - Creates catalog, schemas, volumes
- [ ] `02_day1_bronze_validation.sql` - Validates bronze tables

## ✅ Python Package (`src/`)

- [ ] Folder structure created:
  - [ ] `utils/`
  - [ ] `transforms/`
  - [ ] `udfs/`
  - [ ] `validation/`
- [ ] `__init__.py` files in each folder
- [ ] At least one utility function (e.g., config loading helper)

## ✅ Unity Catalog Setup (Databricks)

- [ ] Catalog created (e.g., `vattenfall_dev`)
- [ ] Schemas created: `raw`, `refined`, `analytics`
- [ ] Volumes created: `landing`, `checkpoints`
- [ ] Sample CSV files copied to landing volumes

## ✅ Bronze Ingestion Notebooks (`notebooks/02_bronze/`)

**Minimum Requirement:** At least **1** working Auto Loader notebook

**Recommended:** All 4 domains

- [ ] `01_market_prices_autoloader.py` - Market prices bronze ingestion
- [ ] `02_weather_autoloader.py` - Weather bronze ingestion
- [ ] `03_grid_events_autoloader.py` - Grid events bronze ingestion
- [ ] `04_reference_data_load.py` - Reference data loading

**Each notebook should:**
- [ ] Load config from `project_config.yml`
- [ ] Use Auto Loader (`.format("cloudFiles")`)
- [ ] Write to bronze Delta table in `vattenfall_dev.raw` schema
- [ ] Use checkpoint location for incremental processing
- [ ] Include `_rescued_data` column for schema evolution

## ✅ Setup Notebooks (`notebooks/01_setup/`)

- [ ] `01_project_overview.py` - Project documentation
- [ ] `02_uc_setup.py` - Unity Catalog creation (or use SQL file)
- [ ] `03_landing_and_checkpoint_setup.py` - Volume setup

## ✅ Bronze Validation

- [ ] `02_bronze/05_bronze_validation.py` notebook or SQL queries
- [ ] Verify bronze tables exist
- [ ] Check row counts for each bronze table
- [ ] Display sample rows from each table
- [ ] Verify schema (columns, types)

## ✅ Code Quality

- [ ] Notebooks have descriptive titles
- [ ] Code is commented where needed
- [ ] Config values are NOT hardcoded (loaded from `project_config.yml`)
- [ ] No exposed credentials or secrets
- [ ] Consistent naming conventions

## ✅ Functional Tests

Run these manual tests to verify Day 1 is complete:

### Test 1: Config Loading
```python
import yaml
with open("config/project_config.yml", "r") as f:
    config = yaml.safe_load(f)
print(config)  # Should show catalog, schemas, tables, paths
```

### Test 2: Unity Catalog Objects Exist
```sql
SHOW CATALOGS;  -- Should show vattenfall_dev
USE CATALOG vattenfall_dev;
SHOW SCHEMAS;   -- Should show raw, refined, analytics
SHOW VOLUMES IN raw;  -- Should show landing, checkpoints
```

### Test 3: Bronze Tables Exist
```sql
USE CATALOG vattenfall_dev;
USE SCHEMA raw;
SHOW TABLES;  -- Should show bronze_market_prices, bronze_weather, etc.
```

### Test 4: Data in Bronze Tables
```sql
SELECT COUNT(*) FROM vattenfall_dev.raw.bronze_market_prices;
SELECT * FROM vattenfall_dev.raw.bronze_market_prices LIMIT 10;
```

### Test 5: Auto Loader Checkpoint Works
- Run an Auto Loader notebook twice
- Second run should detect no new files (checkpoint working)

## 📋 Submission Checklist

Before submitting Day 1, verify:

- [ ] Repository is pushed to GitHub
- [ ] README clearly explains the project
- [ ] All documentation files are complete
- [ ] At least ONE Auto Loader bronze ingestion works
- [ ] Bronze validation queries confirm data exists
- [ ] No hardcoded values (config is used)
- [ ] Code is clean and readable

## 🎯 Stretch Goals (Optional)

If you finish early:

- [ ] Implement all 4 bronze ingestions (market prices, weather, grid events, reference)
- [ ] Add Python utility functions in `src/utils/`
- [ ] Create data quality validation checks
- [ ] Add unit tests in `tests/`
- [ ] Set up GitHub Actions workflow for repo checks
- [ ] Add example Markdown diagrams to documentation

## ❌ What NOT to Do on Day 1

Don't spend time on:
- Silver layer transformations (Day 2-3)
- Gold layer aggregations (Day 3-4)
- Dashboards (Day 4)
- Databricks Workflows (Day 5)
- Advanced Unity Catalog permissions (Day 5)

Focus on getting the foundation right. The rest of the week builds on Day 1.

---

## Day 1 Complete? ✅

If you've checked all the items above, you're ready to proceed to Day 2 (Silver layer).

**Day 2 Focus:** Data cleaning, validation, deduplication, and type casting in the silver layer.

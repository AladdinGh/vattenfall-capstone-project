# Repository Structure

## Top-Level Overview

```
vattenfall-week9-capstone-yourname/
├── README.md                   # Project overview and quick start
├── .gitignore                  # Ignore checkpoints, cache, credentials
├── config/                     # Configuration files
├── docs/                       # Project documentation
├── sample_data/                # Sample CSV files by domain
├── sql/                        # Reusable SQL scripts
├── src/                        # Python package (utils, transforms, UDFs)
├── notebooks/                  # Databricks notebooks by layer
├── tests/                      # Unit tests
└── .github/workflows/          # CI/CD automation
```

## Folder Details

### `config/`
Configuration files for catalog, schema, table names, paths, and business rules.

**Files:**
- `project_config.yml` - Single config with all settings

**Contents:**
- catalog and schema names
- table names (bronze, silver, gold)
- volume paths
- business rules (thresholds, validation limits)

### `docs/`
Project documentation (this folder).

**Files:**
- `00_how_to_use_this_repo.md` - Navigation guide
- `01_business_context.md` - Vattenfall business case
- `02_architecture_overview.md` - Technical design
- `03_repo_structure.md` - This file
- `04_day1_bronze_scope.md` - Day 1 focus and expectations
- `05_day1_deliverables.md` - Success criteria

### `sample_data/`
Sample CSV files organized by business domain.

**Structure:**
```
sample_data/
├── market_prices/      # Electricity wholesale prices
├── weather/            # Temperature, wind, cloud data
├── grid_events/        # Outages, maintenance events
└── reference/          # Region codes, facility types
```

### `sql/`
Reusable SQL scripts for setup and validation.

**Files:**
- `01_uc_setup.sql` - Create catalog, schemas, volumes
- `02_day1_bronze_validation.sql` - Validate bronze ingestion

### `src/`
Python package for reusable code (imported by notebooks).

**Structure:**
```
src/
├── utils/              # Helper functions (config loading, paths)
├── transforms/         # Data transformation logic
├── udfs/               # User-defined functions for Spark
└── validation/         # Data quality checks
```

Each folder has `__init__.py` to make it a Python package.

### `notebooks/`
Databricks notebooks organized by data layer.

**Structure:**
```
notebooks/
├── 01_setup/           # Unity Catalog and landing setup
├── 02_bronze/          # Raw data ingestion (Auto Loader)
├── 03_silver/          # Cleaned operational data
├── 04_business_logic/  # Domain-specific transformations
├── 05_gold/            # Business analytics aggregations
├── 06_governance/      # Permissions and compliance
└── 07_operations/      # Monitoring and troubleshooting
```

**Day 1 Focus:**
- `01_setup/` - UC setup, landing zone
- `02_bronze/` - Auto Loader ingestion

### `tests/`
Unit tests for Python functions in `src/`.

**Purpose:**
- Validate utility functions
- Test transformation logic
- Ensure data quality checks work correctly

### `.github/workflows/`
CI/CD automation (GitHub Actions).

**Purpose:**
- Run tests on pull requests
- Validate repo structure
- Check documentation completeness

## Navigation Tips

1. **Start here:** `README.md` → `docs/00_how_to_use_this_repo.md`
2. **Understand the business:** `docs/01_business_context.md`
3. **Review architecture:** `docs/02_architecture_overview.md`
4. **Configure:** `config/project_config.yml`
5. **Set up Unity Catalog:** `sql/01_uc_setup.sql`
6. **Run notebooks:** `notebooks/01_setup/` → `notebooks/02_bronze/`

# How to Use This Repository

## Quick Start

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd vattenfall-week9-capstone-yourname
   ```

2. **Review the documentation**
   - Read `01_business_context.md` to understand the Vattenfall business case
   - Review `02_architecture_overview.md` for the technical architecture
   - Check `03_repo_structure.md` to navigate the codebase

3. **Set up Unity Catalog**
   - Run `sql/01_uc_setup.sql` in Databricks SQL Editor
   - This creates the catalog, schemas, and volumes

4. **Configure the project**
   - Review `config/project_config.yml`
   - Update catalog/schema names if needed for your environment

5. **Run bronze ingestion**
   - Navigate to `notebooks/01_setup/` and run setup notebooks first
   - Then run notebooks in `notebooks/02_bronze/` to ingest data

## Repository Navigation

- **`config/`** - Configuration files (catalog, schema, table names)
- **`docs/`** - Project documentation (you are here)
- **`sample_data/`** - Sample CSV files organized by domain
- **`sql/`** - Reusable SQL scripts for setup and validation
- **`src/`** - Python utilities, transforms, UDFs, validation logic
- **`notebooks/`** - Databricks notebooks organized by layer (setup, bronze, silver, gold)
- **`tests/`** - Unit tests for utility functions

## Day 1 Focus

Day 1 focuses on **bronze layer ingestion** using Auto Loader:
- Market prices data
- Weather data
- Grid events data
- Reference data

See `04_day1_bronze_scope.md` and `05_day1_deliverables.md` for detailed expectations.

## Getting Help

- Check the documentation in `docs/` first
- Review example notebooks for patterns
- Refer to Databricks Auto Loader documentation
- Ask instructors if blocked

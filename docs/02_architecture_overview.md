# Architecture Overview

## Medallion Architecture

This project follows the **medallion architecture** pattern (Bronze → Silver → Gold):

```
┌─────────────────┐
│  Sample Data    │
│  (CSV files)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Landing Zone   │  ← Volume: vattenfall_dev.raw.landing
│  (Unity Catalog)│
└────────┬────────┘
         │
         ▼ Auto Loader (cloudFiles)
┌─────────────────┐
│  BRONZE Layer   │  ← Schema: vattenfall_dev.raw
│  (Raw ingestion)│     Tables: bronze_market_prices, bronze_weather, ...
└────────┬────────┘
         │
         ▼ Cleaning, validation, deduplication
┌─────────────────┐
│  SILVER Layer   │  ← Schema: vattenfall_dev.refined
│  (Clean data)   │     Tables: silver_market_prices, silver_weather, ...
└────────┬────────┘
         │
         ▼ Aggregations, joins, business logic
┌─────────────────┐
│  GOLD Layer     │  ← Schema: vattenfall_dev.analytics
│  (Analytics)    │     Tables: gold_daily_price_summary, gold_grid_kpis, ...
└─────────────────┘
```

## Unity Catalog Structure

**Catalog:** `vattenfall_dev`

**Schemas:**
- `raw` - Bronze tables and landing volumes
- `refined` - Silver tables (cleaned operational data)
- `analytics` - Gold tables (business aggregations)

**Volumes:**
- `landing` - Raw CSV files arrive here
- `checkpoints` - Auto Loader state for incremental processing

## Data Flow

### Ingestion Pattern (Auto Loader)
1. CSV files land in `/Volumes/vattenfall_dev/raw/landing/<domain>/`
2. Auto Loader detects new files automatically
3. Data is read incrementally and written to bronze Delta tables
4. Checkpoints track processed files to avoid duplicates

### Transformation Pattern
- Bronze → Silver: Type casting, validation, deduplication
- Silver → Gold: Aggregations, joins, business logic

## Technology Stack

- **Storage:** Delta Lake (ACID transactions, time travel)
- **Compute:** Databricks serverless
- **Orchestration:** Databricks Workflows (not implemented Day 1)
- **Governance:** Unity Catalog (catalog, schema, permissions)
- **Language:** Python (PySpark), SQL

## Day 1 Scope

- ✅ Unity Catalog setup (catalog, schemas, volumes)
- ✅ Landing zone for sample data
- ✅ Bronze ingestion with Auto Loader (at least one domain)
- ✅ Bronze validation queries

Silver and Gold layers are built in subsequent days.

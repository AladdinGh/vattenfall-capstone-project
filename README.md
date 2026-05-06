# Vattenfall Energy Data Lakehouse - Capstone Project

## Overview

This capstone project simulates a production-grade Databricks implementation for Vattenfall, a leading European energy company. The project demonstrates end-to-end data engineering practices for the energy sector, transforming raw operational data into actionable insights through a governed medallion architecture.

## Business Context

Energy companies like Vattenfall operate in a complex environment where multiple data streams must be integrated to:
- Optimize energy trading decisions based on real-time market prices
- Predict grid load and maintenance needs using weather patterns
- Monitor grid health and respond to incidents proactively
- Maintain compliance with regulatory reporting requirements

This project builds a scalable data lakehouse that unifies these data sources to enable operational excellence and data-driven decision making.

## Data Sources

### 1. Energy Market Price Data
Real-time and historical pricing information from energy markets including:
- Spot prices (day-ahead and intraday markets)
- Forward curves and futures contracts
- Regional price variations
- Supply and demand indicators

### 2. Weather Observations
Meteorological data critical for energy forecasting:
- Temperature, wind speed, and solar irradiance
- Historical weather patterns
- Weather station locations and measurements
- Correlation with energy demand and renewable generation

### 3. Grid Telemetry & Incident Events
Operational data from the electrical grid:
- Real-time sensor readings from substations and transmission lines
- Power flow measurements
- Incident logs and outage events
- Maintenance records and asset health indicators

### 4. Reference Data
Master data and dimensional information:
- Geographic location hierarchies
- Asset catalogs and specifications
- Customer segments
- Regulatory and compliance metadata

## Architecture

The project implements a **medallion architecture** with progressive data refinement:

### Bronze Layer (Raw Ingestion)
- Ingests raw files from all source systems
- Preserves original data format and structure
- Implements schema-on-read patterns
- Tracks data lineage and audit metadata

### Silver Layer (Cleaned & Conformed)
- Cleanses and validates data quality
- Standardizes formats, timestamps, and units
- Implements slowly changing dimensions (SCD)
- Joins and enriches datasets
- Handles deduplication and error records

### Gold Layer (Business-Level Aggregates)
- Creates business-ready aggregations and KPIs
- Implements dimensional models for analytics
- Pre-calculates metrics for reporting performance
- Enforces data governance and access policies

### Reporting Layer
- Powers dashboards and BI tools
- Supports ad-hoc analysis and data science use cases
- Provides APIs for downstream consumption
- Enables self-service analytics

## Project Objectives

By the end of this capstone, you will have built:

1. **Automated Data Pipelines** using Lakeflow Spark Declarative Pipelines (SDP)
2. **Unity Catalog Governance** with fine-grained access controls
3. **Data Quality Frameworks** with expectations and monitoring
4. **Performance-Optimized Tables** using Delta Lake features
5. **Production-Ready Dashboards** for operational and executive reporting
6. **End-to-End Data Lineage** from source to consumption

## Weekly Deliverables

Each week focuses on a specific aspect of the lakehouse:

- **Week 1**: Environment setup and bronze layer ingestion
- **Week 2**: Silver layer transformations and data quality
- **Week 3**: Gold layer business metrics and dimensional models
- **Week 4**: Dashboards, governance, and production deployment

## Technology Stack

- **Platform**: Databricks on AWS
- **Storage**: Delta Lake with Unity Catalog
- **Compute**: Serverless and provisioned clusters
- **Orchestration**: Databricks Jobs
- **Languages**: Python, SQL
- **Visualization**: Databricks Dashboards (Lakeview)

## Getting Started

1. Navigate to the `notebooks/` directory for pipeline code
2. Review `data/` folder for sample input files
3. Check `dashboards/` for visualization definitions
4. Follow the weekly guides in `docs/` for step-by-step instructions
5. Run inspection queries in `sql/` to validate data quality

## Project Structure

```
vattenfall-capstone-project/
├── README.md
├── notebooks/
│   ├── bronze/
│   ├── silver/
│   │   ├── 03_silver_grid_events.py
│   │   ├── 04_silver_asset_reference.py
│   │   └── 05_silver_regional_operations_integrated.py
│   └── gold/
├── pipelines/
├── src/
│   └── transforms/
│       ├── grid_event_transforms.py
│       ├── asset_reference_transforms.py
│       ├── integration_transforms.py
│       ├── market_price_transforms.py
│       └── weather_transforms.py
├── data/
│   ├── energy_prices/
│   ├── weather/
│   ├── grid_telemetry/
│   └── reference/
├── sql/
│   └── day3_silver_inspection_examples.sql
├── dashboards/
└── docs/
    └── 04_silver_layer_documentation.md
```

## Silver Layer Overview

**📊 Tables Created (3):**

1. **silver_grid_events** (`vattenfall_dev.refined.silver_grid_events`)
   * 165 records, 44 columns
   * Cleaned event data with temporal dimensions
   * Impact scores and quality flags
   * Notebook: `03_silver_grid_events.py`

2. **silver_asset_reference** (`vattenfall_dev.refined.silver_asset_reference`)
   * 50 records, 28 columns
   * Integrated substations + regions
   * Business keys: asset_key, region_key, country_key
   * Categorizations: voltage_level, capacity, asset_age
   * Notebook: `04_silver_asset_reference.py`

3. **silver_regional_operations_base** (`vattenfall_dev.refined.silver_regional_operations_base`)
   * 12 records, 72 columns
   * Pre-joined events + assets (integration layer)
   * 6 business enrichments calculated
   * Notebook: `05_silver_regional_operations_integrated.py`

**🔧 Transformation Modules (5):**
   * `grid_event_transforms.py` - 9 functions
   * `asset_reference_transforms.py` - 6 functions
   * `integration_transforms.py` - 6 functions
   * `market_price_transforms.py` - prepared for future
   * `weather_transforms.py` - prepared for future

**📝 SQL Inspection Queries:**
   * Run queries from `sql/day3_silver_inspection_examples.sql`
   * 10 sections covering validation, analysis, and insights
   * Includes data quality checks and business metric verification


## 🔍 Critical Findings

**High-Priority Actions:**
   * 🔴 SUB105 (Finland): 26 yrs, 800 MVA, 4,766 customers - REPLACE
   * 🔴 SUB136 (Finland): 28 yrs, 800 MVA, 2,202 customers - REPLACE

**Regional Performance:**
   * 🟠 Turku: 1,200 population impact rate - CAPACITY EXPANSION NEEDED
   * 🟠 Copenhagen: 228 min avg duration - IMPROVE RESTORATION SPEED
   * 🟠 Finland: 58% of events - INFRASTRUCTURE INVESTMENT REQUIRED

**Data Quality Issue:**
   * ⚠️  153 of 165 events lack asset reference data
   * →  Production requires complete asset reference dataset



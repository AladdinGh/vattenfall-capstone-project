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
│   └── 04_silver_inspection_examples.sql
├── dashboards/
└── docs/
    └── 04_silver_layer_documentation.md
```

─────────────────────────────────────────────────────────────────────────────

## Bronze Layer Overview

**📦 Raw Data Ingestion Foundation**

**Tables Created (5):**
* `bronze_grid_events` - Grid incident events and outages (165 records)
* `bronze_substations` - Substation asset catalog (25 records)
* `bronze_regions` - Geographic reference data (25 records)
* `bronze_market_prices` - Energy market pricing data
* `bronze_weather_obs` - Weather station observations

**Key Features:**
* ✅ Schema-on-read with Auto Loader
* ✅ Original data preservation (including `_rescued_data`)
* ✅ Audit columns: `source_system`, `last_updated_ts`
* ✅ Delta Lake format for ACID transactions
* ✅ Incremental ingestion ready

**Data Lineage:**
* Source: CSV files in `/data/` directories
* Destination: `vattenfall_dev.raw` schema
* Ingestion pattern: Batch loading with full history

─────────────────────────────────────────────────────────────────────────────

## Silver Layer Overview

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


─────────────────────────────────────────────────────────────────────────────

## Gold Layer Overview

**📊 Business Analytics Layer**  
**Analysis Period:** January 1-15, 2024 | **Regions:** Denmark, Finland, Norway, Sweden

---

### 1️⃣ Daily Market Summary

**Question:** *"What is the daily market situation by region?"*

**Gold Table:** `gold_daily_market_summary` (60 records)  
**Grain:** report_day × region

#### Regional Market Profiles

| Region | Avg Price | Market Status | Key Characteristics |
|--------|-----------|---------------|---------------------|
| 🔴 **Finland** | **48.62 EUR/MWh** | Premium Market | 9% premium over DK/NO<br>6 of top 10 most expensive days<br>Peak: Jan 9 at 56.66 EUR/MWh |
| 🟡 **Sweden** | **44.98 EUR/MWh** | Moderate | Mid-range pricing<br>Stress episodes on Jan 10 & 15<br>4 high-price hours each day |
| 🟢 **Denmark** | **44.40 EUR/MWh** | Low Cost | Most affordable<br>No extreme outliers<br>Stable pricing |
| 🟢 **Norway** | **44.40 EUR/MWh** | Low Cost | Tied with Denmark<br>One spike: Jan 7 (55.27 EUR/MWh)<br>Generally stable |

#### Key Insights

* **Structural Capacity Constraints:** Finland's persistent 9% premium suggests supply-demand imbalances requiring infrastructure investment
* **Price Volatility Risk:** Jan 12 extreme swings (14.55 EUR/MWh stddev) despite moderate average indicate market instability
* **Nordic Integration:** Sweden bridges high-cost Finland with low-cost DK/NO, suggesting transmission bottlenecks

---

### 2️⃣ Weather Impact Summary

**Question:** *"What weather conditions may affect operations by day and region?"*

**Gold Table:** `gold_weather_impact_summary` (60 records)  
**Grain:** report_day × region

#### Weather Risk Overview

**🚨 CRITICAL FINDING:** Every single day (15/15) posed operational risk

| Risk Category | Percentage | Days | Characteristics |
|---------------|------------|------|-----------------|
| 🔴 **EXTREME** | 43% | 26 days | Risk score 8-9/9<br>High winds + heavy precipitation |
| 🟠 **HIGH** | 57% | 34 days | Risk score 6-7/9<br>Sustained high winds |

#### Primary Risk Factors

* **🌬️ Wind Speed:** 8-13 m/s sustained (PRIMARY risk factor)
* **❄️ Temperature:** -4.3°C to -2.4°C (equipment stress, ice accumulation)
* **🌧️ Precipitation:** 0-68 mm daily (ice loading on transmission lines)
* **☁️ Cloud Cover:** 43-63% average (reduced solar generation)

#### Regional Weather Patterns

**All regions experienced similar severe conditions, but:**
* **Finland** requires priority attention due to historical incident concentration (58% of events)
* **Extreme weather days:** Jan 1, 3, 6, 13, 15 (risk score 8)
* **Correlation finding:** Low wind speeds create +5.7% price premium (reduced renewable generation)

---

### 3️⃣ Grid Incident Summary

**Question:** *"What operational incidents happened, where, and how severe were they?"*

**Gold Table:** `gold_grid_incident_summary` (97 records)  
**Grain:** event_day × region × severity_band

#### Regional Incident Analysis

| Region | Incidents | Customers Affected | Avg Duration | Status |
|--------|-----------|-------------------|--------------|--------|
| 🔴 **Sweden** | **60** (36%) | **169,145** (39%) | **148 min** | CRITICAL<br>Highest volume + longest durations |
| 🟡 **Finland** | **47** (28%) | **115,327** (27%) | **116 min** | HIGH<br>Balanced frequency-duration |
| 🟢 **Norway** | **30** (18%) | **88,381** (21%) | **91 min** | MODERATE<br>Shortest durations<br>Worst single day: Jan 4 (22,067 customers) |
| 🟢 **Denmark** | **28** (17%) | **57,809** (13%) | **112 min** | STABLE<br>Best operational performance |

**Total Impact:** 165 incidents | 430,662 customers affected | 19,685 minutes total downtime

#### Severity Impact Analysis

| Severity Band | Incidents | Avg Duration | Customer Impact | Key Finding |
|---------------|-----------|--------------|-----------------|-------------|
| **Critical Priority** | 49 (30%) | 132 min | **84% of total impact** | High-severity focus critical |
| **High Priority** | 14 (8%) | **226 min** | 12% of impact | 2× restoration time = complex technical challenges |
| **Medium Priority** | 49 (30%) | 69 min | 3% of impact | Routine restoration |
| **Low Priority** | 53 (32%) | 76 min | 1% of impact | Minimal impact |

#### Strategic Implications

✅ **Sweden Requires Immediate Intervention:** 36% of all incidents + longest restoration times + dominates worst-day rankings  
✅ **Critical Priority = 84% Impact:** Focus prevention on high-severity scenarios, not equal distribution  
✅ **High Priority Complexity:** 2× restoration time warrants root cause analysis and specialized protocols  
✅ **Norway Jan 4 Event:** 22,067 customers (25% of Norway's 2-week impact) - investigate weather/equipment/maintenance factors  
✅ **Denmark Best Practice:** Lowest incident count - benchmark processes for other regions  

---

### 4️⃣ Regional Condition Daily

**Question:** *"What is the overall operational health of each region by day?"*

**Gold Table:** `gold_regional_condition_daily` (60 records)  
**Grain:** report_day × region  
**Health Score:** 0-100 composite metric (market + weather + incidents)

#### Regional Health Rankings

| Tier | Region | Health Score | Alert Days | Status | Key Metrics |
|------|--------|--------------|------------|--------|-------------|
| **Tier 1** | 🟡 **Denmark** | **49.2** | 10/15 (67%) | Moderately Stressed | 28 incidents<br>57,809 customers<br>Balanced stress profile |
| **Tier 1** | 🟡 **Norway** | **49.2** | 6/15 (40%) | Moderately Stressed | 30 incidents<br>88,381 customers<br>One catastrophic day (Jan 4) |
| **Tier 2** | 🔴 **Finland** | **39.3** | 14/15 (93%) | **Highly Stressed** | 47 incidents<br>115,327 customers<br>Premium pricing (48.62 EUR/MWh) |
| **Tier 2** | 🔴 **Sweden** | **34.0** | 14/15 (93%) | **CRITICAL** | 60 incidents (36% of Nordic)<br>169,145 customers<br>**Triple threat pattern** |

#### Operational Condition Distribution

| Condition | Days | Percentage | Implication |
|-----------|------|------------|-------------|
| CRITICAL | 12 | 20% | Immediate intervention required |
| POOR | 19 | 32% | Elevated operational risk |
| FAIR | 29 | 48% | Manageable but monitored |
| GOOD | 0 | 0% | **No "good" operational days** |
| EXCELLENT | 0 | 0% | **No "excellent" operational days** |

#### Critical Findings

🚨 **Zero EXCELLENT/GOOD Days:** All 60 region-days showed elevated risk  
🚨 **73% Required Alerts:** 44 of 60 region-days triggered operational alerts  
🚨 **Sweden Triple Threat:** High incidents + extreme weather + longest restoration = strategic crisis  
🚨 **Finland Persistent Stress:** 93% alert rate + premium pricing = infrastructure limits reached  

#### Recommended Actions

| Priority | Region | Action | Expected Outcome |
|----------|--------|--------|------------------|
| **P0** | Sweden | Strategic grid resilience investment + preventive maintenance program | Reduce incident volume 20-30% |
| **P0** | Finland | Capacity expansion + infrastructure modernization | Lower alert rate to <70% |
| **P1** | Norway | Root cause analysis of Jan 4 event + emergency response protocols | Prevent catastrophic single-day events |
| **P1** | Denmark | Benchmark best practices + knowledge transfer program | Share operational excellence across Nordic grid |

---

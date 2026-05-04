# Business Context — Vattenfall Energy Trading & Grid Management

## About Vattenfall

Vattenfall is one of Europe's largest energy companies, producing and distributing electricity and heat across several European markets. The company operates:
- Power generation (hydro, wind, nuclear, gas)
- Energy trading and wholesale markets
- Grid operations and distribution
- Retail energy sales

## Project Overview

This capstone project builds a **data platform for energy trading and grid management operations**, integrating multiple data sources to support business decisions.

## Business Domains

### 1. Market Prices (Wholesale Energy Trading)
**Purpose:** Track electricity wholesale prices to optimize trading strategies.

**Data:**
- Hourly spot prices by market zone
- Day-ahead and intraday markets
- Price volatility indicators

**Business Questions:**
- When should we buy/sell energy contracts?
- Which markets offer the best arbitrage opportunities?
- How do prices correlate with demand and weather?

### 2. Weather Data (Generation & Demand Forecasting)
**Purpose:** Weather directly impacts both energy generation (wind, solar) and demand (heating, cooling).

**Data:**
- Temperature by region
- Wind speed (critical for wind power generation)
- Cloud cover (solar generation)

**Business Questions:**
- How much wind power can we expect tomorrow?
- Will extreme temperatures drive demand spikes?
- Should we adjust our trading positions based on forecasts?

### 3. Grid Events (Operational Reliability)
**Purpose:** Track grid incidents, maintenance, and outages to ensure reliability and compliance.

**Data:**
- Unplanned outages
- Scheduled maintenance
- Grid load by region

**Business Questions:**
- Which substations have the most incidents?
- How do outages impact customer experience?
- Where should we invest in grid upgrades?

### 4. Reference Data (Master Data)
**Purpose:** Provide lookup tables for regions, facilities, equipment types, and organizational units.

**Data:**
- Region codes and names
- Facility types (wind farm, substation, etc.)
- Equipment specifications

## Business Value

This platform enables:
- **Traders** to make data-driven buy/sell decisions
- **Grid operators** to monitor and respond to incidents
- **Analysts** to identify patterns and optimize operations
- **Executives** to track KPIs and compliance metrics

## Week 9 Capstone Scope

This week focuses on building the **foundational data ingestion and medallion architecture**:
- **Bronze:** Raw data ingestion with Auto Loader
- **Silver:** Cleaned and validated operational data
- **Gold:** Business-ready aggregations for reporting

Future weeks could extend to ML forecasting, real-time dashboards, and advanced analytics.

# Vattenfall Grid Operations Analytics
## Capstone Project Presentation Outline

---

## 1. BUSINESS CONTEXT 

### 1.1 The Challenge
- **Industry**: Nordic Electricity Distribution (Denmark, Finland, Sweden, Norway)
- **Problem Statement**: Grid infrastructure aging + extreme weather = increasing outages
- **Business Impact**:
  - 430,662 customers affected (Jan 2024)
  - €227,000+ in lost revenue opportunity
  - Average 159 min restoration time per incident
  - Reactive rather than predictive maintenance

### 1.2 Stakeholder Needs
- **Operations Teams**: Need to identify high-risk substations before failure
- **Executive Leadership**: Require country-level performance ratings and ROI on infrastructure investments
- **Maintenance Teams**: Want prioritized asset replacement schedules
- **Finance**: Need to quantify economic impact and optimize maintenance budgets

### 1.3 Success Metrics
- Build unified analytics platform spanning 4 countries
- Identify top 10 worst-performing substations
- Quantify weather vs. aging infrastructure impact
- Deliver daily country health scores
- Calculate financial impact of poor timing

---

## 2. ARCHITECTURE 

### 2.1 Data Lakehouse Design
```
Bronze Layer (Raw Ingestion)
├── Landing: Raw JSON/CSV from 3 sources
├── Validation: Schema enforcement, data quality checks
└── Storage: Delta tables with full history

Silver Layer (Refined/Enriched)
├── Operations: Cleaned incident data with temporal features
├── Weather: Hourly observations aggregated to daily
├── Market Prices: Normalized regional pricing
└── Asset Reference: Substation metadata with age categories

Gold Layer (Business-Ready Analytics)
├── Regional Condition Daily: Country health scores
├── Substation Risk Scoring: Performance rankings
└── Weather Attribution: Causation analysis
```

### 2.2 Technology Stack
- **Platform**: Databricks on AWS (Serverless)
- **Storage**: Delta Lake (ACID transactions, time travel)
- **Processing**: PySpark + Spark SQL
- **Orchestration**: Databricks Workflows (scheduled pipelines)
- **Visualization**: Matplotlib with calmer color palette
- **Governance**: Unity Catalog (3-level namespace)

### 2.3 Data Flow
1. **Ingestion**: 3 source systems → Bronze tables (raw)
2. **Transformation**: Bronze → Silver (cleaned, enriched)
3. **Aggregation**: Silver → Gold (business metrics)
4. **Insights**: Gold → Notebooks (analysis + visualization)

### 2.4 Key Design Decisions
- **Why Delta Lake?**: ACID guarantees, schema evolution, time travel for auditing
- **Why Medallion Architecture?**: Separation of concerns, incremental processing, data lineage
- **Why Serverless?**: Cost optimization, auto-scaling, zero cluster management
- **Why Unity Catalog?**: Centralized governance, fine-grained access control

---

## 3. OUTPUTS 

### 3.1 Strategic Insights Delivered

#### Q1: Copenhagen Risk Score Breakdown
- **Finding**: Copenhagen scores 43.0 (highest risk)
- **Root Cause**: 53% due to restoration duration (228 min vs 159 avg)
- **Action**: Improve emergency response protocols

#### Q2: Worst Substation Performers
- **Top 3**: SUB128 (6.5x worse), SUB149, SUB112
- **Paradox**: SUB121 (10 yrs old) ranks #2 worst despite being "modern"
- **Action**: Prioritize SUB128 replacement, investigate SUB121 anomaly

#### Q3: Weather vs. Aging Causation
- **67% involve adverse weather** (cold, wind, precipitation)
- **50% involve aging infrastructure** (15+ years)
- **33% show compounding risk** (weather + aging)
- **Action**: Target weatherization upgrades for aging assets

#### Q5: Country Operational Ratings
- **Denmark**: 49.2 (FAIR) - Best performer
- **Norway**: 49.2 (FAIR)
- **Finland**: 39.3 (POOR)
- **Sweden**: 34.0 (POOR) - Needs urgent attention
- **Action**: Investigate Sweden's 60 incidents (highest)

#### Q6: Temporal Patterns
- **Peak incident hour**: Varies by region
- **Weekend incidents**: 25% of total, take 1.2x longer to resolve
- **Action**: Increase weekend staffing levels

#### Q10: Market Price vs Grid Stress
- **€227,000 total lost revenue** in January 2024
- **Incidents occur when prices 15% HIGHER** than average
- **Double penalty**: Failures during expensive electricity periods
- **Action**: Enhanced monitoring during price spikes

### 3.2 Deliverables
- ✅ **7 Gold Tables**: Ready for BI tools and dashboards
- ✅ **6 Analytical Notebooks**: Reproducible insights
- ✅ **Calmer Visualizations**: Professional color palette
- ✅ **Daily Health Scores**: Automated country ratings
- ✅ **Risk Scoring System**: Substation prioritization

---

## 4. VALIDATION

### 4.1 Data Quality Checks
- **Row Count Validation**: 12 operational events, 826 market prices, 1,140 weather observations
- **Schema Validation**: All tables match expected structure
- **Referential Integrity**: All substations exist in asset reference
- **Temporal Consistency**: No future timestamps, no missing dates
- **Business Rules**: Durations > 0, customers affected > 0

### 4.2 Analytical Validation
- **Sanity Checks**:
  - Total customers affected (430,662) < total grid coverage
  - Average restoration time (159 min) aligns with industry standards
  - Weather correlation (67%) matches domain expectations
- **Cross-Validation**:
  - Country totals = sum of regional values
  - Lost revenue calculation verified against market prices
  - Risk scores mathematically consistent

### 4.3 Edge Cases Handled
- **Missing Weather Data**: Flagged incidents without weather observations
- **Price Gaps**: Forward-filled missing hourly prices
- **Region Mapping**: Consistent Copenhagen/Helsinki/Turku → country codes

---

## 5. OPTIMIZATION 

### 5.1 Performance Improvements
- **Query Execution**:
  - Photon acceleration enabled (fully supported)
  - All cells execute in <10 seconds
  - No unnecessary shuffles or broadcasts
- **Data Layout**:
  - Delta tables optimized with `OPTIMIZE`
  - Partitioned by date for time-range queries
  - Z-ordering on high-cardinality columns (substation_id)

### 5.2 Code Quality
- **Reusability**: Dataframes loaded once, reused across analyses
- **Modularity**: Clear separation between data prep and visualization
- **Documentation**: Every cell titled and explained
- **Color Standardization**: Calmer palette (#B5876E, #C19A7F, #C9B88A, #7BA7BC)

### 5.3 Cost Optimization
- **Serverless Compute**: Pay only for execution time
- **Delta Caching**: Reduce redundant scans
- **Selective Columns**: Only read needed fields in queries

---

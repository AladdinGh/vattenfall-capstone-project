-- ============================================================================
-- Vattenfall Capstone - Day 3 Silver Layer Inspection
-- ============================================================================
-- Purpose: Core validation queries for silver_regional_operations_base
-- ============================================================================

-- Record count
SELECT COUNT(*) AS total_records 
FROM vattenfall_dev.refined.silver_regional_operations_base;

-- Sample records with key columns
SELECT 
    event_id,
    timestamp,
    event_type,
    severity,
    substation_id,
    region_name,
    country_name,
    duration_minutes,
    affected_customers,
    total_impact_score,
    data_quality_score
FROM vattenfall_dev.refined.silver_regional_operations_base
LIMIT 10;

-- Business enrichments sample
SELECT 
    event_id,
    substation_id,
    region_name,
    severity,
    asset_age_years,
    asset_age_category,
    capacity_mva,
    impact_intensity,
    duration_severity_score,
    high_risk_event,
    population_impact_rate,
    high_capacity_asset
FROM vattenfall_dev.refined.silver_regional_operations_base
LIMIT 10;

-- Data quality score distribution
SELECT 
    CASE 
        WHEN data_quality_score >= 9 THEN 'High (>=0.9)'
        WHEN data_quality_score >= 7 THEN 'Medium (0.7-0.9)'
        ELSE 'Low (<0.7)'
    END AS quality_tier,
    COUNT(*) AS event_count,
    ROUND(AVG(data_quality_score), 1) AS avg_score
FROM vattenfall_dev.refined.silver_regional_operations_base
GROUP BY 1
ORDER BY avg_score DESC;

-- High-risk events
SELECT 
    event_id,
    substation_id,
    region_name,
    country_name,
    severity,
    asset_age_years,
    asset_age_category,
    affected_customers,
    duration_minutes,
    impact_intensity,
    duration_severity_score,
    population_impact_rate
FROM vattenfall_dev.refined.silver_regional_operations_base
WHERE high_risk_event = TRUE
ORDER BY duration_severity_score DESC;

-- Regional performance by country
SELECT 
    country_name,
    COUNT(*) AS event_count,
    COUNT(DISTINCT substation_id) AS affected_substations,
    SUM(affected_customers) AS total_customers_affected,
    ROUND(AVG(duration_minutes), 1) AS avg_duration_min,
    ROUND(AVG(population_impact_rate), 1) AS avg_population_impact_rate
FROM vattenfall_dev.refined.silver_regional_operations_base
GROUP BY country_name
ORDER BY event_count DESC;

-- Aging vs new assets comparison
SELECT 
    CASE 
        WHEN asset_age_category IN ('aging', 'mature') THEN 'Aging/Mature'
        ELSE 'New/Moderate'
    END AS asset_group,
    COUNT(*) AS event_count,
    ROUND(AVG(duration_minutes), 1) AS avg_duration_min,
    ROUND(AVG(affected_customers), 0) AS avg_affected_customers,
    COUNT(CASE WHEN severity = 'critical' THEN 1 END) AS critical_events
FROM vattenfall_dev.refined.silver_regional_operations_base
WHERE asset_age_category IS NOT NULL
GROUP BY 1;

-- Severity distribution
SELECT 
    severity,
    COUNT(*) AS event_count,
    ROUND(AVG(duration_minutes), 1) AS avg_duration_min,
    ROUND(AVG(affected_customers), 0) AS avg_affected_customers,
    ROUND(AVG(impact_intensity), 2) AS avg_impact_intensity
FROM vattenfall_dev.refined.silver_regional_operations_base
GROUP BY severity
ORDER BY 
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'major' THEN 2
        WHEN 'moderate' THEN 3
        WHEN 'minor' THEN 4
    END;

-- Top impact events
SELECT 
    event_id,
    substation_id,
    region_name,
    event_type,
    severity,
    affected_customers,
    capacity_mva,
    ROUND(impact_intensity, 2) AS impact_intensity,
    high_risk_event
FROM vattenfall_dev.refined.silver_regional_operations_base
ORDER BY impact_intensity DESC
LIMIT 10;

-- ============================================================================
-- BUSINESS INTELLIGENCE QUERIES
-- ============================================================================

-- Question 1: Which regions show elevated operational incidents?
SELECT
  region_name,
  country_name,
  COUNT(*) AS total_incidents,
  COUNT(DISTINCT substation_id) AS affected_substations,
  SUM(affected_customers) AS total_customers_impacted,
  ROUND(AVG(duration_minutes), 1) AS avg_duration_min,
  ROUND(AVG(population_impact_rate), 1) AS avg_population_impact,
  SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_incidents,
  SUM(CASE WHEN high_risk_event = TRUE THEN 1 ELSE 0 END) AS high_risk_events,
  ROUND(AVG(impact_intensity), 2) AS avg_impact_intensity
FROM vattenfall_dev.refined.silver_regional_operations_base
GROUP BY region_name, country_name
ORDER BY total_incidents DESC, avg_population_impact DESC;

-- Question 2: Which days show high market price pressure?
SELECT
  DATE(timestamp) AS price_date,
  region,
  COUNT(*) AS price_observations,
  ROUND(AVG(price_eur_mwh), 2) AS avg_price_eur_mwh,
  ROUND(MIN(price_eur_mwh), 2) AS min_price_eur_mwh,
  ROUND(MAX(price_eur_mwh), 2) AS max_price_eur_mwh,
  ROUND(STDDEV(price_eur_mwh), 2) AS price_volatility,
  SUM(CASE WHEN price_eur_mwh > 70 THEN 1 ELSE 0 END) AS high_price_hours
FROM vattenfall_dev.raw.bronze_market_prices
GROUP BY DATE(timestamp), region
ORDER BY avg_price_eur_mwh DESC, price_volatility DESC
LIMIT 20;

-- Question 3: How do weather conditions relate to grid events?
SELECT
  DATE(e.timestamp) AS event_date,
  e.region_name,
  COUNT(DISTINCT e.event_id) AS total_events,
  SUM(e.affected_customers) AS total_customers_affected,
  ROUND(AVG(w.temperature_celsius), 1) AS avg_temp_c,
  ROUND(AVG(w.wind_speed_ms), 1) AS avg_wind_speed_ms,
  ROUND(AVG(w.precipitation_mm), 2) AS avg_precipitation_mm,
  ROUND(AVG(w.cloud_cover_percent), 1) AS avg_cloud_cover_pct,
  SUM(CASE WHEN e.severity = 'critical' THEN 1 ELSE 0 END) AS critical_events
FROM vattenfall_dev.refined.silver_regional_operations_base e
LEFT JOIN vattenfall_dev.raw.bronze_weather w
  ON DATE(e.timestamp) = DATE(w.timestamp)
  AND LOWER(e.region_name) = LOWER(w.region)
GROUP BY DATE(e.timestamp), e.region_name
ORDER BY total_events DESC, critical_events DESC
LIMIT 20;

-- Question 4: Which regions need operational attention?
-- Regions ranked by composite risk score
SELECT
  region_name,
  country_name,
  COUNT(*) AS total_incidents,
  SUM(affected_customers) AS total_customers_impacted,
  ROUND(AVG(duration_minutes), 1) AS avg_restoration_time,
  ROUND(AVG(population_impact_rate), 1) AS avg_pop_impact_rate,
  SUM(CASE WHEN high_risk_event = TRUE THEN 1 ELSE 0 END) AS high_risk_events,
  COUNT(DISTINCT CASE WHEN asset_age_category IN ('aging', 'mature') THEN substation_id END) AS aging_substations,
  -- Composite risk score: incidents + pop_impact + high_risk_events + aging assets
  ROUND(
    COUNT(*) * 1.0 +
    AVG(population_impact_rate) / 100.0 +
    SUM(CASE WHEN high_risk_event = TRUE THEN 1 ELSE 0 END) * 2.0 +
    COUNT(DISTINCT CASE WHEN asset_age_category IN ('aging', 'mature') THEN substation_id END) * 1.5,
    2
  ) AS composite_risk_score
FROM vattenfall_dev.refined.silver_regional_operations_base
GROUP BY region_name, country_name
ORDER BY composite_risk_score DESC;

-- Question 5: Summary tables for dashboards
-- 5a. Daily Operational Summary (KPI Dashboard)
SELECT
  DATE(timestamp) AS operational_date,
  country_name,
  COUNT(DISTINCT event_id) AS daily_incidents,
  COUNT(DISTINCT substation_id) AS affected_assets,
  SUM(affected_customers) AS total_customers_affected,
  ROUND(AVG(duration_minutes), 1) AS avg_restoration_min,
  SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_count,
  SUM(CASE WHEN severity = 'major' THEN 1 ELSE 0 END) AS major_count,
  SUM(CASE WHEN severity = 'moderate' THEN 1 ELSE 0 END) AS moderate_count,
  SUM(CASE WHEN high_risk_event = TRUE THEN 1 ELSE 0 END) AS high_risk_count,
  ROUND(AVG(impact_intensity), 2) AS avg_impact_intensity
FROM vattenfall_dev.refined.silver_regional_operations_base
GROUP BY DATE(timestamp), country_name
ORDER BY operational_date DESC, country_name;

-- 5b. Asset Reliability Summary (Maintenance Dashboard)
SELECT
  substation_id,
  region_name,
  country_name,
  capacity_mva,
  voltage_level,
  asset_age_years,
  asset_age_category,
  COUNT(*) AS incident_count,
  SUM(affected_customers) AS total_customers_impacted,
  ROUND(AVG(duration_minutes), 1) AS avg_outage_duration,
  ROUND(AVG(impact_intensity), 2) AS avg_impact_intensity,
  SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_incidents,
  MAX(CASE WHEN high_risk_event = TRUE THEN 1 ELSE 0 END) AS has_high_risk_event,
  -- Reliability score (inverse of incidents and impact)
  ROUND(100.0 / (1.0 + COUNT(*) + AVG(impact_intensity)), 2) AS reliability_score
FROM vattenfall_dev.refined.silver_regional_operations_base
GROUP BY 
  substation_id, region_name, country_name, capacity_mva, 
  voltage_level, asset_age_years, asset_age_category
ORDER BY reliability_score ASC, incident_count DESC;

-- 5c. Regional Performance Scorecard (Executive Dashboard)
SELECT
  region_name,
  country_name,
  COUNT(*) AS total_incidents,
  SUM(affected_customers) AS total_customers_affected,
  ROUND(AVG(duration_minutes), 1) AS avg_duration_min,
  ROUND(AVG(population_impact_rate), 1) AS avg_pop_impact_rate,
  COUNT(DISTINCT substation_id) AS total_substations,
  SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_incidents,
  SUM(CASE WHEN high_risk_event = TRUE THEN 1 ELSE 0 END) AS high_risk_events,
  -- Performance grade based on incident frequency and impact
  CASE
    WHEN COUNT(*) >= 5 AND AVG(population_impact_rate) > 500 THEN 'D - Critical Attention'
    WHEN COUNT(*) >= 3 AND AVG(population_impact_rate) > 300 THEN 'C - Needs Improvement'
    WHEN COUNT(*) >= 2 OR AVG(population_impact_rate) > 200 THEN 'B - Monitor Closely'
    ELSE 'A - Good Performance'
  END AS performance_grade
FROM vattenfall_dev.refined.silver_regional_operations_base
GROUP BY region_name, country_name
ORDER BY 
  CASE
    WHEN COUNT(*) >= 5 AND AVG(population_impact_rate) > 500 THEN 1
    WHEN COUNT(*) >= 3 AND AVG(population_impact_rate) > 300 THEN 2
    WHEN COUNT(*) >= 2 OR AVG(population_impact_rate) > 200 THEN 3
    ELSE 4
  END,
  total_incidents DESC;

-- ============================================================================
-- END OF INSPECTION QUERIES
-- ============================================================================

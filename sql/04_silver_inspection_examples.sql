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
-- END OF INSPECTION QUERIES
-- ============================================================================

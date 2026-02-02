/*
============================================
Gold Layer: Ghana-like Seasonal Fare Variation
============================================

Purpose:
  Analyze fare patterns using the derived Ghana-like seasonality (seasonality_gh).
  This maps Bangladeshi flight data to Ghanaian seasonal patterns.

Seasonality Categories:
  - Regular: Normal periods
  - Christmas_NewYear: Dec 15 – Jan 10
  - Easter: Mar 15 – Apr 30
  - Independence_Day: Mar 1 – Mar 15
  - Farmers_Day: Dec 1 – Dec 10
  - BackToSchool: Jan 11 – Feb 15; Aug 15 – Sep 30
  - Homowo_Festival: Aug 1 – Aug 31
  - ChaleWote_Week: Aug 10 – Aug 20
  - Eid_like: From original Eid seasonality
  - Hajj_like: From original Hajj seasonality

Source: silver.stg_flight_prices
*/

{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'kpi', 'ghana']
) }}

WITH seasonal_stats AS (
    SELECT
        seasonality_gh,
        COUNT(*) AS booking_count,
        ROUND(AVG(base_fare_bdt)::numeric, 2) AS avg_base_fare_bdt,
        ROUND(AVG(tax_surcharge_bdt)::numeric, 2) AS avg_tax_surcharge_bdt,
        ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_total_fare_bdt,
        ROUND(MIN(total_fare_bdt)::numeric, 2) AS min_total_fare_bdt,
        ROUND(MAX(total_fare_bdt)::numeric, 2) AS max_total_fare_bdt,
        ROUND(STDDEV(total_fare_bdt)::numeric, 2) AS stddev_total_fare_bdt
    FROM {{ ref('stg_flight_prices') }}
    WHERE seasonality_gh IS NOT NULL AND seasonality_gh != ''
    GROUP BY seasonality_gh
),

overall_avg AS (
    SELECT ROUND(AVG(total_fare_bdt)::numeric, 2) AS overall_avg_fare
    FROM {{ ref('stg_flight_prices') }}
)

SELECT
    s.seasonality_gh,
    s.booking_count,
    s.avg_base_fare_bdt,
    s.avg_tax_surcharge_bdt,
    s.avg_total_fare_bdt,
    s.min_total_fare_bdt,
    s.max_total_fare_bdt,
    s.stddev_total_fare_bdt,
    o.overall_avg_fare,
    ROUND((s.avg_total_fare_bdt - o.overall_avg_fare)::numeric, 2) AS fare_diff_from_avg,
    ROUND(((s.avg_total_fare_bdt - o.overall_avg_fare) / o.overall_avg_fare * 100)::numeric, 2) AS fare_pct_diff_from_avg,
    CASE 
        WHEN s.seasonality_gh IN ('Christmas_NewYear', 'Easter', 'Eid_like', 'Hajj_like', 'ChaleWote_Week') THEN 'Peak'
        WHEN s.seasonality_gh IN ('Independence_Day', 'Homowo_Festival') THEN 'Festival'
        WHEN s.seasonality_gh = 'BackToSchool' THEN 'School'
        ELSE 'Non-Peak'
    END AS season_type,
    CURRENT_TIMESTAMP AS calculated_at
FROM seasonal_stats s
CROSS JOIN overall_avg o
ORDER BY s.avg_total_fare_bdt DESC

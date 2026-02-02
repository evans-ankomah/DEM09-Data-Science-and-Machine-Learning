/*
============================================
Gold Layer: Seasonal Fare Variation
============================================

Purpose:
  Analyze fare patterns across different seasons (from original dataset).
  Compares peak vs non-peak season pricing.

Source: silver.stg_flight_prices
*/

{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'kpi']
) }}

WITH seasonal_stats AS (
    SELECT
        seasonality,
        COUNT(*) AS booking_count,
        ROUND(AVG(base_fare_bdt)::numeric, 2) AS avg_base_fare_bdt,
        ROUND(AVG(tax_surcharge_bdt)::numeric, 2) AS avg_tax_surcharge_bdt,
        ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_total_fare_bdt,
        ROUND(MIN(total_fare_bdt)::numeric, 2) AS min_total_fare_bdt,
        ROUND(MAX(total_fare_bdt)::numeric, 2) AS max_total_fare_bdt,
        ROUND(STDDEV(total_fare_bdt)::numeric, 2) AS stddev_total_fare_bdt
    FROM {{ ref('stg_flight_prices') }}
    WHERE seasonality IS NOT NULL AND seasonality != ''
    GROUP BY seasonality
),

overall_avg AS (
    SELECT ROUND(AVG(total_fare_bdt)::numeric, 2) AS overall_avg_fare
    FROM {{ ref('stg_flight_prices') }}
)

SELECT
    s.seasonality,
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
        WHEN s.seasonality IN ('Eid', 'Hajj', 'Winter') THEN 'Peak'
        ELSE 'Non-Peak'
    END AS season_type,
    CURRENT_TIMESTAMP AS calculated_at
FROM seasonal_stats s
CROSS JOIN overall_avg o
ORDER BY s.avg_total_fare_bdt DESC

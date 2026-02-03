/*
============================================
Gold Layer: Average Fare by Route
============================================

Purpose:
  Analyze fare metrics for each source-destination route.
  Identifies most expensive and cheapest routes.

Source: silver.stg_flight_prices
*/

{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'kpi']
) }}

WITH route_stats AS (
    SELECT
        source,
        source_name,
        destination,
        destination_name,
        -- Create a combined route identifier
        source || ' → ' || destination AS route,
        source_name || ' → ' || destination_name AS route_full,
        COUNT(*) AS booking_count,
        ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_fare_bdt,
        ROUND(MIN(total_fare_bdt)::numeric, 2) AS min_fare_bdt,
        ROUND(MAX(total_fare_bdt)::numeric, 2) AS max_fare_bdt,
        ROUND(AVG(duration_hrs)::numeric, 2) AS avg_duration_hrs,
        -- Price per hour (value metric)
        CASE 
            WHEN AVG(duration_hrs) > 0 
            THEN ROUND((AVG(total_fare_bdt) / AVG(duration_hrs))::numeric, 2)
            ELSE NULL
        END AS avg_fare_per_hour
    FROM {{ ref('stg_flight_prices') }}
    WHERE source IS NOT NULL AND destination IS NOT NULL
      AND source != '' AND destination != ''
    GROUP BY source, source_name, destination, destination_name
),

overall_avg AS (
    SELECT AVG(avg_fare_bdt) AS overall_avg_fare FROM route_stats
)

SELECT
    rs.route,
    rs.route_full,
    rs.source,
    rs.source_name,
    rs.destination,
    rs.destination_name,
    rs.booking_count,
    rs.avg_fare_bdt,
    rs.min_fare_bdt,
    rs.max_fare_bdt,
    rs.avg_duration_hrs,
    rs.avg_fare_per_hour,
    -- Fare comparison to overall average
    ROUND(((rs.avg_fare_bdt - oa.overall_avg_fare) / oa.overall_avg_fare * 100)::numeric, 2) AS fare_vs_avg_pct,
    CURRENT_TIMESTAMP AS calculated_at
FROM route_stats rs
CROSS JOIN overall_avg oa
ORDER BY rs.booking_count DESC

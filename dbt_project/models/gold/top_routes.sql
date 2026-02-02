/*
============================================
Gold Layer: Top Routes
============================================

Purpose:
  Identify the most popular source-destination pairs by booking count.
  Also includes average fare for each route.

Source: silver.stg_flight_prices
*/

{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'kpi']
) }}

SELECT
    source,
    source_name,
    destination,
    destination_name,
    COUNT(*) AS booking_count,
    ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_fare_bdt,
    ROUND(MIN(total_fare_bdt)::numeric, 2) AS min_fare_bdt,
    ROUND(MAX(total_fare_bdt)::numeric, 2) AS max_fare_bdt,
    CURRENT_TIMESTAMP AS calculated_at
FROM {{ ref('stg_flight_prices') }}
WHERE source IS NOT NULL 
  AND destination IS NOT NULL
  AND source != '' 
  AND destination != ''
GROUP BY source, source_name, destination, destination_name
ORDER BY booking_count DESC
LIMIT 50

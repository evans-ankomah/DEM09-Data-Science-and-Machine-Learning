/*
============================================
Gold Layer: Average Fare by Airline
============================================

Purpose:
  Calculate average base fare, tax, and total fare per airline.
  Provides insight into pricing strategies across airlines.

Source: silver.stg_flight_prices
*/

{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'kpi']
) }}

SELECT
    airline,
    COUNT(*) AS booking_count,
    ROUND(AVG(base_fare_bdt)::numeric, 2) AS avg_base_fare_bdt,
    ROUND(AVG(tax_surcharge_bdt)::numeric, 2) AS avg_tax_surcharge_bdt,
    ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_total_fare_bdt,
    ROUND(MIN(total_fare_bdt)::numeric, 2) AS min_total_fare_bdt,
    ROUND(MAX(total_fare_bdt)::numeric, 2) AS max_total_fare_bdt,
    CURRENT_TIMESTAMP AS calculated_at
FROM {{ ref('stg_flight_prices') }}
WHERE airline IS NOT NULL AND airline != ''
GROUP BY airline
ORDER BY avg_total_fare_bdt DESC

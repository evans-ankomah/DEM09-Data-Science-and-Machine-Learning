/*
============================================
Gold Layer: Booking Count by Airline
============================================

Purpose:
  Count total bookings per airline to identify market share
  and popularity of each airline.

Source: silver.stg_flight_prices
*/

{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'kpi']
) }}

SELECT
    airline,
    COUNT(*) AS total_bookings,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS market_share_pct,
    CURRENT_TIMESTAMP AS calculated_at
FROM {{ ref('stg_flight_prices') }}
WHERE airline IS NOT NULL AND airline != ''
GROUP BY airline
ORDER BY total_bookings DESC

/*
============================================
Gold Layer: Average Fare by Travel Class
============================================

Purpose:
  Analyze fare metrics across different travel classes (Economy, Business, First Class).
  Useful for pricing strategy and class-based fare analysis.

Source: silver.stg_flight_prices
*/

{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'kpi']
) }}

WITH class_stats AS (
    SELECT
        class,
        COUNT(*) AS booking_count,
        ROUND(AVG(base_fare_bdt)::numeric, 2) AS avg_base_fare_bdt,
        ROUND(AVG(tax_surcharge_bdt)::numeric, 2) AS avg_tax_surcharge_bdt,
        ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_total_fare_bdt,
        ROUND(MIN(total_fare_bdt)::numeric, 2) AS min_fare_bdt,
        ROUND(MAX(total_fare_bdt)::numeric, 2) AS max_fare_bdt,
        ROUND(STDDEV(total_fare_bdt)::numeric, 2) AS fare_stddev
    FROM {{ ref('stg_flight_prices') }}
    WHERE class IS NOT NULL AND class != ''
    GROUP BY class
),

total_bookings AS (
    SELECT SUM(booking_count) AS total FROM class_stats
)

SELECT
    cs.class,
    cs.booking_count,
    ROUND((cs.booking_count::numeric / tb.total * 100), 2) AS class_share_pct,
    cs.avg_base_fare_bdt,
    cs.avg_tax_surcharge_bdt,
    cs.avg_total_fare_bdt,
    cs.min_fare_bdt,
    cs.max_fare_bdt,
    cs.fare_stddev,
    CURRENT_TIMESTAMP AS calculated_at
FROM class_stats cs
CROSS JOIN total_bookings tb
ORDER BY cs.avg_total_fare_bdt DESC

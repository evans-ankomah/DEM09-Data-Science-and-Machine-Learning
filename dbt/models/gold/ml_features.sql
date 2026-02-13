/*
============================================
Gold Layer: ML Feature-Ready Table
============================================

Purpose:
  Flat, feature-ready table for ML training.
  Excludes base_fare_bdt and tax_surcharge_bdt to prevent data leakage
  (Total Fare = Base Fare + Tax).

Source: silver.stg_flight_prices
Target: gold.ml_features
*/

{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'ml', 'features']
) }}

SELECT
    -- Categorical features
    airline,
    source,
    destination,
    class,
    stopovers,
    booking_source,
    seasonality,
    duration_bucket,
    booking_lead_bucket,
    aircraft_type,

    -- Numerical features
    duration_hrs,
    days_before_departure,

    -- Derived time features from departure_datetime
    EXTRACT(MONTH FROM departure_datetime)::int AS departure_month,
    EXTRACT(DOW FROM departure_datetime)::int AS departure_weekday,
    EXTRACT(HOUR FROM departure_datetime)::int AS departure_hour,

    -- Target variable
    total_fare_bdt

FROM {{ ref('stg_flight_prices') }}
WHERE total_fare_bdt > 0
  AND airline IS NOT NULL AND airline != ''
  AND source IS NOT NULL AND source != ''
  AND destination IS NOT NULL AND destination != ''

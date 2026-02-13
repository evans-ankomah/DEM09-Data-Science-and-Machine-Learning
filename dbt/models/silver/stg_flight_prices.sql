/*
============================================
Silver Layer: Staged Flight Prices (Incremental)
============================================

Purpose:
  Clean, validate, and standardize raw flight price data from the Bronze layer.

Source: bronze.raw_flight_prices
Target: silver.stg_flight_prices

OPTIMIZATION: Uses incremental materialization for faster updates.
  - Only processes new/updated rows based on updated_at timestamp
  - Uses booking_hash as unique key for merge strategy

Transformations Applied:
  1. Trim whitespace from text fields
  2. Parse datetime fields to proper timestamps
  3. Ensure non-negative fare values
  4. Standardize categorical fields
  5. Create duration_bucket (Short/Medium/Long)
  6. Create booking_lead_bucket (Last Minute/Standard/Early Bird)
*/

{{ config(
    materialized='incremental',
    schema='silver',
    unique_key='booking_hash',
    incremental_strategy='delete+insert',
    tags=['silver', 'staging', 'incremental']
) }}

WITH source_data AS (
    -- Select raw data from Bronze layer
    SELECT
        id,
        booking_hash,
        airline,
        source,
        source_name,
        destination,
        destination_name,
        departure_datetime,
        arrival_datetime,
        duration_hrs,
        stopovers,
        aircraft_type,
        class,
        booking_source,
        base_fare_bdt,
        tax_surcharge_bdt,
        total_fare_bdt,
        seasonality,
        days_before_departure,
        created_at,
        updated_at
    FROM {{ source('bronze', 'raw_flight_prices') }}
    
    {% if is_incremental() %}
        -- Only get new or updated rows since last run
        WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM {{ this }})
    {% endif %}
),

cleaned_data AS (
    -- Clean and standardize the data
    SELECT
        id,
        booking_hash,
        
        -- Trim and standardize text fields
        TRIM(airline) AS airline,
        TRIM(source) AS source,
        TRIM(source_name) AS source_name,
        TRIM(destination) AS destination,
        TRIM(destination_name) AS destination_name,
        
        -- Parse datetime fields
        CASE 
            WHEN departure_datetime IS NOT NULL AND departure_datetime != ''
            THEN TO_TIMESTAMP(departure_datetime, 'YYYY-MM-DD HH24:MI:SS')
            ELSE NULL
        END AS departure_datetime,
        
        CASE 
            WHEN arrival_datetime IS NOT NULL AND arrival_datetime != ''
            THEN TO_TIMESTAMP(arrival_datetime, 'YYYY-MM-DD HH24:MI:SS')
            ELSE NULL
        END AS arrival_datetime,
        
        -- Numeric fields
        COALESCE(duration_hrs, 0) AS duration_hrs,
        
        -- Standardize categorical fields
        TRIM(stopovers) AS stopovers,
        TRIM(aircraft_type) AS aircraft_type,
        TRIM(class) AS class,
        TRIM(booking_source) AS booking_source,
        
        -- Fare fields (ensure non-negative)
        GREATEST(COALESCE(base_fare_bdt, 0), 0) AS base_fare_bdt,
        GREATEST(COALESCE(tax_surcharge_bdt, 0), 0) AS tax_surcharge_bdt,
        GREATEST(COALESCE(total_fare_bdt, 0), 0) AS total_fare_bdt,
        
        -- Original seasonality from dataset
        TRIM(seasonality) AS seasonality,
        
        COALESCE(days_before_departure, 0) AS days_before_departure,
        created_at,
        updated_at
    FROM source_data
)

SELECT
    id,
    booking_hash,
    airline,
    source,
    source_name,
    destination,
    destination_name,
    departure_datetime,
    arrival_datetime,
    duration_hrs,
    
    -- Duration Bucket (categorize flight length)
    CASE
        WHEN duration_hrs <= 3 THEN 'Short (0-3h)'
        WHEN duration_hrs <= 6 THEN 'Medium (3-6h)'
        ELSE 'Long (6+h)'
    END AS duration_bucket,
    
    stopovers,
    aircraft_type,
    class,
    booking_source,
    base_fare_bdt,
    tax_surcharge_bdt,
    total_fare_bdt,
    seasonality,
    days_before_departure,
    
    -- Booking Lead Time Bucket
    CASE
        WHEN days_before_departure <= 3 THEN 'Last Minute (0-3 days)'
        WHEN days_before_departure <= 14 THEN 'Short Notice (4-14 days)'
        WHEN days_before_departure <= 30 THEN 'Standard (15-30 days)'
        ELSE 'Early Bird (30+ days)'
    END AS booking_lead_bucket,
    
    created_at,
    updated_at,
    CURRENT_TIMESTAMP AS processed_at
FROM cleaned_data

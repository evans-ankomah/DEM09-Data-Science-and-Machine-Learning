/*
============================================
Silver Layer: Staged Flight Prices
============================================

Purpose:
  Clean, validate, and standardize raw flight price data from the Bronze layer.
  Adds Ghana-like seasonality mapping (seasonality_gh) based on departure date.

Source: bronze.raw_flight_prices
Target: silver.stg_flight_prices

Ghana-Like Seasonality Rules (Priority Order):
  1. Eid (from dataset) → Eid_like
  2. Hajj (from dataset) → Hajj_like
  3. Aug 10-20 → ChaleWote_Week
  4. Aug 1-31 → Homowo_Festival
  5. Dec 15 - Jan 10 → Christmas_NewYear
  6. Mar 1-15 → Independence_Day
  7. Mar 15 - Apr 30 → Easter
  8. Dec 1-10 → Farmers_Day
  9. Jan 11 - Feb 15 OR Aug 15 - Sep 30 → BackToSchool
  10. Otherwise → Regular
*/

{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'staging']
) }}

WITH source_data AS (
    -- Select raw data from Bronze layer
    SELECT
        id,
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
        created_at
    FROM {{ source('bronze', 'raw_flight_prices') }}
),

cleaned_data AS (
    -- Clean and standardize the data
    SELECT
        id,
        
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
        
        -- Numeric fields (already cleaned in MySQL)
        duration_hrs,
        
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
        
        days_before_departure,
        created_at
    FROM source_data
),

final AS (
    SELECT
        id,
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
        
        -- Ghana-like seasonality mapping
        CASE
            -- Priority 1 & 2: Use dataset seasonality for Eid and Hajj
            WHEN seasonality = 'Eid' THEN 'Eid_like'
            WHEN seasonality = 'Hajj' THEN 'Hajj_like'
            
            -- Priority 3: ChaleWote_Week (Aug 10-20)
            WHEN EXTRACT(MONTH FROM departure_datetime) = 8 
                 AND EXTRACT(DAY FROM departure_datetime) BETWEEN 10 AND 20 
            THEN 'ChaleWote_Week'
            
            -- Priority 4: Homowo_Festival (August)
            WHEN EXTRACT(MONTH FROM departure_datetime) = 8 
            THEN 'Homowo_Festival'
            
            -- Priority 5: Christmas_NewYear (Dec 15 - Jan 10)
            WHEN (EXTRACT(MONTH FROM departure_datetime) = 12 
                  AND EXTRACT(DAY FROM departure_datetime) >= 15)
                 OR (EXTRACT(MONTH FROM departure_datetime) = 1 
                     AND EXTRACT(DAY FROM departure_datetime) <= 10)
            THEN 'Christmas_NewYear'
            
            -- Priority 6: Independence_Day (Mar 1-15)
            WHEN EXTRACT(MONTH FROM departure_datetime) = 3 
                 AND EXTRACT(DAY FROM departure_datetime) BETWEEN 1 AND 15
            THEN 'Independence_Day'
            
            -- Priority 7: Easter (Mar 15 - Apr 30)
            WHEN (EXTRACT(MONTH FROM departure_datetime) = 3 
                  AND EXTRACT(DAY FROM departure_datetime) >= 15)
                 OR (EXTRACT(MONTH FROM departure_datetime) = 4)
            THEN 'Easter'
            
            -- Priority 8: Farmers_Day (Dec 1-10)
            WHEN EXTRACT(MONTH FROM departure_datetime) = 12 
                 AND EXTRACT(DAY FROM departure_datetime) BETWEEN 1 AND 10
            THEN 'Farmers_Day'
            
            -- Priority 9: BackToSchool (Jan 11 - Feb 15 OR Aug 15 - Sep 30)
            WHEN (EXTRACT(MONTH FROM departure_datetime) = 1 
                  AND EXTRACT(DAY FROM departure_datetime) >= 11)
                 OR (EXTRACT(MONTH FROM departure_datetime) = 2 
                     AND EXTRACT(DAY FROM departure_datetime) <= 15)
                 OR (EXTRACT(MONTH FROM departure_datetime) = 8 
                     AND EXTRACT(DAY FROM departure_datetime) >= 15)
                 OR (EXTRACT(MONTH FROM departure_datetime) = 9)
            THEN 'BackToSchool'
            
            -- Default: Regular
            ELSE 'Regular'
        END AS seasonality_gh,
        
        created_at,
        CURRENT_TIMESTAMP AS processed_at
        
    FROM cleaned_data
)

SELECT * FROM final

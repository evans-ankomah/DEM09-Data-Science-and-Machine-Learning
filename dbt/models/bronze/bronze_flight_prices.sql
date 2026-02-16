/*
============================================
Bronze Layer: Raw Flight Prices (View)
============================================

Purpose:
  Documented view over the bronze.raw_flight_prices table
  loaded by the Airflow DAG. Provides full Bronze → Silver → Gold
  lineage within dbt.

Source: bronze.raw_flight_prices (loaded by Airflow PythonOperator)
Target: bronze.bronze_flight_prices (view)

Note:
  This is a pass-through view — no transformations.
  The raw data is loaded by the Airflow DAG (CSV → Postgres),
  and this dbt model simply makes it visible in the dbt DAG
  for lineage tracking and documentation.
*/

{{ config(
    materialized='view',
    schema='bronze',
    tags=['bronze', 'raw', 'source']
) }}

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

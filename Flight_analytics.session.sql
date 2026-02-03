-- Top 5 airlines by market share
SELECT airline, total_bookings, market_share_pct 
FROM public_gold.booking_count_by_airline 
ORDER BY total_bookings DESC 
LIMIT 5;

-- Ghana seasonality fare comparison
SELECT seasonality_gh, booking_count, avg_total_fare_bdt, season_type 
FROM public_gold.seasonal_fare_variation_gh 
ORDER BY avg_total_fare_bdt DESC;

-- Sample of Silver layer with Ghana seasonality
SELECT id, airline, source, destination, seasonality, seasonality_gh, total_fare_bdt 
FROM public_silver.stg_flight_prices 
LIMIT 20;
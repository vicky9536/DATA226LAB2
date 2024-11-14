
  create or replace   view dev.analytics.price90days_all_symbols
  
   as (
    SELECT *
FROM dev.raw_data.time_series_daily
WHERE close IS NOT NULL
  );


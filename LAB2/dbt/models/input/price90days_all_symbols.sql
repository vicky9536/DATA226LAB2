SELECT *
FROM {{source('raw_data', 'time_series_daily')}}
WHERE close IS NOT NULL
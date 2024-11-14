-- models/staging/stg_market_data.sql
{{ config(
    materialized='view',
    tags=['lab1'],
    schema='analytics'
) }}

SELECT 
    stock,
    open,
    high,
    low,
    close,
    volume,
    date,
    LAG(close) OVER (PARTITION BY stock ORDER BY date) as previous_close,
    (close - LAG(close) OVER (PARTITION BY stock ORDER BY date)) / 
        NULLIF(LAG(close) OVER (PARTITION BY stock ORDER BY date), 0) * 100 as daily_return
FROM {{ source('raw_data', 'market_data') }}
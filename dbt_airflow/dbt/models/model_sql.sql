-- models/lab1/market_data.sql
{{ config(
    materialized='table',
    tags=['lab1']
) }}

SELECT 
    stock,
    open,
    high,
    low,
    close,
    volume,
    date
FROM {{ ref('raw_market_data') }}

-- models/forecast/prep_training.sql
{{ config(
    materialized='table',
    tags=['prep_for_training']
) }}

SELECT 
    stock,
    date,
    close,
    LAG(close, 1) OVER (PARTITION BY stock ORDER BY date) as prev_day_close,
    LAG(close, 7) OVER (PARTITION BY stock ORDER BY date) as prev_week_close
FROM {{ ref('market_data') }}
WHERE date >= DATEADD(day, -90, CURRENT_DATE())
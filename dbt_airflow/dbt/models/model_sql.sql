-- models/forecast/prep_training.sql
{{ config(
    materialized='table',
    tags=['prep_for_training']
) }}

WITH market_data AS (
    SELECT 
        stock,
        date,
        close
    FROM {{ ref('market_data') }}
)
SELECT 
    stock,
    date,
    close,
    LAG(close, 1) OVER (PARTITION BY stock ORDER BY date) as prev_day_close,
    LAG(close, 7) OVER (PARTITION BY stock ORDER BY date) as prev_week_close,
    CASE 
        WHEN COUNT(*) = 0 THEN NULL -- Handle scenarios with no data
        ELSE COUNT(*) OVER () -- Optional: provide total record count
    END AS total_records
FROM market_data
WHERE date >= DATEADD(day, -90, CURRENT_DATE());

{{ config(
    materialized='table',
    tags=['prep_for_training']
) }}

WITH daily_metrics AS (
    SELECT 
        stock,
        date,
        close,
        daily_return,
        AVG(close) OVER (
            PARTITION BY stock 
            ORDER BY date 
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) as moving_avg_7d,
        AVG(close) OVER (
            PARTITION BY stock 
            ORDER BY date 
            ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
        ) as moving_avg_30d,
        ROW_NUMBER() OVER (PARTITION BY stock ORDER BY date DESC) as recency_rank
    FROM {{ ref('stg_market_data') }}
)

SELECT 
    stock,
    date,
    close,
    daily_return,
    moving_avg_7d,
    moving_avg_30d,
    CASE 
        WHEN close > moving_avg_7d THEN 'ABOVE_7D_MA'
        ELSE 'BELOW_7D_MA'
    END as price_position_7d,
    CASE 
        WHEN close > moving_avg_30d THEN 'ABOVE_30D_MA'
        ELSE 'BELOW_30D_MA'
    END as price_position_30d
FROM daily_metrics
WHERE recency_rank <= 90
WITH validation AS (
    SELECT 
        stock,
        date,
        daily_return
    FROM {{ ref('stg_market_data') }}
    WHERE ABS(daily_return) > 30  -- Flag suspicious daily returns > 30%
)

SELECT *
FROM validation
{% snapshot market_data_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

SELECT 
    MD.stock || '_' || MD.date as id,
    MD.stock,
    MD.date,
    MD.close,
    MA.moving_avg_7d,
    MA.moving_avg_30d,
    CURRENT_TIMESTAMP() as updated_at
FROM {{ source('raw_data', 'market_data') }} MD
LEFT JOIN {{ ref('market_analysis') }} MA 
    ON MD.stock = MA.stock 
    AND MD.date = MA.date

{% endsnapshot %}
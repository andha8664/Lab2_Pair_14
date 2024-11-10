SELECT
    sessionId,
    ts
FROM {{ source('raw_data', 'session_timestamp') }}

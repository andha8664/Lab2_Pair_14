SELECT
    userId,
    sessionId,
    channel
FROM {{ source('raw_data', 'user_session_channel') }}

{{
  config(
    materialized = 'table',
    partition_by={
      "field": "EVENT_DATE",
      "data_type": "date"
    },
    cluster_by=['EVENT_HOUR', 'EVENT_TYPE']
    )
}}

SELECT
    DATE(EVENT_DATE)                    AS EVENT_DATE,
    FORMAT_TIMESTAMP('%H', EVENT_DATE)  AS EVENT_HOUR,
    EVENT_TYPE                          AS EVENT_TYPE,
    COUNT(1)                            AS NUM_EVENTS,
    APPROX_COUNT_DISTINCT(USERNAME)     AS NUM_DISTINCT_USERS
FROM {{ ref("DTM_GH_EVENTS") }}
WHERE EVENT_DATE > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY DATE(EVENT_DATE), FORMAT_TIMESTAMP('%H', EVENT_DATE), EVENT_TYPE
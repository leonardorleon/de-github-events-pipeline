{{
  config(
    materialized = 'incremental',
    cluster_by = ['EVENT_DATE'],
    unique_key = ['EVENT_DATE', 'USERNAME'],
    partition_by={
      "field": "EVENT_DATE",
      "data_type": "date",
      "granularity": "day"
    }
    )
}}

SELECT
    DATE(EVENT_DATE)    AS EVENT_DATE,
    USERNAME            AS USERNAME,
    COUNT(*)            AS TOTAL_PUSHES
FROM {{ ref("DTM_GH_EVENTS") }}
WHERE EVENT_TYPE = 'PushEvent'
    AND USER_TYPE = 'User'
    AND lower(USERNAME) NOT LIKE '%bot%'
    AND USERNAME IS NOT NULL
{% if is_incremental() %}
    AND DATE(EVENT_DATE) >= coalesce((select max(EVENT_DATE) from {{ this }}), '1900-01-01')
{% endif %}
GROUP BY 
    DATE(EVENT_DATE),
    USERNAME
QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE(EVENT_DATE), USERNAME ORDER BY COUNT(*) DESC) <= 100
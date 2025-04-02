{{
  config(
    materialized = 'incremental',
    cluster_by = ['EVENT_DATE','EVENT_TYPE'],
    unique_key = ['EVENT_DATE','EVENT_TYPE'],
    partition_by={
      "field": "EVENT_DATE",
      "data_type": "date",
      "granularity": "day"
    }
    )
}}

SELECT
    DATE(EVENT_DATE)    AS EVENT_DATE,
    EVENT_TYPE          AS EVENT_TYPE,
    count(*)            AS TOTAL_EVENTS
FROM {{ ref("DTM_GH_EVENTS") }}
{% if is_incremental() %}
  WHERE DATE(EVENT_DATE) >= coalesce((select max(EVENT_DATE) from {{ this }}), '1900-01-01')
{% endif %}
GROUP BY 
DATE(EVENT_DATE),
EVENT_TYPE
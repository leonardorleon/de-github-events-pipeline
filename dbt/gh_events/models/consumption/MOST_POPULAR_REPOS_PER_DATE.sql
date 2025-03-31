{{
  config(
    materialized = 'incremental',
    cluster_by = ['EVENT_DATE'],
    unique_key = ['EVENT_DATE', 'REPO_NAME'],
    partition_by={
      "field": "EVENT_DATE",
      "data_type": "date",
      "granularity": "day"
    }
    )
}}

SELECT
    DATE(EVENT_DATE)                      AS EVENT_DATE,
    REPO_NAME                             AS REPO_NAME,
    MAX(REPO_STARGAZERS_COUNT)            AS TOTAL_STARGAZERS,
    MAX(REPO_WATCHERS_COUNT)              AS TOTAL_WATCHERS,
    MAX(REPO_FORKS_COUNT)                 AS TOTAL_FORKS,
    MAX(REPO_OPEN_ISSUES_COUNT)           AS TOTAL_OPEN_ISSUES,
FROM {{ ref("DTM_GH_EVENTS") }}
WHERE REPO_NAME IS NOT NULL
{% if is_incremental() %}
    AND DATE(EVENT_DATE) >= coalesce((select max(EVENT_DATE) from {{ this }}), '1900-01-01')
{% endif %}
GROUP BY 
    DATE(EVENT_DATE),
    REPO_NAME
QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE(EVENT_DATE), REPO_NAME ORDER BY COUNT(*) DESC) <= 100
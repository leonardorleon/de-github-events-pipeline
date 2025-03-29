{{
  config(
    materialized = 'view'
    )
}}


SELECT
    -- select main event fields
    {{gh_event_main_fields()}}
    -- select repo fields
    {{gh_event_repo_fields()}}
    -- select actor fields
    {{gh_event_actor_fields()}}
    -- select organization fields
    {{gh_event_org_fields()}}
    
    PARSE_JSON(payload) as payload_json

    


    -- CAST(json_extract(payload, "$.push_id")         AS STRING)      AS PUSH_ID,
    -- CAST(json_extract(payload, "$.size")            AS INT)         AS _SIZE,
    -- CAST(json_extract(payload, "$.distinct_size")   AS INT)         AS DISTINCT_SIZE,
    -- CAST(json_extract(payload, "$.before")          AS STRING)      AS _BEFORE,
    -- json_extract_array(json_extract(payload, "$.commits"))          AS COMMITS,
    -- CAST(json_extract(payload, "$.ref")             AS STRING)      AS REF,
    -- CAST(json_extract(payload, "$.head")            AS STRING)      AS HEAD,
FROM {{ source('landing_zone', 'PushEvent') }}
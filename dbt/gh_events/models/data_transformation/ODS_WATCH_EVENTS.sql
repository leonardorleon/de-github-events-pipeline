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

    PARSE_JSON(payload) as PAYLOAD_JSON

FROM {{ source('landing_zone', 'WatchEvent') }}
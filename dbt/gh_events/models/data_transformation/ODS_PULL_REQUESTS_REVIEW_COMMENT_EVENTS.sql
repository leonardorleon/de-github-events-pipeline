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

    PARSE_JSON(payload) as PAYLOAD_JSON,
    PARSE_JSON(JSON_EXTRACT(payload, '$.pull_request.user'))        as USER_JSON,
    PARSE_JSON(JSON_EXTRACT(payload, '$.pull_request.base'))        as BASE_JSON,
    PARSE_JSON(JSON_EXTRACT(payload, '$.pull_request.base.repo'))   as BASE_REPO_JSON,
    PARSE_JSON(JSON_EXTRACT(payload, '$.pull_request.base.user'))   as BASE_USER_JSON,
    PARSE_JSON(JSON_EXTRACT(payload, '$.pull_request.head'))        as HEAD_JSON,
    PARSE_JSON(JSON_EXTRACT(payload, '$.pull_request.head.repo'))   as HEAD_REPO_JSON,
    PARSE_JSON(JSON_EXTRACT(payload, '$.pull_request.head.user'))   as HEAD_USER_JSON,
    PARSE_JSON(JSON_EXTRACT(payload, '$.pull_request.merged_by'))   as MERGED_BY_JSON,
    CURRENT_TIMESTAMP()   AS LOAD_TIMESTAMP

FROM {{ source('landing_zone', 'PullRequestReviewCommentEvent') }}
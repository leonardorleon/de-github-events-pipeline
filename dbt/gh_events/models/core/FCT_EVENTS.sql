{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = 'EVENT_TYPE',
    partition_by={
      "field": "CREATED_AT",
      "data_type": "timestamp",
      "granularity": "day"
    },
    incremental_strategy = 'insert_overwrite'
    )
}}

with unioned as (
    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID,
        CREATED_AT
    FROM {{ ref("ODS_COMMIT_COMMENT_EVENTS") }} AS COMMENT_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(CREATED_AT) 
            from {{ this }} 
            WHERE EVENT_TYPE = "{{ get_event_type('ODS_COMMIT_COMMENT_EVENTS') }}"
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID,
        CREATED_AT
    FROM {{ ref("ODS_CREATE_EVENTS") }} AS CREATE_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(CREATED_AT) 
            from {{ this }} 
            WHERE EVENT_TYPE = "{{ get_event_type('ODS_CREATE_EVENTS') }}"
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID,
        CREATED_AT
    FROM {{ ref("ODS_DELETE_EVENTS") }} AS DELETE_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(CREATED_AT) 
            from {{ this }} 
            WHERE EVENT_TYPE = "{{ get_event_type('ODS_DELETE_EVENTS') }}"
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID,
        CREATED_AT
    FROM {{ ref("ODS_FORK_EVENTS") }} AS FORK_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(CREATED_AT) 
            from {{ this }} 
            WHERE EVENT_TYPE = "{{ get_event_type('ODS_FORK_EVENTS') }}"
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID,
        CREATED_AT
    FROM {{ ref("ODS_GOLLUM_EVENTS") }} AS GOLLUM_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(CREATED_AT) 
            from {{ this }} 
            WHERE EVENT_TYPE = "{{ get_event_type('ODS_GOLLUM_EVENTS') }}"
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID,
        CREATED_AT
    FROM {{ ref("ODS_ISSUE_COMMENT_EVENTS") }} AS ISSUE_COMMENT_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(CREATED_AT) 
            from {{ this }} 
            WHERE EVENT_TYPE = "{{ get_event_type('ODS_ISSUE_COMMENT_EVENTS') }}"
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID,
        CREATED_AT
    FROM {{ ref("ODS_ISSUES_EVENTS") }} AS ISSUE_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(CREATED_AT) 
            from {{ this }} 
            WHERE EVENT_TYPE = "{{ get_event_type('ODS_ISSUES_EVENTS') }}"
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID,
        CREATED_AT
    FROM {{ ref("ODS_MEMBER_EVENTS") }} AS MEMBER_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(CREATED_AT) 
            from {{ this }} 
            WHERE EVENT_TYPE = "{{ get_event_type('ODS_MEMBER_EVENTS') }}"
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID,
        CREATED_AT
    FROM {{ ref("ODS_PUBLIC_EVENTS") }} AS PUBLIC_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(CREATED_AT) 
            from {{ this }} 
            WHERE EVENT_TYPE = "{{ get_event_type('ODS_PUBLIC_EVENTS') }}"
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID,
        CREATED_AT
    FROM {{ ref("ODS_PULL_REQUEST_EVENTS") }} AS PULL_REQUEST_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(CREATED_AT) 
            from {{ this }} 
            WHERE EVENT_TYPE = "{{ get_event_type('ODS_PULL_REQUEST_EVENTS') }}"
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID,
        CREATED_AT
    FROM {{ ref("ODS_PULL_REQUESTS_REVIEW_COMMENT_EVENTS") }} AS PULL_REQUESTS_REVIEW_COMMENT_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(CREATED_AT) 
            from {{ this }} 
            WHERE EVENT_TYPE = "{{ get_event_type('ODS_PULL_REQUESTS_REVIEW_COMMENT_EVENTS') }}"
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID,
        CREATED_AT
    FROM {{ ref("ODS_PUSH_EVENTS") }} AS PUSH_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(CREATED_AT) 
            from {{ this }} 
            WHERE EVENT_TYPE = "{{ get_event_type('ODS_PUSH_EVENTS') }}"
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID,
        CREATED_AT
    FROM {{ ref("ODS_WATCH_EVENTS") }} AS WATCH_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(CREATED_AT) 
            from {{ this }} 
            WHERE EVENT_TYPE = "{{ get_event_type('ODS_WATCH_EVENTS') }}"
            ) 
        , '1900-01-01')
    {%- endif %}
)

SELECT
    *,
    CURRENT_TIMESTAMP()   AS LOAD_TIMESTAMP
FROM unioned
{# QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CREATED_AT) = 1 #}
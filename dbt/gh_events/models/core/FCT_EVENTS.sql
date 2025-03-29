{{
  config(
    materialized = 'incremental',
    unique_key = 'id'
    )
}}

with unioned as (
    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID
    FROM {{ ref("ODS_COMMIT_COMMENT_EVENTS") }} AS COMMENT_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT >= coalesce((select max(CREATED_AT) from {{ this }} WHERE EVENT_TYPE = "{{ get_event_type(this) }}") , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID
    FROM {{ ref("ODS_CREATE_EVENTS") }} AS CREATE_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT >= coalesce((select max(CREATED_AT) from {{ this }} WHERE EVENT_TYPE = "{{ get_event_type(this) }}") , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID
    FROM {{ ref("ODS_DELETE_EVENTS") }} AS DELETE_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT >= coalesce((select max(CREATED_AT) from {{ this }} WHERE EVENT_TYPE = "{{ get_event_type(this) }}") , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID
    FROM {{ ref("ODS_FORK_EVENTS") }} AS FORK_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT >= coalesce((select max(CREATED_AT) from {{ this }} WHERE EVENT_TYPE = "{{ get_event_type(this) }}") , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID
    FROM {{ ref("ODS_GOLLUM_EVENTS") }} AS GOLLUM_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT >= coalesce((select max(CREATED_AT) from {{ this }} WHERE EVENT_TYPE = "{{ get_event_type(this) }}") , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID
    FROM {{ ref("ODS_ISSUE_COMMENT_EVENTS") }} AS ISSUE_COMMENT_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT >= coalesce((select max(CREATED_AT) from {{ this }} WHERE EVENT_TYPE = "{{ get_event_type(this) }}") , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID
    FROM {{ ref("ODS_ISSUES_EVENTS") }} AS ISSUE_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT >= coalesce((select max(CREATED_AT) from {{ this }} WHERE EVENT_TYPE = "{{ get_event_type(this) }}") , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID
    FROM {{ ref("ODS_MEMBER_EVENTS") }} AS MEMBER_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT >= coalesce((select max(CREATED_AT) from {{ this }} WHERE EVENT_TYPE = "{{ get_event_type(this) }}") , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID
    FROM {{ ref("ODS_PUBLIC_EVENTS") }} AS PUBLIC_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT >= coalesce((select max(CREATED_AT) from {{ this }} WHERE EVENT_TYPE = "{{ get_event_type(this) }}") , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID
    FROM {{ ref("ODS_PULL_REQUEST_EVENTS") }} AS PULL_REQUEST_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT >= coalesce((select max(CREATED_AT) from {{ this }} WHERE EVENT_TYPE = "{{ get_event_type(this) }}") , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID
    FROM {{ ref("ODS_PULL_REQUESTS_REVIEW_COMMENT_EVENTS") }} AS PULL_REQUESTS_REVIEW_COMMENT_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT >= coalesce((select max(CREATED_AT) from {{ this }} WHERE EVENT_TYPE = "{{ get_event_type(this) }}") , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID
    FROM {{ ref("ODS_PUSH_EVENTS") }} AS PUSH_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT >= coalesce((select max(CREATED_AT) from {{ this }} WHERE EVENT_TYPE = "{{ get_event_type(this) }}") , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        ID,
        EVENT_TYPE,
        PUBLIC,
        REPO_ID,
        ACTOR_ID,
        ORG_ID
    FROM {{ ref("ODS_WATCH_EVENTS") }} AS WATCH_EVENTS
    {% if is_incremental() -%}
    WHERE CREATED_AT >= coalesce((select max(CREATED_AT) from {{ this }} WHERE EVENT_TYPE = "{{ get_event_type(this) }}") , '1900-01-01')
    {%- endif %}
)

SELECT
    *
FROM unioned
{# QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CREATED_AT) = 1 #}
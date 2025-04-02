{{
  config(
    materialized = 'incremental',
    unique_key = 'USER_ID',
    cluster_by = 'USER_ID',
    partition_by={
      "field": "UPDATED_AT",
      "data_type": "timestamp",
      "granularity": "day"
    },
    incremental_strategy = 'merge'
    )
}}

WITH COMMIT_COMMENT_USERS AS (
    SELECT
        LAX_INT64(USER_JSON.id)         AS USER_ID,
        LAX_STRING(USER_JSON.login)     AS USERNAME,
        LAX_STRING(USER_JSON.type)      AS USER_TYPE,
        CREATED_AT                      AS UPDATED_AT,
        LOAD_TIMESTAMP
    FROM {{ ref('ODS_COMMIT_COMMENT_EVENTS') }}
    {% if is_incremental() -%}
    WHERE LOAD_TIMESTAMP > coalesce(
            (
            select max(LOAD_TIMESTAMP) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

), PULL_REQUESTS_REVIEW_COMMENT_USERS AS (

    SELECT
        LAX_INT64(USER_JSON.id)         AS USER_ID,
        LAX_STRING(USER_JSON.login)     AS USERNAME,
        LAX_STRING(USER_JSON.type)      AS USER_TYPE,
        CREATED_AT                      AS UPDATED_AT,
        LOAD_TIMESTAMP
    FROM {{ ref('ODS_PULL_REQUESTS_REVIEW_COMMENT_EVENTS') }}
    {% if is_incremental() -%}
    WHERE LOAD_TIMESTAMP > coalesce(
            (
            select max(LOAD_TIMESTAMP) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        LAX_INT64(BASE_REPO_JSON.id)        AS USER_ID,
        LAX_STRING(BASE_REPO_JSON.login)    AS USERNAME,
        LAX_STRING(BASE_REPO_JSON.type)     AS USER_TYPE,
        CREATED_AT                          AS UPDATED_AT,
        LOAD_TIMESTAMP
    FROM {{ ref('ODS_PULL_REQUESTS_REVIEW_COMMENT_EVENTS') }}
    {% if is_incremental() -%}
    WHERE LOAD_TIMESTAMP > coalesce(
            (
            select max(LOAD_TIMESTAMP) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        LAX_INT64(HEAD_USER_JSON.id)        AS USER_ID,
        LAX_STRING(HEAD_USER_JSON.login)    AS USERNAME,
        LAX_STRING(HEAD_USER_JSON.type)     AS USER_TYPE,
        CREATED_AT                          AS UPDATED_AT,
        LOAD_TIMESTAMP
    FROM {{ ref('ODS_PULL_REQUESTS_REVIEW_COMMENT_EVENTS') }}
    {% if is_incremental() -%}
    WHERE LOAD_TIMESTAMP > coalesce(
            (
            select max(LOAD_TIMESTAMP) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}


), ISSUE_COMMENT_USERS  AS (

    SELECT
        LAX_INT64(ISSUE_USER_JSON.id)       AS USER_ID,
        LAX_STRING(ISSUE_USER_JSON.login)   AS USERNAME,
        LAX_STRING(ISSUE_USER_JSON.type)    AS USER_TYPE,
        CREATED_AT                          AS UPDATED_AT,
        LOAD_TIMESTAMP
    FROM {{ ref('ODS_ISSUE_COMMENT_EVENTS') }}
    {% if is_incremental() -%}
    WHERE LOAD_TIMESTAMP > coalesce(
            (
            select max(LOAD_TIMESTAMP) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        LAX_INT64(COMMENT_USER_JSON.id)         AS USER_ID,
        LAX_STRING(COMMENT_USER_JSON.login)     AS USERNAME,
        LAX_STRING(COMMENT_USER_JSON.type)      AS USER_TYPE,
        CREATED_AT                              AS UPDATED_AT,
        LOAD_TIMESTAMP
    FROM {{ ref('ODS_ISSUE_COMMENT_EVENTS') }}
    {% if is_incremental() -%}
    WHERE LOAD_TIMESTAMP > coalesce(
            (
            select max(LOAD_TIMESTAMP) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

), PULL_REQUEST_USERS AS (
    SELECT
        LAX_INT64(USER_JSON.id)                 AS USER_ID,
        LAX_STRING(USER_JSON.login)             AS USERNAME,
        LAX_STRING(USER_JSON.type)              AS USER_TYPE,
        CREATED_AT                              AS UPDATED_AT,
        LOAD_TIMESTAMP
    FROM {{ ref('ODS_PULL_REQUEST_EVENTS') }}
    {% if is_incremental() -%}
    WHERE LOAD_TIMESTAMP > coalesce(
            (
            select max(LOAD_TIMESTAMP) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        LAX_INT64(BASE_USER_JSON.id)                AS USER_ID,
        LAX_STRING(BASE_USER_JSON.login)            AS USERNAME,
        LAX_STRING(BASE_USER_JSON.type)             AS USER_TYPE,
        CREATED_AT                                  AS UPDATED_AT,
        LOAD_TIMESTAMP
    FROM {{ ref('ODS_PULL_REQUEST_EVENTS') }}
    {% if is_incremental() -%}
    WHERE LOAD_TIMESTAMP > coalesce(
            (
            select max(LOAD_TIMESTAMP) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        LAX_INT64(HEAD_USER_JSON.id)            AS USER_ID,
        LAX_STRING(HEAD_USER_JSON.login)        AS USERNAME,
        LAX_STRING(HEAD_USER_JSON.type)         AS USER_TYPE,
        CREATED_AT                              AS UPDATED_AT,
        LOAD_TIMESTAMP
    FROM {{ ref('ODS_PULL_REQUEST_EVENTS') }}
    {% if is_incremental() -%}
    WHERE LOAD_TIMESTAMP > coalesce(
            (
            select max(LOAD_TIMESTAMP) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

), ISSUES_USERS AS (
    SELECT
        LAX_INT64(ISSUE_USER_JSON.id)       AS USER_ID,
        LAX_STRING(ISSUE_USER_JSON.login)   AS USERNAME,
        LAX_STRING(ISSUE_USER_JSON.type)    AS USER_TYPE,
        CREATED_AT                          AS UPDATED_AT,
        LOAD_TIMESTAMP
    FROM {{ ref('ODS_ISSUES_EVENTS') }}
    {% if is_incremental() -%}
    WHERE LOAD_TIMESTAMP > coalesce(
            (
            select max(LOAD_TIMESTAMP) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

), unioned as (
    SELECT
        USER_ID, USERNAME, USER_TYPE, UPDATED_AT, LOAD_TIMESTAMP
    FROM COMMIT_COMMENT_USERS
    UNION ALL
    SELECT 
        USER_ID, USERNAME, USER_TYPE, UPDATED_AT, LOAD_TIMESTAMP
    FROM PULL_REQUESTS_REVIEW_COMMENT_USERS
    UNION ALL
    SELECT 
        USER_ID, USERNAME, USER_TYPE, UPDATED_AT, LOAD_TIMESTAMP
    FROM ISSUE_COMMENT_USERS
    UNION ALL
    SELECT 
        USER_ID, USERNAME, USER_TYPE, UPDATED_AT, LOAD_TIMESTAMP
    FROM PULL_REQUEST_USERS
    UNION ALL
    SELECT 
        USER_ID, USERNAME, USER_TYPE, UPDATED_AT, LOAD_TIMESTAMP
    FROM ISSUES_USERS   
)

SELECT
    USER_ID,
    USERNAME,
    USER_TYPE,
    UPDATED_AT,
    LOAD_TIMESTAMP
FROM unioned
QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY UPDATED_AT DESC) = 1
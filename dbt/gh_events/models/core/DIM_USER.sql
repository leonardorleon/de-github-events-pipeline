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
    incremental_strategy = 'insert_overwrite'
    )
}}

WITH COMMIT_COMMENT_USERS AS (
    SELECT
        LAX_STRING(USER_JSON.id)        AS USER_ID,
        LAX_STRING(USER_JSON.login)     AS USERNAME,
        LAX_STRING(USER_JSON.type)      AS USER_TYPE,
        CREATED_AT                      AS UPDATED_AT
    FROM {{ ref('ODS_COMMIT_COMMENT_EVENTS') }}
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(UPDATED_AT) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

), PULL_REQUESTS_REVIEW_COMMENT_USERS AS (

    SELECT
        LAX_STRING(USER_JSON.id)        AS USER_ID,
        LAX_STRING(USER_JSON.login)     AS USERNAME,
        LAX_STRING(USER_JSON.type)      AS USER_TYPE,
        CREATED_AT                      AS UPDATED_AT
    FROM {{ ref('ODS_PULL_REQUESTS_REVIEW_COMMENT_EVENTS') }}
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(UPDATED_AT) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        LAX_STRING(BASE_REPO_JSON.id)       AS USER_ID,
        LAX_STRING(BASE_REPO_JSON.login)    AS USERNAME,
        LAX_STRING(BASE_REPO_JSON.type)     AS USER_TYPE,
        CREATED_AT                          AS UPDATED_AT
    FROM {{ ref('ODS_PULL_REQUESTS_REVIEW_COMMENT_EVENTS') }}
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(UPDATED_AT) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        LAX_STRING(HEAD_USER_JSON.id)       AS USER_ID,
        LAX_STRING(HEAD_USER_JSON.login)    AS USERNAME,
        LAX_STRING(HEAD_USER_JSON.type)     AS USER_TYPE,
        CREATED_AT                          AS UPDATED_AT
    FROM {{ ref('ODS_PULL_REQUESTS_REVIEW_COMMENT_EVENTS') }}
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(UPDATED_AT) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}


), ISSUE_COMMENT_USERS  AS (

    SELECT
        LAX_STRING(ISSUE_USER_JSON.id)      AS USER_ID,
        LAX_STRING(ISSUE_USER_JSON.login)   AS USERNAME,
        LAX_STRING(ISSUE_USER_JSON.type)    AS USER_TYPE,
        CREATED_AT                          AS UPDATED_AT
    FROM {{ ref('ODS_ISSUE_COMMENT_EVENTS') }}
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(UPDATED_AT) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        LAX_STRING(COMMENT_USER_JSON.id)        AS USER_ID,
        LAX_STRING(COMMENT_USER_JSON.login)     AS USERNAME,
        LAX_STRING(COMMENT_USER_JSON.type)      AS USER_TYPE,
        CREATED_AT                              AS UPDATED_AT
    FROM {{ ref('ODS_ISSUE_COMMENT_EVENTS') }}
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(UPDATED_AT) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

), PULL_REQUEST_USERS AS (
    SELECT
        LAX_STRING(USER_JSON.id)                AS USER_ID,
        LAX_STRING(USER_JSON.login)             AS USERNAME,
        LAX_STRING(USER_JSON.type)              AS USER_TYPE,
        CREATED_AT                              AS UPDATED_AT
    FROM {{ ref('ODS_PULL_REQUEST_EVENTS') }}
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(UPDATED_AT) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        LAX_STRING(BASE_USER_JSON.id)               AS USER_ID,
        LAX_STRING(BASE_USER_JSON.login)            AS USERNAME,
        LAX_STRING(BASE_USER_JSON.type)             AS USER_TYPE,
        CREATED_AT                                  AS UPDATED_AT
    FROM {{ ref('ODS_PULL_REQUEST_EVENTS') }}
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(UPDATED_AT) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

    UNION ALL

    SELECT
        LAX_STRING(HEAD_USER_JSON.id)           AS USER_ID,
        LAX_STRING(HEAD_USER_JSON.login)        AS USERNAME,
        LAX_STRING(HEAD_USER_JSON.type)         AS USER_TYPE,
        CREATED_AT                              AS UPDATED_AT
    FROM {{ ref('ODS_PULL_REQUEST_EVENTS') }}
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(UPDATED_AT) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

), ISSUES_USERS AS (
    SELECT
        LAX_STRING(ISSUE_USER_JSON.id)      AS USER_ID,
        LAX_STRING(ISSUE_USER_JSON.login)   AS USERNAME,
        LAX_STRING(ISSUE_USER_JSON.type)    AS USER_TYPE,
        CREATED_AT                          AS UPDATED_AT
    FROM {{ ref('ODS_ISSUES_EVENTS') }}
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(UPDATED_AT) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

), unioned as (
    SELECT
        USER_ID, USERNAME, USER_TYPE, UPDATED_AT
    FROM COMMIT_COMMENT_USERS
    UNION ALL
    SELECT 
        USER_ID, USERNAME, USER_TYPE, UPDATED_AT
    FROM PULL_REQUESTS_REVIEW_COMMENT_USERS
    UNION ALL
    SELECT 
        USER_ID, USERNAME, USER_TYPE, UPDATED_AT
    FROM ISSUE_COMMENT_USERS
    UNION ALL
    SELECT 
        USER_ID, USERNAME, USER_TYPE, UPDATED_AT
    FROM PULL_REQUEST_USERS
    UNION ALL
    SELECT 
        USER_ID, USERNAME, USER_TYPE, UPDATED_AT
    FROM ISSUES_USERS   
)

SELECT DISTINCT
    USER_ID,
    USERNAME,
    USER_TYPE,
    UPDATED_AT
FROM unioned

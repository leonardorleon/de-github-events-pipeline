{{
  config(
    materialized = 'incremental',
    unique_key = 'REPO_ID',
    cluster_by = 'REPO_ID',
    partition_by={
      "field": "UPDATED_AT",
      "data_type": "timestamp",
      "granularity": "day"
    },
    incremental_strategy = 'merge'
    )
}}

WITH PULL_REQUEST_REVIEW_COMMENT_BASE AS (
    
    SELECT

        LAX_INT64(BASE_REPO_JSON.id)                    AS REPO_ID,
        LAX_STRING(BASE_REPO_JSON.name)                 AS REPO_NAME,
        LAX_STRING(BASE_REPO_JSON.full_name)            AS REPO_FULL_NAME,
        LAX_BOOL(BASE_REPO_JSON.private)                AS REPO_IS_PRIVATE,
        LAX_STRING(BASE_REPO_JSON.description)          AS REPO_DESCRIPTION,
        LAX_BOOL(BASE_REPO_JSON.fork)                   AS REPO_IS_FORK,
        LAX_STRING(BASE_REPO_JSON.created_at)           AS REPO_CREATED_AT,
        LAX_STRING(BASE_REPO_JSON.updated_at)           AS REPO_UPDATED_AT,
        LAX_STRING(BASE_REPO_JSON.pushed_at)            AS REPO_PUSHED_AT,
        LAX_STRING(BASE_REPO_JSON.git_url)              AS REPO_GIT_URL,
        LAX_INT64(BASE_REPO_JSON.stargazers_count)      AS REPO_STARGAZERS_COUNT,
        LAX_INT64(BASE_REPO_JSON.watchers_count)        AS REPO_WATCHERS_COUNT,
        LAX_STRING(BASE_REPO_JSON.language)             AS REPO_LANGUAGE,
        LAX_STRING(BASE_REPO_JSON.has_issues)           AS REPO_HAS_ISSUES,
        LAX_STRING(BASE_REPO_JSON.has_downloads)        AS REPO_HAS_DOWNLOADS,
        LAX_STRING(BASE_REPO_JSON.has_wiki)             AS REPO_HAS_WIKI,
        LAX_STRING(BASE_REPO_JSON.has_pages)            AS REPO_HAS_PAGES,
        LAX_INT64(BASE_REPO_JSON.forks_count)           AS REPO_FORKS_COUNT,
        LAX_INT64(BASE_REPO_JSON.open_issues_count)     AS REPO_OPEN_ISSUES_COUNT,
        LAX_INT64(BASE_REPO_JSON.forks)                 AS REPO_FORKS,
        LAX_INT64(BASE_REPO_JSON.open_issues)           AS REPO_OPEN_ISSUES,
        LAX_INT64(BASE_REPO_JSON.watchers)              AS REPO_WATCHERS,
        LAX_STRING(BASE_REPO_JSON.default_branch)       AS REPO_DEFAULT_BRANCH,
        CREATED_AT                                      AS UPDATED_AT,
        LOAD_TIMESTAMP

    FROM {{ ref('ODS_PULL_REQUESTS_REVIEW_COMMENT_EVENTS') }}
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(UPDATED_AT) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

), PULL_REQUEST_REVIEW_COMMENT_HEAD AS (
    
    SELECT
        LAX_INT64(HEAD_REPO_JSON.id)                    AS REPO_ID,
        LAX_STRING(HEAD_REPO_JSON.name)                 AS REPO_NAME,
        LAX_STRING(HEAD_REPO_JSON.full_name)            AS REPO_FULL_NAME,
        LAX_BOOL(HEAD_REPO_JSON.private)                AS REPO_IS_PRIVATE,
        LAX_STRING(HEAD_REPO_JSON.description)          AS REPO_DESCRIPTION,
        LAX_BOOL(HEAD_REPO_JSON.fork)                   AS REPO_IS_FORK,
        LAX_STRING(HEAD_REPO_JSON.created_at)           AS REPO_CREATED_AT,
        LAX_STRING(HEAD_REPO_JSON.updated_at)           AS REPO_UPDATED_AT,
        LAX_STRING(HEAD_REPO_JSON.pushed_at)            AS REPO_PUSHED_AT,
        LAX_STRING(HEAD_REPO_JSON.git_url)              AS REPO_GIT_URL,
        LAX_INT64(HEAD_REPO_JSON.stargazers_count)      AS REPO_STARGAZERS_COUNT,
        LAX_INT64(HEAD_REPO_JSON.watchers_count)        AS REPO_WATCHERS_COUNT,
        LAX_STRING(HEAD_REPO_JSON.language)             AS REPO_LANGUAGE,
        LAX_STRING(HEAD_REPO_JSON.has_issues)           AS REPO_HAS_ISSUES,
        LAX_STRING(HEAD_REPO_JSON.has_downloads)        AS REPO_HAS_DOWNLOADS,
        LAX_STRING(HEAD_REPO_JSON.has_wiki)             AS REPO_HAS_WIKI,
        LAX_STRING(HEAD_REPO_JSON.has_pages)            AS REPO_HAS_PAGES,
        LAX_INT64(HEAD_REPO_JSON.forks_count)           AS REPO_FORKS_COUNT,
        LAX_INT64(HEAD_REPO_JSON.open_issues_count)     AS REPO_OPEN_ISSUES_COUNT,
        LAX_INT64(HEAD_REPO_JSON.forks)                 AS REPO_FORKS,
        LAX_INT64(HEAD_REPO_JSON.open_issues)           AS REPO_OPEN_ISSUES,
        LAX_INT64(HEAD_REPO_JSON.watchers)              AS REPO_WATCHERS,
        LAX_STRING(HEAD_REPO_JSON.default_branch)       AS REPO_DEFAULT_BRANCH,
        CREATED_AT                                      AS UPDATED_AT,
        LOAD_TIMESTAMP

    FROM {{ ref('ODS_PULL_REQUESTS_REVIEW_COMMENT_EVENTS') }}
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(UPDATED_AT) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

), PULL_REQUEST_HEAD AS (
    
    SELECT
        LAX_INT64(HEAD_REPO_JSON.id)                    AS REPO_ID,
        LAX_STRING(HEAD_REPO_JSON.name)                 AS REPO_NAME,
        LAX_STRING(HEAD_REPO_JSON.full_name)            AS REPO_FULL_NAME,
        LAX_BOOL(HEAD_REPO_JSON.private)                AS REPO_IS_PRIVATE,
        LAX_STRING(HEAD_REPO_JSON.description)          AS REPO_DESCRIPTION,
        LAX_BOOL(HEAD_REPO_JSON.fork)                   AS REPO_IS_FORK,
        LAX_STRING(HEAD_REPO_JSON.created_at)           AS REPO_CREATED_AT,
        LAX_STRING(HEAD_REPO_JSON.updated_at)           AS REPO_UPDATED_AT,
        LAX_STRING(HEAD_REPO_JSON.pushed_at)            AS REPO_PUSHED_AT,
        LAX_STRING(HEAD_REPO_JSON.git_url)              AS REPO_GIT_URL,
        LAX_INT64(HEAD_REPO_JSON.stargazers_count)      AS REPO_STARGAZERS_COUNT,
        LAX_INT64(HEAD_REPO_JSON.watchers_count)        AS REPO_WATCHERS_COUNT,
        LAX_STRING(HEAD_REPO_JSON.language)             AS REPO_LANGUAGE,
        LAX_STRING(HEAD_REPO_JSON.has_issues)           AS REPO_HAS_ISSUES,
        LAX_STRING(HEAD_REPO_JSON.has_downloads)        AS REPO_HAS_DOWNLOADS,
        LAX_STRING(HEAD_REPO_JSON.has_wiki)             AS REPO_HAS_WIKI,
        LAX_STRING(HEAD_REPO_JSON.has_pages)            AS REPO_HAS_PAGES,
        LAX_INT64(HEAD_REPO_JSON.forks_count)           AS REPO_FORKS_COUNT,
        LAX_INT64(HEAD_REPO_JSON.open_issues_count)     AS REPO_OPEN_ISSUES_COUNT,
        LAX_INT64(HEAD_REPO_JSON.forks)                 AS REPO_FORKS,
        LAX_INT64(HEAD_REPO_JSON.open_issues)           AS REPO_OPEN_ISSUES,
        LAX_INT64(HEAD_REPO_JSON.watchers)              AS REPO_WATCHERS,
        LAX_STRING(HEAD_REPO_JSON.default_branch)       AS REPO_DEFAULT_BRANCH,
        CREATED_AT                                      AS UPDATED_AT,
        LOAD_TIMESTAMP

    FROM {{ ref('ODS_PULL_REQUEST_EVENTS') }}
    {% if is_incremental() -%}
    WHERE CREATED_AT > coalesce(
            (
            select max(UPDATED_AT) 
            from {{ this }} 
            ) 
        , '1900-01-01')
    {%- endif %}

), PULL_REQUEST_BASE AS (
    
    SELECT
        LAX_INT64(BASE_REPO_JSON.id)                    AS REPO_ID,
        LAX_STRING(BASE_REPO_JSON.name)                 AS REPO_NAME,
        LAX_STRING(BASE_REPO_JSON.full_name)            AS REPO_FULL_NAME,
        LAX_BOOL(BASE_REPO_JSON.private)                AS REPO_IS_PRIVATE,
        LAX_STRING(BASE_REPO_JSON.description)          AS REPO_DESCRIPTION,
        LAX_BOOL(BASE_REPO_JSON.fork)                   AS REPO_IS_FORK,
        LAX_STRING(BASE_REPO_JSON.created_at)           AS REPO_CREATED_AT,
        LAX_STRING(BASE_REPO_JSON.updated_at)           AS REPO_UPDATED_AT,
        LAX_STRING(BASE_REPO_JSON.pushed_at)            AS REPO_PUSHED_AT,
        LAX_STRING(BASE_REPO_JSON.git_url)              AS REPO_GIT_URL,
        LAX_INT64(BASE_REPO_JSON.stargazers_count)      AS REPO_STARGAZERS_COUNT,
        LAX_INT64(BASE_REPO_JSON.watchers_count)        AS REPO_WATCHERS_COUNT,
        LAX_STRING(BASE_REPO_JSON.language)             AS REPO_LANGUAGE,
        LAX_STRING(BASE_REPO_JSON.has_issues)           AS REPO_HAS_ISSUES,
        LAX_STRING(BASE_REPO_JSON.has_downloads)        AS REPO_HAS_DOWNLOADS,
        LAX_STRING(BASE_REPO_JSON.has_wiki)             AS REPO_HAS_WIKI,
        LAX_STRING(BASE_REPO_JSON.has_pages)            AS REPO_HAS_PAGES,
        LAX_INT64(BASE_REPO_JSON.forks_count)           AS REPO_FORKS_COUNT,
        LAX_INT64(BASE_REPO_JSON.open_issues_count)     AS REPO_OPEN_ISSUES_COUNT,
        LAX_INT64(BASE_REPO_JSON.forks)                 AS REPO_FORKS,
        LAX_INT64(BASE_REPO_JSON.open_issues)           AS REPO_OPEN_ISSUES,
        LAX_INT64(BASE_REPO_JSON.watchers)              AS REPO_WATCHERS,
        LAX_STRING(BASE_REPO_JSON.default_branch)       AS REPO_DEFAULT_BRANCH,
        CREATED_AT                                      AS UPDATED_AT,
        LOAD_TIMESTAMP

    FROM {{ ref('ODS_PULL_REQUEST_EVENTS') }}
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
    REPO_ID,
    REPO_NAME,
    REPO_FULL_NAME,
    REPO_IS_PRIVATE,
    REPO_DESCRIPTION,
    REPO_IS_FORK,
    REPO_CREATED_AT,
    REPO_UPDATED_AT,
    REPO_PUSHED_AT,
    REPO_GIT_URL,
    REPO_STARGAZERS_COUNT,
    REPO_WATCHERS_COUNT,
    REPO_LANGUAGE,
    REPO_HAS_ISSUES,
    REPO_HAS_DOWNLOADS,
    REPO_HAS_WIKI,
    REPO_HAS_PAGES,
    REPO_FORKS_COUNT,
    REPO_OPEN_ISSUES_COUNT,
    REPO_FORKS,
    REPO_OPEN_ISSUES,
    REPO_WATCHERS,
    REPO_DEFAULT_BRANCH,
    UPDATED_AT,
    LOAD_TIMESTAMP
FROM PULL_REQUEST_REVIEW_COMMENT_BASE
UNION ALL
SELECT
    REPO_ID,
    REPO_NAME,
    REPO_FULL_NAME,
    REPO_IS_PRIVATE,
    REPO_DESCRIPTION,
    REPO_IS_FORK,
    REPO_CREATED_AT,
    REPO_UPDATED_AT,
    REPO_PUSHED_AT,
    REPO_GIT_URL,
    REPO_STARGAZERS_COUNT,
    REPO_WATCHERS_COUNT,
    REPO_LANGUAGE,
    REPO_HAS_ISSUES,
    REPO_HAS_DOWNLOADS,
    REPO_HAS_WIKI,
    REPO_HAS_PAGES,
    REPO_FORKS_COUNT,
    REPO_OPEN_ISSUES_COUNT,
    REPO_FORKS,
    REPO_OPEN_ISSUES,
    REPO_WATCHERS,
    REPO_DEFAULT_BRANCH,
    UPDATED_AT,
    LOAD_TIMESTAMP
FROM PULL_REQUEST_REVIEW_COMMENT_HEAD
UNION ALL
SELECT
    REPO_ID,
    REPO_NAME,
    REPO_FULL_NAME,
    REPO_IS_PRIVATE,
    REPO_DESCRIPTION,
    REPO_IS_FORK,
    REPO_CREATED_AT,
    REPO_UPDATED_AT,
    REPO_PUSHED_AT,
    REPO_GIT_URL,
    REPO_STARGAZERS_COUNT,
    REPO_WATCHERS_COUNT,
    REPO_LANGUAGE,
    REPO_HAS_ISSUES,
    REPO_HAS_DOWNLOADS,
    REPO_HAS_WIKI,
    REPO_HAS_PAGES,
    REPO_FORKS_COUNT,
    REPO_OPEN_ISSUES_COUNT,
    REPO_FORKS,
    REPO_OPEN_ISSUES,
    REPO_WATCHERS,
    REPO_DEFAULT_BRANCH,
    UPDATED_AT,
    LOAD_TIMESTAMP
FROM PULL_REQUEST_HEAD
UNION ALL
SELECT
    REPO_ID,
    REPO_NAME,
    REPO_FULL_NAME,
    REPO_IS_PRIVATE,
    REPO_DESCRIPTION,
    REPO_IS_FORK,
    REPO_CREATED_AT,
    REPO_UPDATED_AT,
    REPO_PUSHED_AT,
    REPO_GIT_URL,
    REPO_STARGAZERS_COUNT,
    REPO_WATCHERS_COUNT,
    REPO_LANGUAGE,
    REPO_HAS_ISSUES,
    REPO_HAS_DOWNLOADS,
    REPO_HAS_WIKI,
    REPO_HAS_PAGES,
    REPO_FORKS_COUNT,
    REPO_OPEN_ISSUES_COUNT,
    REPO_FORKS,
    REPO_OPEN_ISSUES,
    REPO_WATCHERS,
    REPO_DEFAULT_BRANCH,
    UPDATED_AT,
    LOAD_TIMESTAMP
FROM PULL_REQUEST_BASE

)

SELECT
    *
FROM unioned
WHERE REPO_ID IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY REPO_ID ORDER BY UPDATED_AT DESC) = 1
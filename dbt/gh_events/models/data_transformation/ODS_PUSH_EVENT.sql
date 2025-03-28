{{
  config(
    materialized = 'view'
    )
}}


SELECT
    id              AS ID,
    repo.id         AS REPO_ID,
    repo.name       AS REPO_NAME,
    actor.id        AS ACTOR_ID,
    actor.login     AS ACTOR_LOGIN,
    CAST(json_extract(payload, "$.push_id")         AS STRING)      AS PUSH_ID,
    CAST(json_extract(payload, "$.size")            AS INT)         AS _SIZE,
    CAST(json_extract(payload, "$.distinct_size")   AS INT)         AS DISTINCT_SIZE,
    CAST(json_extract(payload, "$.before")          AS STRING)      AS _BEFORE,
    json_extract_array(json_extract(payload, "$.commits"))          AS COMMITS,
    CAST(json_extract(payload, "$.ref")             AS STRING)      AS REF,
    CAST(json_extract(payload, "$.head")            AS STRING)      AS HEAD,
    created_At       AS CREATED_AT
FROM {{ source('landing_zone', 'PushEvent') }}
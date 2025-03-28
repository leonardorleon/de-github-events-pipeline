{{
  config(
    materialized = 'view'
    )
}}

with flattened as (
    SELECT
        id as event_id,
        json_extract(payload, "$.action")    AS action,
        json_extract(payload, "$.locked")    AS locked,
        -- pull request
        json_extract(payload, "$.pull_request.id")    AS pull_reqeust_ID,
        json_extract(payload, "$.pull_request.changed_files")  AS changed_files,
        json_extract(payload, "$.pull_request.closed_at")  AS closed_at,
        json_extract(payload, "$.pull_request.comments")   AS comments,
        json_extract(payload, "$.pull_request.commits")    AS commits,
        json_extract(payload, "$.pull_request.created_at") AS created_at,
        json_extract(payload, "$.pull_request.deletions")  AS deletions,
        json_extract(payload, "$.pull_request.state") AS pull_request_state,
        json_extract(payload, "$.pull_request.title") AS pull_request_title,
        json_extract(payload, "$.pull_request.user")  AS pull_request_user,
        -- base repo
        json_extract(payload, "$.pull_request.base.repo.fork")    AS base_repo_fork,
        json_extract(payload, "$.pull_request.base.repo.forks")    AS base_repo_forks,
        json_extract(payload, "$.pull_request.base.repo.forks_count")    AS base_repo_forks_count,
        json_extract(payload, "$.pull_request.base.repo.full_name")    AS base_repo_full_name,
        json_extract(payload, "$.pull_request.base.repo.has_downloads")  as BASE_REPO_has_downloads,
        json_extract(payload, "$.pull_request.base.repo.has_issues") as BASE_REPO_has_issues,
        json_extract(payload, "$.pull_request.base.repo.has_pages")  as BASE_REPO_has_pages,
        json_extract(payload, "$.pull_request.base.repo.has_wiki")   as BASE_REPO_has_wiki,
        json_extract(payload, "$.pull_request.base.repo.id")   as BASE_REPO_id,
        json_extract(payload, "$.pull_request.base.repo.language")   as BASE_REPO_language,
        json_extract(payload, "$.pull_request.base.repo.name")   as BASE_REPO_name,
        json_extract(payload, "$.pull_request.base.repo.open_issues")   AS base_repo_open_issues,
        json_extract(payload, "$.pull_request.base.repo.open_issues_count") AS base_repo_open_issues_count,
        json_extract(payload, "$.pull_request.base.repo.owner.login")   AS base_repo_owner_login,
        json_extract(payload, "$.pull_request.base.repo.private")   AS base_repo_private,
        json_extract(payload, "$.pull_request.base.repo.stargazers_count")   AS base_repo_stargazers_count,
        json_extract(payload, "$.pull_request.base.repo.watchers_count")   AS base_repo_watchers_count,
        -- head repo
        json_extract(payload, "$.pull_request.head.repo.fork")    AS head_repo_fork,
        json_extract(payload, "$.pull_request.head.repo.forks")    AS head_repo_forks,
        json_extract(payload, "$.pull_request.head.repo.forks_count")    AS head_repo_forks_count,
        json_extract(payload, "$.pull_request.head.repo.full_name")    AS head_repo_full_name,
        json_extract(payload, "$.pull_request.head.repo.has_downloads")  as head_REPO_has_downloads,
        json_extract(payload, "$.pull_request.head.repo.has_issues") as head_REPO_has_issues,
        json_extract(payload, "$.pull_request.head.repo.has_pages")  as head_REPO_has_pages,
        json_extract(payload, "$.pull_request.head.repo.has_wiki")   as head_REPO_has_wiki,
        json_extract(payload, "$.pull_request.head.repo.id")   as head_REPO_id,
        json_extract(payload, "$.pull_request.head.repo.language")   as head_REPO_language,
        json_extract(payload, "$.pull_request.head.repo.name")   as head_REPO_name,
        json_extract(payload, "$.pull_request.head.repo.open_issues")   AS head_repo_open_issues,
        json_extract(payload, "$.pull_request.head.repo.open_issues_count") AS head_repo_open_issues_count,
        json_extract(payload, "$.pull_request.head.repo.owner.login")   AS head_repo_owner_login,
        json_extract(payload, "$.pull_request.head.repo.private")   AS head_repo_private,
        json_extract(payload, "$.pull_request.head.repo.stargazers_count")   AS head_repo_stargazers_count,
        json_extract(payload, "$.pull_request.head.repo.watchers_count")   AS head_repo_watchers_count,
        -- merged by 
        json_extract(payload, "$.pull_request.merged_by.id")  AS  merged_by_id,
        json_extract(payload, "$.pull_request.merged_by.login")   AS  merged_by_login,
        json_extract(payload, "$.pull_request.merged_by.type")    AS merged_by_type,
    FROM {{ source('landing_zone', 'PushEvent') }}

)

select * from flattened
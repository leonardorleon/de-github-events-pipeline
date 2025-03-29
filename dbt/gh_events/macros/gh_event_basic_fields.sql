

{% macro gh_event_main_fields(final=false) %}
    id      AS ID,
    type    AS EVENT_TYPE,
    public  AS PUBLIC,
    created_at  AS CREATED_AT {{ "," if not final }}
{% endmacro %}


{% macro gh_event_repo_fields(final=false) %}
    repo.id     AS REPO_ID,
    repo.name   AS REPO_NAME,
    repo.url    AS REPO_URL {{ "," if not final }}
{% endmacro %}


{% macro gh_event_actor_fields(final=false) %}
    actor.id        AS ACTOR_ID,
    actor.login     AS ACTOR_LOGIN,
    actor.url       AS ACTOR_URL {{ "," if not final }}
{% endmacro %}

{% macro gh_event_org_fields(final=false) %}
    org.id        AS ORG_ID,
    org.login     AS ORG_LOGIN,
    org.url       AS ORG_URL {{ "," if not final }}
{% endmacro %}


{% macro get_event_type(model) -%}
    {%- set model_to_event_mapping = {
        "ODS_COMMIT_COMMENT_EVENTS": "CommitCommentEvent",
        "ODS_CREATE_EVENTS": "CreateEvent",
        "ODS_DELETE_EVENTS": "DeleteEvent",
        "ODS_FORK_EVENTS": "ForkEvent",
        "ODS_GOLLUM_EVENTS": "GollumEvent",
        "ODS_ISSUE_COMMENT_EVENTS": "IssueCommentEvent",
        "ODS_ISSUES_EVENTS": "IssuesEvent",
        "ODS_MEMBER_EVENTS": "MemberEvent",
        "ODS_PUBLIC_EVENTS": "PublicEvent",
        "ODS_PULL_REQUEST_EVENTS": "PullRequestEvent",
        "ODS_PULL_REQUEST_REVIEW_COMMENT_EVENTS": "PullRequestReviewCommentEvent",
        "ODS_PUSH_EVENTS": "PushEvent",
        "ODS_RELEASE_EVENTS": "ReleaseEvent",
        "ODS_WATCH_EVENTS": "WatchEvent"
    } -%}

    {{ model_to_event_mapping.get(model.name, "UnknownEvent") }}
{%- endmacro %}
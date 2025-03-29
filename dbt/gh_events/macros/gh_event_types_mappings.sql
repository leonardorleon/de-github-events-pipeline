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
        "ODS_PULL_REQUESTS_REVIEW_COMMENT_EVENTS": "PullRequestReviewCommentEvent",
        "ODS_PUSH_EVENTS": "PushEvent",
        "ODS_RELEASE_EVENTS": "ReleaseEvent",
        "ODS_WATCH_EVENTS": "WatchEvent"
    } -%}

    {{ model_to_event_mapping.get(model, "UnknownEvent") }}
{%- endmacro %}
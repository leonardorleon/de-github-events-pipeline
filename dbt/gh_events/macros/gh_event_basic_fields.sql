

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
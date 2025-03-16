# de-github-events-pipeline

This project is part of DataTalkClub's data engineering zoomcamp. [click here to visit the repo](https://github.com/DataTalksClub/data-engineering-zoomcamp/)


# Problem Statement

The objective of this project is to create a data pipeline using data from [github archive](https://www.gharchive.org/). This archive is an open source dataset to record the public github timeline so it can be easily accessed by developers for analysis. 

The goal is to generate a dataset from the info retrieved out of the archive and generate a dashboard to obtain some insights about the general activity in github.

# Set up

## Environment

Since this project will use terraform, kestra, dbt, GCS and BigQuery, in order to make it easily reproducible I am preparing a bash file to generate the environment variables that would be used by the different systems. [env_setup.sh](env_setup.sh).

This file only has a few elements that you would need to edit manually, although you don't have to:

```bash
#--------- To fill manually ------------#
project_name=gh_events
project_region=EUROPE-WEST1
bucket_storage_class=STANDARD
credentials_path=~/.google/credentials/google_credentials.json
```

The goal of the script is twofold. It takes the project ID directly from your google credentials to avoid mistakes, and generates unique identifiers for various resources. Finally:

* it creates a `.tfvars` file so terraform can use this info directly in its variables
* it creates a `.env` file which can be passed to kestra to be used in connections with the gcp resources

For kestra, I've added a couple of extra variables which are not needed for testing, such as the github username and the github token. These are used to commit kestra flows to github using a kestra flow. 

* Note: The github token is passed to kestra as a secret, so it is expecting a base64 encoded value. To obtain it simply run:

    `echo -n "place_github_token_here" | base64`

This will return the value that should be passed to `SECRET_GITHUB_ACCESS_TOKEN` in [env_setup.sh](env_setup.sh)

## Docker set up

The [docker-compose.yml](docker-compose.yml) generates a few services:

* a postgres db to be used by kestra for its own purposes
* a kestra container which will be used to create flows and data orchestration

## Make 

I've also created a [Makefile](Makefile) to prepare some of the most commonly used commands to make it simple to execute.

To learn more about the available commands, use `make help`

## Kestra

Beyond the docker-compose setup to run the container, we can set up a couple more things:

1. [00_gcp_kv.yml](kestra/00_gcp_kv.yml): This flow is used to create a key-value store inside kestra, so that we can use those values in flows through `"{{kv('GCP_PROJECT_ID')}}"`. We could potentially use environment variables directly with `"{{ envs.gcp_creds }}"`, but it demonstrates how to use the key value store.

2. [00_push_to_git.yml](kestra/00_push_to_git.yml): This flow shows how to push kestra flows to github directly from kestra. This way we can better work with version control for whatever we develop in the kestra server.
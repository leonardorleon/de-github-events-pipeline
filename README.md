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

The [docker-compose.yml](docker_setup/docker-compose.yml) generates a few services:

* a postgres db to be used by kestra for its own purposes
* a kestra container which will be used to create flows and data orchestration

## Make 

I've also created a [Makefile](Makefile) to prepare some of the most commonly used commands to make it simple to execute.

To learn more about the available commands, use `make help`

## Kestra

Beyond the docker-compose setup to run the container, we can set up a couple more things:

1. [00_gcp_kv.yml](kestra/00_gcp_kv.yml): This flow is used to create a key-value store inside kestra, so that we can use those values in flows through `"{{kv('GCP_PROJECT_ID')}}"`. We could potentially use environment variables directly with `"{{ envs.gcp_creds }}"`, but it demonstrates how to use the key value store.

2. [00_push_to_git.yml](kestra/00_push_to_git.yml): This flow shows how to push kestra flows to github directly from kestra. This way we can better work with version control for whatever we develop in the kestra server.


## DBT

To develop on dbt, I've added the service to the docker-compose.yml file, as well as a dockerfile with the instructions on how to create the image. Besides that, before initializing the dbt project, we need a profiles.yml file, which is often in `~/.dbt/profiles.yml`. This will vary person to person, but it should look something like this:

```bash
bq-dbt-project:
  target: dev
  outputs:
    dev:
      dataset: gh_events_dataset
      fixed_retries: 1
      keyfile: /.google/credentials/google_credentials.json
      location: europe-west1
      method: service-account
      #priority: interactive
      project: sunlit-amulet-341719
      threads: 4
      timeout_seconds: 300
      type: bigquery
    prod:
      dataset: gh_events_dataset_prod
      fixed_retries: 1
      keyfile: /.google/credentials/google_credentials.json
      location: europe-west1
      method: service-account
      #priority: interactive
      project: sunlit-amulet-341719
      threads: 4
      timeout_seconds: 300
      type: bigquery
```

To get the dev environment set up, follow these actions (keep in mind at this point the entrypoint in the dockerfile is `dbt`):

* `docker-compose -f docker_setup/docker-compose.yml build`
* `docker-compose -f docker_setup/docker-compose.yml run dbt-bq init`

  After this command, dbt will ask a few questions. The most importants are
    
    * project name: gh_events
    * profile name: bq-dbt-project

If you already have an existing profile which you want to use, simply go to [dbt_project.yml](dbt/gh_events/dbt_project.yml) and set your profile there: `profile: 'bq-dbt-project'`

Now, there are a few ways to make the container work. One of them is the following command:

* `docker-compose -f docker_setup/docker-compose.yml run --workdir="//usr/app/dbt/gh_events" dbt-bq dbt debug`

However, this is a bit cumbersome, so I've decided to just run the container in interactive mode, that way I don't need to run a long command everytime and spin up the container each time I want to run a command. 

### Run in interactive mode

At this point, I've set up the entrypoint in the dockerfile to `bash`, so we need to build the container again `docker-compose -f docker_setup/docker-compose.yml build`

and we can run the container by running `docker-compose -f docker_setup/docker-compose.yml run -it --workdir="//usr/app/dbt/gh_events" dbt-bq`

This will open the container in interactive mode and keep it open, so we can simply run dbt commands there, such as `dbt debug` to confirm it all works properly.

We can of course, access the container again once it is running by running `docker exec -it [container id] sh`

To make it even easier, I'm setting up a make command to start the dbt container in the [Makefile](Makefile)


### Facilitate working with DBT core

The VScode dbt-power-user extension has many benefits when developing with dbt, and this can be used despite working on a container. We need but two things:

* On the host computer, install the VScode extension: `Dev containers`
* use `ctrl+shift+p` to find vscode commands and look for: `Dev containers: Attach to running container` to attach to the container running dbt. This will open a new vscode window with the full vscode functionality from the container
* In the container window, install `dbt-power-user` extension from the vscode ui. In my case it didn't work out of the box, but that is due to a known issue which can be solved by:
    * running `dbt debug` and finding the location of the python path that dbt uses. In my case it was: `python path: /usr/local/bin/python`
    * In the vscode of the dbt container, select the python interpreter and choose a path, give it the one for dbt `/usr/local/bin/python`. After this, you should be able to use dbt power user features, including the lineage and easier model execution and documentation.

![](images/00_example_dbt_lineage.png)


So in short, the workflow is:

* make up: will start kestra in the background. You can develop on the UI or simply let it work on the active triggers on it's own or via backfill
* make dbt: will start the dbt container, which you can either attach to on the terminal or attach through vscode to use the dbt power user extension
* make stop: will stop the containers in the docker-compose.yml


### DBT Sources

To get started with dbt, let's set up the sources which are ingested through kestra data orchestration. First, let's create a [sources.yml](dbt/gh_events/models/sources.yml) in the root of the models folder. Here we can either set up the databaset (project-id when working on bigquery) and the schema (dataset name when working with bigquery) via environment variables, or when it's not there by the backup. change it up if needed.


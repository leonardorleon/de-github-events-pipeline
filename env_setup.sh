#!/bin/bash

# To make this script executable, run:
# chmod +x env_setup.sh

: ' 
This script is meant to generate the necessary environment 
variables for the project out of the GCP credentials file
'

#--------- To fill manually ------------#
project_name=gh_events
project_region=EUROPE-WEST1
bucket_storage_class=STANDARD
credentials_path=~/.google/credentials/google_credentials.json


# only used if pushing kestra flows to github from the kestra server
github_token_path=~/.github/github_tokenb64.txt     # Make sure the token inside the file is base64
github_username_path=~/.github/github_username.txt

# Extract the project id from the gcp credentials and generate unique identifiers
echo "Reading gcp credentials and creating unique resource identifiers"

project_id=$(jq -r .project_id $credentials_path)
data_lake_bucket="${project_id}_${project_name}_bucket"
region=$project_region
storage_class=$bucket_storage_class
bq_dataset="${project_name}_dataset"

# Create a tfvars file for terraform environment variables
echo "Generatign terraform .tfvars file"

echo "project=\"$project_id\"" > terraform/dev.auto.tfvars
echo "bucket=\"$data_lake_bucket\"" >> terraform/dev.auto.tfvars
echo "region=\"$region\"" >> terraform/dev.auto.tfvars
echo "storage_class=\"$storage_class\"" >> terraform/dev.auto.tfvars
echo "bq_dataset=\"$bq_dataset\"" >> terraform/dev.auto.tfvars


# Create an env file that can be used by kestra
echo "Generatign Kestra environment file"

credentials_content=$(cat $credentials_path)
github_token=$(cat $github_token_path)  
github_user=$(cat $github_username_path)

echo "# service account credentials for gcp" > env_file.env
echo "KESTRA_GCP_CREDS='$credentials_content'" >> env_file.env

echo "# rest of credentials" >> env_file.env
echo "KESTRA_GCP_PROJECT_ID=$project_id" >> env_file.env
echo "KESTRA_GCP_REGION=$region" >> env_file.env
echo "KESTRA_GCP_BUCKET_NAME=$data_lake_bucket" >> env_file.env
echo "KESTRA_BQ_DATASET=$bq_dataset" >> env_file.env
echo "KESTRA_GITHUB_USERNAME=$github_user" >> env_file.env
echo "SECRET_GITHUB_ACCESS_TOKEN=$github_token" >> env_file.env 
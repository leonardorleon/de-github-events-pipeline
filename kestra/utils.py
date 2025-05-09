import logging
import json
from google.cloud import storage, bigquery


def create_table_with_json_schema(project_id,
                                  dataset_id,
                                  table_id,
                                  schema_file_path,
                                  delete_if_exists=False,
                                  partition_field=None):
    """
    Creates a BigQuery table using a JSON schema file, with options to delete the table if it already exists,
    to partition the table, and to add an audit field ("load_timestamp").

    Args:
        project_id (str): The GCP project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.
        schema_file_path (str): The path to the JSON schema file.
        delete_if_exists (bool): Whether to delete the table if it already exists.
        partition_field (str): The field to use for partitioning the table. If None, the table will not be partitioned.

    Returns:
        None
    """
    client = bigquery.Client(project=project_id)

    table_ref = client.dataset(dataset_id).table(table_id)

    if delete_if_exists:
        client.delete_table(table_ref, not_found_ok=True)
        logging.info(f"Deleted table {table_id} if it existed.")

    schema = client.schema_from_json(schema_file_path)
    
    # Add the "load_timestamp" field to the schema for an audit field
    load_timestamp_field = bigquery.SchemaField("load_timestamp", "TIMESTAMP", mode="NULLABLE")
    schema.append(load_timestamp_field)

    table = bigquery.Table(table_ref, schema=schema)

    # Add partitioning if a partition field is provided
    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(field=partition_field)
        logging.info(f"Table {table_id} will be partitioned by {partition_field}.")

    client.create_table(table, exists_ok=True)

    logging.info(f"Created table {table_id} in dataset {dataset_id} using schema from {schema_file_path}")


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """
    Uploads a file to Google Cloud Storage.

    Args:
        bucket_name (str): The name of the GCS bucket.
        source_file_name (str): The local path to the file to upload.
        destination_blob_name (str): The destination path in the GCS bucket.

    Returns:
        None
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    logging.info(f"File {source_file_name} uploaded to {destination_blob_name}.")


def create_external_table(project_id, dataset_id, table_id, gcs_uri, source_format, schema_file_path):
    """
    Creates or replaces an external table in BigQuery using a schema file and a GCS URI as the data source.

    Args:
        project_id (str): The GCP project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.
        gcs_uri (str): The GCS URI of the external data source.
        source_format (str): The format of the external data (e.g., "CSV", "NEWLINE_DELIMITED_JSON").
        schema_file_path (str): The path to the JSON schema file.

    Returns:
        None
    """
    client = bigquery.Client(project=project_id)

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    external_config = bigquery.ExternalConfig(source_format)
    external_config.source_uris = [gcs_uri]
    external_config.autodetect = True
    # schema = prepare_schema(schema_file_path)
    schema = client.schema_from_json(schema_file_path)

    table = bigquery.Table(table_ref, schema=schema)
    table.external_data_configuration = external_config

    client.delete_table(table_ref, not_found_ok=True)  # Delete the table if it exists
    client.create_table(table)  # Create the table

    logging.info(f"Created or replaced external table {table_id} in dataset {dataset_id}")


def get_fields_from_schema(schema_file_path):
    """
    Extracts field names from a JSON schema file.

    Args:
        schema_file_path (str): The path to the JSON schema file.

    Returns:
        list: A list of field names extracted from the schema.
    """
    with open(schema_file_path, 'r') as schema_file:
        schema_json = json.load(schema_file)

    fields = []
    for field in schema_json:
        fields.append(field.get("name"))
    
    return fields


def perform_merge_statement(project_id, dataset_id, source_table_id, target_table_id, unique_key, schema_file_path):
    """
    Executes a dynamic merge statement in BigQuery to merge data from a source table into a target table.

    Args:
        project_id (str): The GCP project ID.
        dataset_id (str): The BigQuery dataset ID.
        source_table_id (str): The source BigQuery table ID.
        target_table_id (str): The target BigQuery table ID.
        unique_key (str): The unique key column for the merge operation.
        schema_file_path (str): The path to the JSON schema file used to determine column names.

    Returns:
        str: The executed merge statement.
    """
    client = bigquery.Client(project=project_id)

    # Get column names from the source table
    column_names = get_fields_from_schema(schema_file_path)

    update_string = ', '.join([f'T.{col} = S.{col}' for col in column_names])
    update_string = f"{update_string}, T.load_timestamp = CURRENT_TIMESTAMP()"

    insert_string = ', '.join(column_names)
    insert_string = f"{insert_string}, load_timestamp"

    values_string = ', '.join([f'S.{col}' for col in column_names])
    values_string = f"{values_string}, CURRENT_TIMESTAMP()"

    # Create the merge statement
    merge_statement = f"""
    MERGE `{project_id}.{dataset_id}.{target_table_id}` T
    USING `{project_id}.{dataset_id}.{source_table_id}` S
    ON T.{unique_key} = S.{unique_key}
    WHEN MATCHED THEN
      UPDATE SET {update_string}
    WHEN NOT MATCHED THEN
      INSERT ({insert_string})
      VALUES ({values_string});
    """

    logging.info("Performing merge statement...")

    query_job = client.query(merge_statement)
    query_job.result()  # Wait for the job to complete

    logging.info("Merge statement completed...")

    return merge_statement
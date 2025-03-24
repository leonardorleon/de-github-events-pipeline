from google.cloud import storage
from google.cloud import bigquery


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """
    Uploads a file to Google Cloud Storage.

    Args:
        bucket_name (str): The name of the GCS bucket.
        source_file_name (str): The path to the file to upload.
        destination_blob_name (str): The destination path in the GCS bucket.

    Returns:
        None
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")


def create_external_table(project_id, dataset_id, table_id, gcs_uri, source_format):
    """
    Creates or replaces an external table in BigQuery. For this use case, autodetect is set to True.

    Args:
        project_id (str): The GCP project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.
        gcs_uri (str): The GCS URI of the external data source.

    Returns:
        None
    """
    client = bigquery.Client(project=project_id)

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    external_config = bigquery.ExternalConfig(source_format)
    external_config.source_uris = [gcs_uri]
    external_config.autodetect = True

    table = bigquery.Table(table_ref)
    table.external_data_configuration = external_config

    client.delete_table(table_ref, not_found_ok=True)  # Delete the table if it exists
    client.create_table(table)  # Create the table

    print(f"Created or replaced external table {table_id} in dataset {dataset_id}")


def create_staging_table(project_id, dataset_id, table_id):
    """
    Creates a BigQuery table if it doesn't exist using the schema from an external table of the same name and suffix "_ext".

    Args:
        project_id (str): The GCP project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.

    Returns:
        None
    """
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)

    query = f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{table_id}`
    PARTITION BY DATE(created_at)
    AS SELECT * FROM `{project_id}.{dataset_id}.{table_id}_ext` WHERE 1=0;
    """
    query_job = client.query(query)
    query_job.result()  # Wait for the job to complete
    print(f"Ensured {table_id} exists in BigQuery")


def get_column_names(client, project_id, dataset_id, table_id):
    """
    Retrieves the column names from a BigQuery table.

    Args:
        client (bigquery.Client): The BigQuery client.
        project_id (str): The GCP project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.

    Returns:
        list: A list of column names.
    """
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    return [schema_field.name for schema_field in table.schema]


def perform_merge_statement(project_id, dataset_id, source_table_id, target_table_id, unique_key):
    """
    Prepares a dynamic merge statement between the source and target tables.

    Args:
        project_id (str): The GCP project ID.
        dataset_id (str): The BigQuery dataset ID.
        source_table_id (str): The source BigQuery table ID.
        target_table_id (str): The target BigQuery table ID.
        unique_key (str): The unique key column for the merge operation.

    Returns:
        str: The dynamic merge statement.
    """
    client = bigquery.Client(project=project_id)

    # Get column names from the source table
    column_names = get_column_names(client, project_id, dataset_id, source_table_id)

    update_string = ', '.join([f'T.{col} = S.{col}' for col in column_names])
    insert_string = ', '.join(column_names)
    values_string = ', '.join([f'S.{col}' for col in column_names])

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

    query_job = client.query(merge_statement)
    query_job.result()  # Wait for the job to complete

    return merge_statement
import json
import os
import argparse
from kestra import Kestra
from utils import upload_to_gcs, create_external_table, perform_merge_statement, create_table_with_json_schema


def extract_events_from_json(file_path):
    """
    Reads a Github events JSON file where each line is a separate JSON object and returns a list of events.

    Args:
        file_path (str): The path to the JSON file to be read.

    Returns:
        list: A list of events, where each event is a dictionary representing a JSON object.
    """
    events = []
    logger.info(f"Reading file: {file_path} and generating events array")
    with open(file_path, 'r') as file:
        for line in file:
            events.append(json.loads(line))

    logger.info(f"Events array generated")
    return events


def extract_events_by_type(events, output_dir, file_name):
    """
    Takes an array of events and separates them according to their event type. 
    It writes them to a directory for their corresponding event type.

    Args:
        events (list): A list of events, where each event is a dictionary representing a JSON object.
        output_dir (str): The path to the directory where the output files should be saved.
        file_name (str): The date and hour string extracted from the input file name to be used in the output file names.

    Returns:
        an array containing the paths for the processed files
    """
    if not os.path.exists(output_dir):
        logger.info(f"Output dir does not exist. Creating: {output_dir}")
        os.makedirs(output_dir)
    
    logger.info(f"Looping through events and classifying them by event type")

    events_by_type = {}
    for event in events:
        event_type = event.get('type')
        # initiate the array for each event type key as they appear
        if event_type not in events_by_type:
            events_by_type[event_type] = []
        
        # Process payload, clean empty payloads and others set to string
        if isinstance(event.get("payload"), dict):
            if not event["payload"]:
                event["payload"] = None
            else:
                event["payload"] = json.dumps(event["payload"])
        
        # Clean "other" in the same way as payload
        if isinstance(event.get("other"), dict):
            if not event["other"]:
                event["other"] = None
            else:
                event["other"] = json.dumps(event["other"])

        events_by_type[event_type].append(event)
    
    logger.info(f"Events properly classified. Writing to their respective directories")

    event_type_paths = []
    for i, (event_type, events) in enumerate(events_by_type.items()):      
        type_dir = os.path.join(output_dir, event_type)

        if not os.path.exists(type_dir):
            logger.info(f"{type_dir} does not exist. Creating directory")
            os.makedirs(type_dir)

        event_type_file = f'{file_name}-{event_type}.json'
        
        event_type_file_path = os.path.join(type_dir, event_type_file)

        with open(event_type_file_path, 'w') as file:
            logger.info(f"Writing {event_type_file} to {type_dir}")
            for event in events:
                file.write(json.dumps(event) + '\n')

        # Determine the file mode: 'w' for the first iteration, 'a' for subsequent iterations
        mode = 'w' if i == 0 else 'a'
        with open(os.path.join(output_dir,'last_processed_files.txt'), mode ) as file:
            file.write(f"{event_type_file_path}\n")

        event_type_paths.append(event_type_file_path)

    logger.info(f"All files processed!")
    return event_type_paths


def main(input_file, output_dir):
    """
    Processes a GitHub events archive JSON file and separates events by event type into the specified output directory.

    Args:
        input_file (str): The path to the GitHub events archive JSON file.
        output_dir (str): The path to the directory where the output files should be saved.

    Returns:
        None
    """

    # Define values from environment variables
    bucket_name = os.getenv('GCP_BUCKET_NAME')
    if not bucket_name:
        raise ValueError("GCP_BUCKET_NAME environment variable is not set")
    
    project_id = os.getenv('GCP_PROJECT_ID')
    if not project_id:
        raise ValueError("GCP_PROJECT_ID environment variable is not set")
    
    dataset = os.getenv('GCP_DATASET')
    if not dataset:
        raise ValueError("GCP_DATASET environment variable is not set")

    schema_file_path = "kestra/schema.json"

    logger.info("STEP 1: Extract events")
    events = extract_events_from_json(input_file)

    file_name = input_file.split('.')[0]
    file_date = file_name.rsplit('-',1)[0]

    logger.info("STEP 2: Classify events by their type")
    event_paths = extract_events_by_type(events, output_dir, file_name)


    logger.info("STEP 3: Go through each event type, upload to datalake and ingest to BigQuery")
    for path in event_paths:
        base_dir, event_type, filename = path.split("/")

        # if event_type != "PullRequestEvent":
        #     continue

        # Upload data to data lake
        destination_blob_name = f"{base_dir}/{file_date}/{filename}"
        upload_to_gcs(bucket_name, path, destination_blob_name)

        logger.info(f"Uploaded {event_type} to {destination_blob_name} in GCS")

        # Create external table
        gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"
        ext_table_id = f"{event_type}_ext"
        config = "NEWLINE_DELIMITED_JSON" 

        create_external_table(project_id, 
                              dataset, 
                              ext_table_id, 
                              gcs_uri, 
                              config, 
                              schema_file_path=schema_file_path) # TODO: replace

        logger.info(f"Created {ext_table_id} from {gcs_uri} in GCS")

        # Ensure main tables exist
        # create_staging_table(project_id, dataset, event_type) # TODO: replace

        create_table_with_json_schema(project_id=project_id,
                                      dataset_id=dataset,
                                      table_id=event_type,
                                      schema_file_path=schema_file_path,
                                      delete_if_exists=False,
                                      partition_field="created_at")


        logger.info(f"Staging table {event_type} exists")

        # Perform a merge into the staging table
        perform_merge_statement(project_id=project_id
                                ,dataset_id=dataset
                                ,source_table_id=ext_table_id
                                ,target_table_id=event_type
                                ,unique_key="id"
                                ,schema_file_path=schema_file_path)


if __name__ == "__main__":
    
    logger = Kestra.logger()

    parser = argparse.ArgumentParser(description="This script processes an input file from github archive and separates events by event type in the location of the output directory")

    parser.add_argument('-i','--input_file', type=str, required=True, help="Path to the github events archive JSON")
    parser.add_argument('-o','--output_dir', type=str, required=True, help="Path to the directory where the outputs should be saved")

    args = parser.parse_args()

    input_file = args.input_file
    output_dir = args.output_dir

    logger.info(f"Processing events from: {input_file}, separating event types and saving to directory: {output_dir}/")

    main(input_file=input_file, output_dir=output_dir)

import json
import os
import argparse
from kestra import Kestra


def read_json_file(file_path):
    """
    Reads a JSON file where each line is a separate JSON object and returns a list of events.

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


def write_events_by_type(events, output_dir, file_date):
    """
    Takes an array of events and separates them according to their event type. 
    It writes them to a directory for their corresponding event type

    Args:
        file_path (str): The path to the JSON file to be read.

    Returns:
        None
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
        events_by_type[event_type].append(event)
    
    logger.info(f"Events properly classified. Writing to their respective directories")

    files_processed = []
    for event_type, events in events_by_type.items():
        type_dir = os.path.join(output_dir, event_type)

        if not os.path.exists(type_dir):
            logger.info(f"{type_dir} does not exist. Creating directory")
            os.makedirs(type_dir)

        event_type_file = f'{file_date}-{event_type}.json'
        
        event_type_file_path = os.path.join(type_dir, event_type_file)

        files_processed.append(event_type_file_path)

        with open(event_type_file_path, 'w') as file:
            logger.info(f"Writing {event_type_file} to {type_dir}")
            json.dump(events, file, indent=4)

    logger.info(f"All files processed, writing list of files processed this run")
    with open(os.path.join(output_dir,'last_processed_files.txt') , 'w') as file:
        for file_path in files_processed:
            file.write(f"{file_path}\n")


def main(input_file, output_dir):
    """
    Processes a GitHub events archive JSON file and separates events by event type into the specified output directory.

    Args:
        input_file (str): The path to the GitHub events archive JSON file.
        output_dir (str): The path to the directory where the output files should be saved.

    Returns:
        None
    """
    events = read_json_file(input_file)

    file_date = input_file.split('.')[0]

    write_events_by_type(events, output_dir, file_date)

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

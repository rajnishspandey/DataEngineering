import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

# Set paths
TMPFILE = "temp.tmp"
LOGFILE = "logfile.txt"
TARGETFILE = "transformed_data.csv"

# Extract
# CSV Extract Function
def extract_from_csv(file_to_process):
    try:
        dataframe = pd.read_csv(file_to_process)
    except Exception as e:
        log(f"Error reading CSV file {file_to_process}: {e}")
        return None

    return dataframe

# JSON Extract Function
def extract_from_json(file_to_process):
    try:
        dataframe = pd.read_json(file_to_process, lines=True)
    except Exception as e:
        log(f"Error reading JSON file {file_to_process}: {e}")
        return None

    return dataframe

# XML Extract Function
def extract_from_xml(file_to_process):
    try:
        tree = ET.parse(file_to_process)
        root = tree.getroot()

        dataframe = pd.DataFrame(columns=["name", "height", "weight"])
        for person in root:
            name = person.find("name").text
            height = float(person.find("height").text)
            weight = float(person.find("weight").text)

            dataframe = dataframe.append({"name": name, "height": height, "weight": weight}, ignore_index=True)
    except Exception as e:
        log(f"Error reading XML file {file_to_process}: {e}")
        return None

    return dataframe

# Extract Function
def extract():
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])

    # Process all CSV files
    for csvfile in glob.glob("./source/*.csv"):
        extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)])

    # Process all JSON files
    for jsonfile in glob.glob("./source/*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)])

    # Process all XML files
    for xmlfile in glob.glob("./source/*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)])

    return extracted_data

# Transform
def transform(data):
    # Convert height which is in inches to millimeter
    data["height"] = data["height"] * 25.4

    # Convert weight which is in pounds to kilograms
    data["weight"] = data["weight"] * 0.45359237

    return data

# Load
def load(targetfile, data_to_load):
    try:
        data_to_load.to_csv(targetfile, index=False)
    except Exception as e:
        log(f"Error loading data to CSV file {targetfile}: {e}")

# Logging
def log(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open(LOGFILE, "a") as f:
        f.write(f"{timestamp},{message}\n")

# Running ETL process
log("ETL Job Started")

log("Extract phase Started")
extracted_data = extract()
log("Extract phase Ended")

log("Transform phase Started")
transformed_data = transform(extracted_data)
log("Transform phase Ended")

log("Load phase Started")
load(TARGETFILE, transformed_data)
log("Load phase Ended")

log("ETL Job Ended")

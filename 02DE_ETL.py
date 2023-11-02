import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

TMPFILE    = "dealership_temp.tmp"               # file used to store all extracted data
LOGFILE    = "dealership_logfile.txt"            # all event logs will be stored in this file
TARGETFILE = "dealership_transformed_data.csv"   # file where transformed data is stored

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

        dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price","fuel"])
        for car in root:
            car_model = car.find("car_model").text
            year_of_manufacture = int(car.find("year_of_manufacture").text)
            price = float(car.find("price").text)
            fuel = car.find("fuel").text

            dataframe = dataframe.append({"car_model": car_model, "year_of_manufacture": year_of_manufacture, "price": price,"fuel":fuel}, ignore_index=True)
    except Exception as e:
        log(f"Error reading XML file {file_to_process}: {e}")
        return None

    return dataframe

# Extract Function
def extract():
    extracted_data = pd.DataFrame(columns=['car_model','year_of_manufacture','price', 'fuel']) # create an empty data frame to hold extracted data
    
    #process all csv files
    for csvfile in glob.glob("./datasource/*.csv"):
        #extracted_data = extracted_data.append(extracted_data, ignore_index=True) #'ADD_FUNCTION_CALL'
        extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)])

    #process all json files
    for jsonfile in glob.glob("./datasource/*.json"):
        #extracted_data = extracted_data.append(extracted_data, ignore_index=True)
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)])

    #process all xml files
    for xmlfile in glob.glob("./datasource/*.xml"):
        #extracted_data = extracted_data.append(extracted_data, ignore_index=True)
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)])
        
    return extracted_data

# Transform
def transform(data):
    # Convert height which is in inches to millimeter
    data["price"] = round(data.price, 2)

    return data

# Load
def load(TARGETFILE, data_to_load):
    try:
        data_to_load.to_csv(TARGETFILE, index=False)
    except Exception as e:
        log(f"Error loading data to CSV file {TARGETFILE}: {e}")

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

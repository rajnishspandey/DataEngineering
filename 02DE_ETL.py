import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

tmpfile    = "dealership_temp.tmp"               # file used to store all extracted data
logfile    = "dealership_logfile.txt"            # all event logs will be stored in this file
targetfile = "dealership_transformed_data.csv"   # file where transformed data is stored

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
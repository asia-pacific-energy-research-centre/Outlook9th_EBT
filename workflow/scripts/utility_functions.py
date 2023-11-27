

import pandas as pd 
import re
import os
from datetime import datetime

USE_SINGLE_ECONOMY = True
<<<<<<< HEAD
SINGLE_ECONOMY = '20_USA'
=======
SINGLE_ECONOMY = '19_THA'# '19_THA' #20_USA 03_CDA
>>>>>>> 3ebc80e371d6f4956818ee01dd47506643295ecb

EBT_EARLIEST_YEAR = 1980
OUTLOOK_BASE_YEAR = 2020
OUTLOOK_LAST_YEAR = 2070

SECTOR_LAYOUT_SHEET = 'sector_layout_20230719'
FUEL_LAYOUT_SHEET = 'fuel_layout_20230329'

def set_working_directory():
    # Change the working drive
    wanted_wd = 'Outlook9th_EBT'
    os.chdir(re.split(wanted_wd, os.getcwd())[0] + wanted_wd)

def find_most_recent_file_date_id(directory_path):
    """Find the most recent file in a directory based on the date ID in the filename."""
    # List all files in the directory
    files = os.listdir(directory_path)

    # Initialize variables to keep track of the most recent file and date
    most_recent_date = datetime.min
    most_recent_file = None

    # Define a regex pattern for the date ID (format YYYYMMDD)
    date_pattern = re.compile(r'(\d{8})')
    
    # Loop through the files to find the most recent one
    for file in files:
        # Use regex search to find the date ID in the filename
        match = date_pattern.search(file)
        if match:
            date_id = match.group(1)
            # Parse the date ID into a datetime object
            try:
                file_date = datetime.strptime(date_id, '%Y%m%d')
                # If this file's date is more recent, update the most recent variables
                if file_date > most_recent_date:
                    most_recent_date = file_date
                    most_recent_file = file
            except ValueError:
                # If the date ID is not in the expected format, skip this file
                continue

    # Output the most recent file
    if most_recent_file:
        print(f"The most recent file is: {most_recent_file} with the date ID {most_recent_date.strftime('%Y%m%d')}")
    else:
        print("No files found with a valid date ID.")
    return most_recent_file
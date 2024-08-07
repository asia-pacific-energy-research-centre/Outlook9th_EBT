# Grab the latest energy, emissions, and capacity data files and aggregate economies according to the economy code

import pandas as pd
import os
from datetime import datetime
from utility_functions import find_most_recent_file_date_id

def aggregate_economies(SINGLE_ECONOMY_ID):
    # Dictionary of economy codes
    economy_codes = {
        '00_APEC': ['01_AUS', '02_BD', '03_CDA', '04_CHL', '05_PRC', '06_HKC', '07_INA', '08_JPN', '09_ROK', '10_MAS', '11_MEX', '12_NZ', '13_PNG', '14_PE', '15_PHL', '16_RUS', '17_SGP', '18_CT', '19_THA', '20_USA', '21_VN'],
        '22_SEA': ['02_BD', '07_INA', '10_MAS', '15_PHL', '17_SGP', '19_THA', '21_VN'],
        '23_NEA': ['05_PRC', '06_HKC', '08_JPN', '09_ROK', '18_CT'],
        '23b_ONEA': ['01_AUS', '05_PRC', '06_HKC', '08_JPN', '09_ROK', '12_NZ', '13_PNG', '18_CT'],
        '24_OAM': ['01_AUS', '03_CDA', '04_CHL', '11_MEX', '12_NZ', '13_PNG', '14_PE', '20_USA'],
        '25_OCE': ['01_AUS', '02_BD', '05_PRC', '06_HKC', '07_INA', '08_JPN', '09_ROK', '10_MAS', '12_NZ', '13_PNG', '15_PHL', '17_SGP', '18_CT', '19_THA', '21_VN']
    }

    # Check if the SINGLE_ECONOMY_ID is in the dictionary
    if SINGLE_ECONOMY_ID not in economy_codes:
        raise ValueError(f'SINGLE_ECONOMY_ID {SINGLE_ECONOMY_ID} not found in economy_codes dictionary')

    # Get the list of economies for the specified economy code
    economies = economy_codes[SINGLE_ECONOMY_ID]

    # Initialize DataFrames to empty DataFrames
    all_energy_data = pd.DataFrame()
    all_emissions_data = pd.DataFrame()
    all_capacity_data = pd.DataFrame()

    # List to keep track of skipped economies
    skipped_economies = []

    # Grab the latest energy, emissions, and capacity data files for each economy
    for economy in economies:
        try:
            # Define the folder path where the data files are stored
            folder_path = f'results/{economy}/'

            # Read in the latest energy data file
            energy_data_file = find_most_recent_file_date_id(f'{folder_path}/merged/')
            energy_data = pd.read_csv(f'{folder_path}/merged/{energy_data_file}')

            # Read in the latest emissions data file
            emissions_data_file = find_most_recent_file_date_id(f'{folder_path}/emissions/')
            emissions_data = pd.read_csv(f'{folder_path}/emissions/{emissions_data_file}')

            # Read in the latest capacity data file
            capacity_data_file = find_most_recent_file_date_id(f'{folder_path}/capacity/')
            capacity_data = pd.read_csv(f'{folder_path}/capacity/{capacity_data_file}')

            # Concatenate the data files for each economy
            all_energy_data = pd.concat([all_energy_data, energy_data], ignore_index=True)
            all_emissions_data = pd.concat([all_emissions_data, emissions_data], ignore_index=True)
            all_capacity_data = pd.concat([all_capacity_data, capacity_data], ignore_index=True)

        except (FileNotFoundError, pd.errors.EmptyDataError) as e:
            print(f"Skipping economy {economy} due to missing files")
            skipped_economies.append(economy)

    # Print the list of skipped economies
    if skipped_economies:
        print("The following economies were skipped due to missing files:", skipped_economies)

    # Rename all economy codes to the SINGLE_ECONOMY_ID
    all_energy_data['economy'] = SINGLE_ECONOMY_ID
    all_emissions_data['economy'] = SINGLE_ECONOMY_ID
    all_capacity_data['economy'] = SINGLE_ECONOMY_ID

    # Group by all columns except year columns and sum the values
    def group_and_sum(df):
        year_cols = [col for col in df.columns if str(col).isnumeric()]
        non_year_cols = [col for col in df.columns if col not in year_cols]
        return df.groupby(non_year_cols).sum().reset_index()

    all_energy_data = group_and_sum(all_energy_data)
    all_emissions_data = group_and_sum(all_emissions_data)
    all_capacity_data = group_and_sum(all_capacity_data)

    # Function to save the aggregated data and handle old files
    def save_aggregated_data(all_data, data_type):
        # Define the folder path where you want to save the file
        folder_path = f'results/{SINGLE_ECONOMY_ID}/{data_type}/'
        old_folder_path = f'{folder_path}/old'
        # Check if the folder already exists
        if not os.path.exists(folder_path) and isinstance(SINGLE_ECONOMY_ID, str):
            # If the folder doesn't exist, create it
            os.makedirs(folder_path)

        # Check if the old folder exists
        if not os.path.exists(old_folder_path):
            # If the old folder doesn't exist, create it
            os.makedirs(old_folder_path)

        # Identify the previous file
        previous_filename = None
        if os.path.exists(folder_path):
            for file in os.listdir(folder_path):
                if file.startswith(f'{data_type}_{SINGLE_ECONOMY_ID}') and file.endswith('.csv'):
                    previous_filename = file
                    break

        # Move the old file to the 'old' folder if it exists
        if previous_filename:
            old_file_path = f'{folder_path}/{previous_filename}'
            new_old_file_path = f'{old_folder_path}/{previous_filename}'

            # Remove the old file in the 'old' folder if it exists
            if os.path.exists(new_old_file_path):
                os.remove(new_old_file_path)

            os.rename(old_file_path, new_old_file_path)

        # Save the data to a new CSV file
        date_today = datetime.now().strftime('%Y%m%d')
        if isinstance(SINGLE_ECONOMY_ID, str):
            all_data.to_csv(f'{folder_path}/{data_type}_{SINGLE_ECONOMY_ID}_{date_today}.csv', index=False)
        else:
            all_data.to_csv(f'results/{data_type}_{date_today}.csv', index=False)

    # Save the aggregated data
    save_aggregated_data(all_energy_data, 'merged_file_energy')
    save_aggregated_data(all_emissions_data, 'emissions')
    save_aggregated_data(all_capacity_data, 'capacity')

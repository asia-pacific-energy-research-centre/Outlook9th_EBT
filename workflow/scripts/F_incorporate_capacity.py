#take in data on capacity from numerous sectors and incorporate into the final outptus.
#since these are much smaller datasets we will not do as much work on them, instead, hopefully, expect the modeller to ahve them fully mapped and so on.

import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from utility_functions import *


def incorporate_capacity_data(final_df,SINGLE_ECONOMY_ID):
    # List of columns we want to keep
    columns_to_keep = ['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors']

    # Create all_capacity_data DataFrame with only the specified columns from final_df
    # Check if the column exists in final_df to avoid KeyError
    all_capacity_data = pd.DataFrame(columns=[col for col in columns_to_keep if col in final_df.columns])

    # Drop final_df from memory to free up space
    del final_df
    
    ###################
    #UNTIL ALL PREFIXES ARE CHANGED PERMANENTLY WE WILL DO THIS
    done = False
    pattern = f'data/processed/{SINGLE_ECONOMY_ID}/capacity_data/*'
    if os.path.exists(pattern):
        files = glob.glob(pattern)
        for file in files:
            if 'EBT_capacity_' in file:
                done = True
                os.rename(file, file.replace('EBT_capacity_','EBT_generation_capacity_'))
    else:
        raise FileNotFoundError(f'Could not find any capacity data for {SINGLE_ECONOMY_ID} in {pattern}')
    if done:
        print('################### \n changing gen capacity prefixes remember to drop this when its not needed anymore \n ###################')
    ###################
    
    for sheet in ['generation_capacity', 'transport_stocks', 'transport_stock_shares', 'transport_activity']:
        # Read in the capacity data
        pattern = f'data/processed/{SINGLE_ECONOMY_ID}/capacity_data/*{sheet}*.csv'
        files = glob.glob(pattern)
        if len(files) == 0:
            print(f'Could not find capacity data for {sheet}')
        else:
            for file in files:
                capacity_df = pd.read_csv(file)

                # #check cols match what we expect
                # if set(capacity_df.columns.tolist()) != set(all_capacity_data.columns.tolist()):
                #     print('cols do not match expected cols for {} models capacity data: {}'.format(model, set(capacity_df.columns.tolist()) - set(all_capacity_data.columns.tolist())))
                #     break
                #insert sheet name before the yearscols but after everything else
                capacity_df['sheet'] = sheet
                capacity_df = capacity_df[columns_to_keep + ['sheet'] + [col for col in capacity_df.columns if col not in columns_to_keep + ['sheet']]]
                
                
                # Concatenate to all_capacity_data
                all_capacity_data = pd.concat([all_capacity_data, capacity_df], ignore_index=True)

    # Define the folder path where you want to save the file
    folder_path = f'results/{SINGLE_ECONOMY_ID}/capacity/'
    old_folder_path = f'{folder_path}/old'
    # Check if the folder already exists
    if not os.path.exists(folder_path) and isinstance(SINGLE_ECONOMY_ID, str):
        # If the folder doesn't exist, create it
        os.makedirs(folder_path)

    # Check if the old folder exists
    if not os.path.exists(old_folder_path):
        # If the old folder doesn't exist, create it
        os.makedirs(old_folder_path)

    # Identify the previous capacity file
    previous_capacity_filename = None
    if os.path.exists(folder_path):
        for file in os.listdir(folder_path):
            if file.startswith(f'capacity_{SINGLE_ECONOMY_ID}') and file.endswith('.csv'):
                previous_capacity_filename = file
                break

    # Move the old capacity file to the 'old' folder if it exists
    if previous_capacity_filename:
        old_file_path = f'{folder_path}/{previous_capacity_filename}'
        new_old_file_path = f'{old_folder_path}/{previous_capacity_filename}'
        
        # Remove the old file in the 'old' folder if it exists
        if os.path.exists(new_old_file_path):
            os.remove(new_old_file_path)
        
        os.rename(old_file_path, new_old_file_path)

    # Save the data to a new Excel file
    date_today = datetime.now().strftime('%Y%m%d')
    if isinstance(SINGLE_ECONOMY_ID, str):
        all_capacity_data.to_csv(f'{folder_path}/capacity_{SINGLE_ECONOMY_ID}_{date_today}.csv', index=False)
    else:
        all_capacity_data.to_csv(f'results/capacity_{date_today}.csv', index=False)
        
    return all_capacity_data
        
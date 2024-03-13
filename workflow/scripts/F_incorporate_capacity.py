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
    
    for model in ['power', 'transport']:
        # Read in the capacity data
        pattern = f'data/processed/{SINGLE_ECONOMY_ID}/capacity_data/*{model}*.csv'
        files = glob.glob(pattern)
        if len(files) == 0:
            print(f'Could not find capacity data for {model}')
        else:
            for file in files:
                capacity_df = pd.read_csv(file)

                # #check cols match what we expect
                # if set(capacity_df.columns.tolist()) != set(all_capacity_data.columns.tolist()):
                #     print('cols do not match expected cols for {} models capacity data: {}'.format(model, set(capacity_df.columns.tolist()) - set(all_capacity_data.columns.tolist())))
                #     break

                # Concatenate to all_capacity_data
                all_capacity_data = pd.concat([all_capacity_data, capacity_df], ignore_index=True)

    # Define the folder path where you want to save the file
    folder_path = f'results/{SINGLE_ECONOMY_ID}/capacity/'
    # Check if the folder already exists
    if not os.path.exists(folder_path) and isinstance(SINGLE_ECONOMY_ID, str):
        # If the folder doesn't exist, create it
        os.makedirs(folder_path)

    # Save the data to a new Excel file
    date_today = datetime.now().strftime('%Y%m%d')
    if isinstance(SINGLE_ECONOMY_ID, str):
        all_capacity_data.to_csv(f'{folder_path}/capacity_{SINGLE_ECONOMY_ID}_{date_today}.csv', index=False)
    else:
        all_capacity_data.to_csv(f'results/capacity_{date_today}.csv', index=False)
        
    return all_capacity_data
        
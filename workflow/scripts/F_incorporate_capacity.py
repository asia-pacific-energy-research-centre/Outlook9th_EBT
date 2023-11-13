#take in data on capacity from numerous sectors and incorporate into the final outptus.
#since these are much smaller datasets we will not do as much work on them, instead, hopefully, expect the modeller to ahve them fully mapped and so on.

import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from utility_functions import *


def incorporate_capacity_data(final_df):
    all_capacity_data = pd.DataFrame(columns=final_df.columns.tolist()-['subtotal_results', 'subtotal_layout'])
    #drop final_df from memory
    del final_df
    
    for model in ['power', 'transport']:
        #read in the capacity data
        capacity_df = pd.read_csv('data/processed/capacity_data/{}_capacity.csv'.format(model))
        
        #check cols match what we expect
        if set(capacity_df.columns.tolist()) != set(all_capacity_data.columns.tolist()):
            print('cols do not match expected cols for {} models capacity data: {}'.format(model, set(capacity_df.columns.tolist()) - set(all_capacity_data.columns.tolist())))
            break
        
        #concat to all_capacity_data
        all_capacity_data = pd.concat([all_capacity_data, capacity_df])

    #save all_capacity_data
        
    # Define the folder path where you want to save the file
    folder_path = f'results/{SINGLE_ECONOMY}/capacity/'
    # Check if the folder already exists
    if not os.path.exists(folder_path) and USE_SINGLE_ECONOMY:
        # If the folder doesn't exist, create it
        os.makedirs(folder_path)

    #save the data to a new Excel file
    date_today = datetime.now().strftime('%Y%m%d')
    if USE_SINGLE_ECONOMY:
        all_capacity_data.to_csv(f'{folder_path}/capacity_{SINGLE_ECONOMY}_{date_today}.csv', index=False)
    else:
        all_capacity_data.to_csv(f'results/capacity_{date_today}.csv', index=False)
        
    return all_capacity_data
        
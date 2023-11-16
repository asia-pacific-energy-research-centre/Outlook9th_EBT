#take in data on capacity from numerous sectors and incorporate into the final outptus.
#since these are much smaller datasets we will not do as much work on them, instead, hopefully, expect the modeller to ahve them fully mapped and so on.

import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from utility_functions import *


def incorporate_capacity_data(final_df):
    all_capacity_data = pd.DataFrame(columns=final_df.columns.tolist()).drop(columns=['subtotal_results', 'subtotal_layout', 'fuels', 'subfuels'])
    #drop final_df from memory
    del final_df
    
    for model in ['power', 'transport']:
        #read in the capacity data
        files = glob.glob('data/processed/capacity_data/*{}*.csv'.format(model))
        if files.__len__() == 0:
            print('could not find capacity data for {}'.format(model))
        elif files.__len__() > 1:
            #find a file that contains transport in its name and is a csv. if there are more than one throw an error
            if glob.glob('data/processed/capacity_data/*{}*.csv'.format(model)).__len__() > 1:
                raise Exception('more than one file found for {} capacity data'.format(model))
        else:
            capacity_df = pd.read_csv(glob.glob('data/processed/capacity_data/*{}*.csv'.format(model))[0])
            
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
        
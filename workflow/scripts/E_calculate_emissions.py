#calcalate emissions on the merged data:
#tak in merged final_df data (merged because it is the results and layout nerged) and the meissions factors , merge and then calculate emissions

import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from utility_functions import *

def calculate_emissions(final_df,SINGLE_ECONOMY_ID):
    #read in emissions factors
    emissions_factors = pd.read_csv('config/9th_edition_emissions_factors.csv')
    # emissions_factors.columns : fuel_code,	Emissions factor (MT/PJ)
    #melt the final_df on all cols except year cols
    year_cols = [col for col in final_df.columns if str(col).isnumeric()]
    #[year for year in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1)]
    non_year_and_value_cols = [col for col in final_df.columns if col not in year_cols]
    
    final_df = pd.melt(final_df, id_vars=non_year_and_value_cols, value_vars=year_cols, var_name='year', value_name='value')
    #merge with final_df where the fuel_code matches either the fuels col or subfuels col.
    x_in_subfuels = final_df.loc[final_df['subfuels']=='x']
    not_x_in_subfuels = final_df.loc[final_df['subfuels']!='x']
    #merge with emissions_factors
    x_in_subfuels = pd.merge(x_in_subfuels, emissions_factors, how='left', left_on='fuels', right_on='fuel_code')
    not_x_in_subfuels = pd.merge(not_x_in_subfuels, emissions_factors, how='left', left_on='subfuels', right_on='fuel_code')
    #drop the fuel_code col
    x_in_subfuels.drop(columns=['fuel_code'], inplace=True)
    not_x_in_subfuels.drop(columns=['fuel_code'], inplace=True)
    
    #concat
    final_df = pd.concat([x_in_subfuels, not_x_in_subfuels])
    #calculate emissions
    final_df['emissions'] = final_df['value'] * final_df['Emissions factor (MT/PJ)']
    #drop the emissions factor col 
    final_df.drop(columns=['Emissions factor (MT/PJ)', 'value'], inplace=True)
    
    #make wide
    final_df = final_df.pivot_table(index=non_year_and_value_cols, columns='year', values='emissions').reset_index()
    #save emissions
    
    # Define the folder path where you want to save the file
    folder_path = f'results/{SINGLE_ECONOMY_ID}/emissions/'
    # Check if the folder already exists
    if not os.path.exists(folder_path) and (isinstance(SINGLE_ECONOMY_ID, str)):
        # If the folder doesn't exist, create it
        os.makedirs(folder_path)

    #save the data to a new Excel file
    date_today = datetime.now().strftime('%Y%m%d')
    if (isinstance(SINGLE_ECONOMY_ID, str)):
        final_df.to_csv(f'{folder_path}/emissions_{SINGLE_ECONOMY_ID}_{date_today}.csv', index=False)
    else:
        final_df.to_csv(f'results/emissions_{date_today}.csv', index=False)
        
    return final_df#CURRENTLY, NO DOUBT THERE ARE SISUES WITH THIS, SUCH AS HAVING NO SUBSECTORAL CONSIDERATION. I.E. ARE THERE ANY EMISSIONS FOR NON ENERGY USE?

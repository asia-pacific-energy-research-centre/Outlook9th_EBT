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
    
    # #######################################################
    # # Preliminary step: Calculate proportions and adjust values for power sector gas with CCS
    # def adjust_values_based_on_proportions(final_df):
    #     # Define masks for relevant '18_01_02_gas_power' and 'ccs' data
    #     relevant_mask = (
    #         final_df['sub2sectors'].isin(['18_01_02_gas_power', '18_01_02_gas_power_ccs']) & 
    #         (final_df['fuels'] == '08_gas') & 
    #         (final_df['subfuels'] == 'x') & 
    #         final_df['year'].between(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR)
    #     )
    #     relevant_df = final_df.loc[relevant_mask].copy()

    #     # Calculate total values for combining '18_01_02_gas_power' and '18_01_02_gas_power_ccs'
    #     totals = relevant_df.groupby(['scenarios', 'year'])['value'].transform('sum')

    #     # Now calculate proportions for '18_01_02_gas_power' directly in relevant_df
    #     relevant_df['proportion'] = relevant_df['value'] / totals

    #     # Filter to get the proportions specifically for '18_01_02_gas_power'
    #     proportions = relevant_df.loc[relevant_df['sub2sectors'] == '18_01_02_gas_power', ['scenarios', 'year', 'proportion']]

    #     # Identify '09_01_02_gas_power' rows to adjust and replicate for 'ccs'
    #     gas_power_09_mask = (
    #         (final_df['sub2sectors'] == '09_01_02_gas_power') & 
    #         (final_df['sub3sectors'] == 'x') &
    #         (final_df['fuels'] == '08_gas') & 
    #         (final_df['subfuels'] == '08_01_natural_gas') & 
    #         final_df['year'].between(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR)
    #     )

    #     gas_power_09_df = final_df.loc[gas_power_09_mask]
        
    #     gas_power_09_df = gas_power_09_df.merge(proportions, on=['scenarios', 'year'], how='left')
        
    #     gas_power_09_ccs_df = gas_power_09_df.copy()
    #     gas_power_09_ccs_df['sub2sectors'] = '09_01_02_gas_power_ccs'
        
    #     gas_power_09_df['value'] = gas_power_09_df['value'] * (gas_power_09_df['proportion'])
    #     gas_power_09_df.drop(columns=['proportion'], inplace=True)
        
    #     gas_power_09_ccs_df['value'] = gas_power_09_ccs_df['value'] * (1 - gas_power_09_ccs_df['proportion'])
    #     gas_power_09_ccs_df.drop(columns=['proportion'], inplace=True)

    #     gas_power_df = pd.concat([gas_power_09_df, gas_power_09_ccs_df], ignore_index=True)
        
    #     final_df = final_df.loc[~gas_power_09_mask]

    #     # Append the new 'ccs' rows to the DataFrame
    #     final_df = pd.concat([final_df, gas_power_df], ignore_index=True)

    #     return final_df
    # #######################################################
    # final_df = adjust_values_based_on_proportions(final_df)
    
    #merge with final_df where the fuel_code matches either the fuels col or subfuels col.
    x_in_subfuels = final_df.loc[final_df['subfuels']=='x']
    not_x_in_subfuels = final_df.loc[final_df['subfuels']!='x']
    #merge with emissions_factors
    x_in_subfuels = pd.merge(x_in_subfuels, emissions_factors, how='left', left_on='fuels', right_on='fuel_code')
    not_x_in_subfuels = pd.merge(not_x_in_subfuels, emissions_factors, how='left', left_on='subfuels', right_on='fuel_code')
    
    #concat
    final_df = pd.concat([x_in_subfuels, not_x_in_subfuels])
    
    def split_ccs_rows(df):
        # Reset index to ensure clean indexing
        df = df.reset_index(drop=True)
        
        # Define conditions for industry and power sector CCS rows
        industry_ccs_condition = df['sub3sectors'].str.endswith('_ccs') & df['sub3sectors'].str.startswith('14_')
        power_ccs_condition = df['sub2sectors'].str.endswith('_ccs') & df['sub2sectors'].str.startswith('09_')
        
        # Create a copy for manipulation
        df_copy = df.copy()

        # Apply sector-specific factors
        df_copy.loc[industry_ccs_condition, 'Emissions factor (MT/PJ)'] *= 0.8
        df_copy.loc[power_ccs_condition, 'Emissions factor (MT/PJ)'] *= 0.9
        df_copy.loc[industry_ccs_condition, 'sub3sectors'] = df_copy.loc[industry_ccs_condition, 'sub3sectors'] + '_cap'
        df_copy.loc[power_ccs_condition, 'sub2sectors'] = df_copy.loc[power_ccs_condition, 'sub2sectors'] + '_cap'

        # Adjust original df for the non-captured portion
        df.loc[industry_ccs_condition, 'Emissions factor (MT/PJ)'] *= (1 - 0.8)
        df.loc[power_ccs_condition, 'Emissions factor (MT/PJ)'] *= (1 - 0.9)

        # Concatenate the modified copy back with the original dataframe
        df_final = pd.concat([df, df_copy], ignore_index=True)

        return df_final

    # Apply the emissions factor adjustments
    final_df = split_ccs_rows(final_df)
    
    #calculate emissions
    final_df['emissions'] = final_df['value'] * final_df['Emissions factor (MT/PJ)']
    
    # Drop unnecessary columns
    final_df.drop(columns=['Emissions factor (MT/PJ)', 'value', 'fuel_code'], inplace=True)
    
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

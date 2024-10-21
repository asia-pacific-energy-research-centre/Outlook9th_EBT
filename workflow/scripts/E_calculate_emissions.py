import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from utility_functions import *

def calculate_emissions(final_df, SINGLE_ECONOMY_ID):
    # Make a copy of the final_df to avoid unintended modifications on the original
    final_df_copy = final_df.copy()

    # Read in emissions factors
    emissions_factors = pd.read_csv('config/9th_edition_emissions_factors_all_gases_simplified.csv')

    # Melt the final_df on all cols except year cols
    year_cols = [col for col in final_df.columns if str(col).isnumeric()]
    non_year_and_value_cols = [col for col in final_df.columns if col not in year_cols]

    final_df_copy = pd.melt(final_df_copy, id_vars=non_year_and_value_cols, value_vars=year_cols, var_name='year', value_name='value')

    # Set any NaNs in the value col to 0
    final_df_copy['value'] = final_df_copy['value'].fillna(0)

    # Merge emissions factors based on fuels and sectors cols
    final_df_copy = pd.merge(final_df_copy, emissions_factors, how='outer', on=['fuels', 'subfuels', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors'], indicator=True)

    # Handle duplicate rows after merging
    if final_df_copy.duplicated(subset=['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels', 'subtotal_layout', 'subtotal_results', 'year', 'Unit', 'Gas', 'CO2e emissions factor', 'Sector not applicable', 'Fuel not applicable', 'No expected energy use', '_merge']).any():
        raise Exception('There are some duplicate rows in the merged dataframe.')

    # Check for rows that did not merge correctly
    if len(final_df_copy.loc[final_df_copy['_merge'] == 'left_only']) > 0:
        raise Exception('Some rows did not merge with the emissions factors data. Please create more mappings for the missing values.')

    # Remove right_only rows
    final_df_copy = final_df_copy[final_df_copy['_merge'] != 'right_only'].drop(columns=['_merge'])

    # Set emissions factor to 0 where sectors or fuels are not applicable
    condition = (final_df_copy['Sector not applicable'] == True) | (final_df_copy['Fuel not applicable'] == True) | (final_df_copy['No expected energy use'] == True)
    final_df_copy.loc[condition, 'CO2e emissions factor'] = 0

    # Drop unnecessary columns
    final_df_copy = final_df_copy.drop(columns=['Unit', 'Sector not applicable', 'Fuel not applicable', 'No expected energy use'])

    # Check for duplicate rows again
    if final_df_copy.duplicated(subset=non_year_and_value_cols + ['year', 'Gas']).any():
        raise Exception('There are some duplicate rows in the final emissions dataframe.')

    def split_ccs_rows(df):
        # Reset index to ensure clean indexing
        df = df.reset_index(drop=True)

        # Define conditions for CCS rows
        industry_ccs_condition = df['sub3sectors'].str.endswith('_ccs') & df['sub3sectors'].str.startswith('14_') & df['Gas'] == 'CARBON DIOXIDE'
        power_ccs_condition = df['sub2sectors'].str.endswith('_ccs') & df['sub2sectors'].str.startswith('09_') & df['Gas'] == 'CARBON DIOXIDE'

        # Create a copy for manipulation
        df_copy = df.loc[industry_ccs_condition | power_ccs_condition].copy()

        # Apply sector-specific factors
        df_copy.loc[industry_ccs_condition, 'CO2e emissions factor'] *= 0.8
        df_copy.loc[power_ccs_condition, 'CO2e emissions factor'] *= 0.9
        df_copy.loc[industry_ccs_condition, 'sub3sectors'] += '_cap'
        df_copy.loc[power_ccs_condition, 'sub2sectors'] += '_cap'

        # Adjust original df for the non-captured portion
        df.loc[industry_ccs_condition, 'CO2e emissions factor'] *= (1 - 0.8)
        df.loc[power_ccs_condition, 'CO2e emissions factor'] *= (1 - 0.9)

        # Concatenate the modified copy back with the original dataframe
        df_final = pd.concat([df, df_copy], ignore_index=True)

        return df_final

    # Apply the emissions factor adjustments
    final_df_copy = split_ccs_rows(final_df_copy)

    # Set emissions factors for bunkers sectors to 0
    final_df_copy.loc[final_df_copy['sectors'] == '05_international_aviation_bunkers', 'CO2e emissions factor'] = 0
    final_df_copy.loc[final_df_copy['sectors'] == '06_international_marine_bunkers', 'CO2e emissions factor'] = 0

    # Calculate emissions
    final_df_copy['CO2e emissions (Mt/PJ)'] = final_df_copy['value'] * final_df_copy['CO2e emissions factor']

    # Drop unnecessary columns
    final_df_copy.drop(columns=['CO2e emissions factor', 'value'], inplace=True)

    # Aggregate emissions data
    final_df_aggregate = final_df_copy.groupby(non_year_and_value_cols + ['year']).sum().reset_index()
    final_df_aggregate['Gas'] = 'CO2e'

    # Concatenate aggregate and original emissions data
    final_df_copy = pd.concat([final_df_copy, final_df_aggregate], ignore_index=True)

    # Check for duplicates and NaNs
    if final_df_copy.duplicated(subset=non_year_and_value_cols + ['year', 'Gas']).any():
        raise Exception('There are some duplicate rows in the final emissions dataframe.')
    if final_df_copy.isnull().any().any():
        raise Exception('There are some NaNs in the final emissions dataframe.')

    # Pivot data to make it wide format
    final_df_copy = final_df_copy.pivot_table(index=non_year_and_value_cols + ['Gas'], columns='year', values='CO2e emissions (Mt/PJ)').reset_index()

    # Save emissions data
    folder_path = f'results/{SINGLE_ECONOMY_ID}/emissions/'
    old_folder_path = f'{folder_path}/old'
    if not os.path.exists(folder_path) and isinstance(SINGLE_ECONOMY_ID, str):
        os.makedirs(folder_path)
    if not os.path.exists(old_folder_path):
        os.makedirs(old_folder_path)

    gas_to_filename = {'CO2e': 'emissions_co2e', 'CARBON DIOXIDE': 'emissions_co2', 'METHANE': 'emissions_ch4', 'NITROUS OXIDE': 'emissions_no2'}
    for gas in gas_to_filename.keys():
        gas_df = final_df_copy.loc[final_df_copy['Gas'] == gas].copy()
        gas_df.drop(columns=['Gas'], inplace=True)
        filename = gas_to_filename[gas]

        # Move previous emissions file to 'old' folder if exists
        previous_emissions_filename = next((file for file in os.listdir(folder_path) if file.startswith(f'{filename}_{SINGLE_ECONOMY_ID}') and file.endswith('.csv')), None)
        if previous_emissions_filename:
            old_file_path = f'{folder_path}/{previous_emissions_filename}'
            new_old_file_path = f'{old_folder_path}/{previous_emissions_filename}'
            if os.path.exists(new_old_file_path):
                os.remove(new_old_file_path)
            os.rename(old_file_path, new_old_file_path)

        # Save the new emissions data
        date_today = datetime.now().strftime('%Y%m%d')
        if isinstance(SINGLE_ECONOMY_ID, str):
            gas_df.to_csv(f'{folder_path}/{filename}_{SINGLE_ECONOMY_ID}_{date_today}.csv', index=False)
        else:
            gas_df.to_csv(f'results/{filename}_{date_today}.csv', index=False)

    return final_df_copy

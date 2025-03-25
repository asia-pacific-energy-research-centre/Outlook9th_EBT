import os
import re
import pandas as pd 
import numpy as np
import glob
from datetime import datetime
from utility_functions import *
import yaml 
import D_merging_results as D

def check_for_negatives_or_postives_in_wrong_sectors(df, economy, file, INGORE_NANS=False, ABSOLUTE_THRESHOLD=0.001):
    """
    Checks that:
    
      - For each row with sector "03_exports",'10_losses_and_own_use', 04_international_marine_bunkers, 05_international_aviation_bunkers, all year columns (4-digit string columns) are negative.
      - For each row that is NOT in  "09_total_transformation_sector",07_total_primary_energy_supply, '11_statistical_discrepancy, 06_stock_changes 08_transfers , or the rows we expect to be negaitve, all year columns are positive.
    
    If any row fails these checks, a breakpoint is triggered immediately before a ValueError is raised.
    
    Parameters:
      df (pd.DataFrame): DataFrame containing the energy data. It must include a 'sectors' column and 
                         year columns formatted as 4-digit strings.
    
    Raises:
      ValueError: if any row violates the expected sign conventions.
    """
    import re
    import pandas as pd  # Ensure pandas is imported
    # Identify year columns (assumed to be 4-digit strings)
    year_columns = [col for col in df.columns if re.match(r'^\d{4}$', str(col))]
    
    # List to collect error messages.
    errors = []
    errors_df = pd.DataFrame()
    minor_errors_df = pd.DataFrame()
    negative_sectors = {"03_exports", '10_losses_and_own_use', '04_international_marine_bunkers', '05_international_aviation_bunkers'}
    negative_sector_exceptions = {"09_total_transformation_sector", '07_total_primary_energy_supply', '11_statistical_discrepancy', '06_stock_changes', '08_transfers'}
    for idx, row in df.iterrows():
        sector = row['sectors']
        # For rows, all year values should be negative.
        if sector in negative_sectors:
            for col in year_columns:
                val = row[col]
                # if null its also an issue, but 0 is ok
                if (pd.isna(val) and not INGORE_NANS) or val > 0:
                    # breakpoint()
                    errors.append(f"Row {idx} (sector: {sector}) has non-negative value {val} in column {col}.")
                    row['error'] = f"Row {idx} (sector: {sector}) has non-negative value {val} in column {col}."
                    errors_df = pd.concat([errors_df, row], axis=1)
        # For all rows except transformation sector rows, all year values should be positive.
        elif sector not in negative_sector_exceptions and sector not in negative_sectors:
            for col in year_columns:
                val = row[col]
                if (pd.isna(val) and not INGORE_NANS) or val < 0:
                    #check the value is not really small. probably easiest just to base it off absolute value.. e.g. <0.01
                    if abs(val) < ABSOLUTE_THRESHOLD:
                        #record that we changed it in a separate df
                        minor_errors_df = pd.concat([minor_errors_df, row], axis=1)
                        #set the value to 0
                        row[col] = 0
                        #fid the row in the df and change it
                        df.loc[idx] = row
                        continue 
                    # breakpoint()
                    errors.append(f"Row {idx} (sector: {sector}) has non-positive value {val} in column {col}.")
                    row['error'] = f"Row {idx} (sector: {sector}) has non-positive value {val} in column {col}."
                    errors_df = pd.concat([errors_df, row], axis=1)
    
    if errors:
        breakpoint()
        #save them to a file        
        error_path = f'data/temp/error_checking/{economy}_invalid_signs_in_data_{os.path.basename(file).strip(".csv").strip(".xlsx")}.csv'
        errors_df.T.to_csv(error_path)
        
        # raise ValueError(f"Found sign errors in the data for: {file}. See the error file for details: {error_path}. The errors are as follows: {errors}")
        ERROR = True
        error_text = f"Found sign errors in the data for: {file}. See the error file for details: {error_path}. The errors are as follows: {errors}"
    else:
        ERROR = False
        error_text = None
    if minor_errors_df.shape[0] > 0:
        minor_error_path = f'data/temp/error_checking/{economy}_minor_invalid_signs_in_data_{os.path.basename(file).strip(".csv").strip(".xlsx")}.csv'
        minor_errors_df.T.to_csv(minor_error_path)
    
        #since we may have slightly changed the original data, we should save it back to the file
        D.save_merged_file(df, economy, previous_merged_df_filename=None,shared_categories_w_subtotals = shared_categories + ['subtotal_layout', 'subtotal_results'], folder_path=f'results/{economy}/merged', old_folder_path=f'results/{economy}/merged/old', COMPARE_TO_PREVIOUS_MERGE = True)
    
    return ERROR, error_text, minor_errors_df, df

    
#and also we will need a fucntion for adding on supply for new data that is projected (over the whole projeciton year). this will also be sueful for making sure that demand and supply match > we can add on supply even for data that is not projected in this repo, just in case a supply modeller didnt do it exactly right.
#note that this will need to be the last step of any adjustments to the modelled data because otherwise we risk creating a loop where increasing demand leads to icnreased supply which leads to increased demand etc.

#this will work by iterating thrpough each fuel,subfuel and counting up the total rquired energy supply as the sum of demand sectors (14_industry_sector 15_transport_sector 16_other_sector), absolute of own use and non eenrgy use (17_nonenergy_use, 10_losses_and_own_use) and ohters like that and the minus of the sum of 09_total_transformation_sector  (so inputs (negatives made postivie) are treated as extra demnd and outputs (postives made negative) will take awayfrom required supply)

#we will then use the 07_total_primary_energy_supply and subtract the total required energy supply to get the total energy supply that is missing or extra. Then if we know there is extra supply, we can add that to 03_exports or minus from 01_production(depending on what is larger for that fuel in that econmoy.) or if there is missing supply we can add that to 02_imports or 01_production (depending on what is larger for that fuel in that econmoy.) - there is also the case where there is NO supply for a fuel in an economy, in this case let the user know and they can decide what to do (expecting 17_electricity to be in here and in this case it would require actually adding teh required electricity supply to the transformation sector but we'll do that in a separate funtion).

#there will be some cases where the value needs to change by a lot. We will identify these using the proportional differnece and let the user know in case they want to handle it manually.

def double_check_difference_one_year_after_base_year(df_copy, economy, threshold=0.1):
    """
    Double check data for any inconsistencies.

    This function examines the fuel sector/sub1sectors-level data for the base year and the year after
    the base year. It calculates the percentage difference between the two years and, if the difference
    exceeds the specified threshold (e.g., 10%), it checks whether the difference is already expected
    (as loaded from a YAML file). If unexpected differences are found, it writes the error dict to a YAML
    file (with human-readable keys) so that you can copy-paste them into your configuration.

    Parameters:
      df (pd.DataFrame): The data to be checked.
      threshold (float): The proportional difference threshold beyond which an error is raised.
    
    Raises:
      ValueError: if any differences exceed the threshold that are not accounted for in the expected differences.
    """
    import re, os, yaml, pandas as pd
    df = df_copy.copy()
    # Global variable OUTLOOK_BASE_YEAR is assumed to be defined externally.
    str_OUTLOOK_BASE_YEAR = str(OUTLOOK_BASE_YEAR)
    
    # Identify year columns (assumed to be 4-digit strings)
    year_columns = [col for col in df.columns if re.match(r'^\d{4}$', str(col))]
    #make sure the cols are all strings
    df.columns = df.columns.astype(str)
    # if not all(isinstance(col, str) for col in df.columns):
    #     breakpoint()
    #     raise ValueError("Year columns must be strings in the data.")
    
    #drop 19_total 20_total_renewables 21_modern_renewables which are aggregtes of fuels
    df = df[~df['fuels'].isin(['19_total', '20_total_renewables', '21_modern_renewables'])]
    df = df[~df['fuels'].isin(['19_total', '20_total_renewables', '21_modern_renewables'])]
    #and sector aggregates like 07_total_primary_energy_supply, 12_total_final_consumption, 13_total_final_energy_consumption
    df = df[~df['sectors'].isin(['07_total_primary_energy_supply', '12_total_final_consumption', '13_total_final_energy_consumption'])]    
    # Separate negative and positive transformation values.
    df.loc[(df['sectors'] == '09_total_transformation_sector') & (df[str_OUTLOOK_BASE_YEAR] < 0), 'sectors'] = '09_total_transformation_sector_negative'
    df.loc[(df['sectors'] == '09_total_transformation_sector') & (df[str_OUTLOOK_BASE_YEAR] >= 0), 'sectors'] = '09_total_transformation_sector_positive'
    
    # Separate base year and base year+1 data.
    base_year_plus_one = str(int(str_OUTLOOK_BASE_YEAR) + 1)
    base_year_df = df[[str_OUTLOOK_BASE_YEAR] + ['economy', 'scenarios', 'sectors', 'sub1sectors', 'fuels', 'subfuels', 'subtotal_layout']]
    base_year_df_plus_one = df[[base_year_plus_one] + ['economy', 'scenarios', 'sectors', 'sub1sectors', 'fuels', 'subfuels', 'subtotal_results']]
    base_year_df = base_year_df[base_year_df['subtotal_layout'] == False]
    base_year_df_plus_one = base_year_df_plus_one[base_year_df_plus_one['subtotal_results'] == False]
    
    # @and drop subtoal columns
    df.drop(columns = ['subtotal_results', 'subtotal_layout'], inplace=True)
    
    # Sum up values by sectors, subsectors, and fuels.
    base_year_df = base_year_df.groupby(['economy', 'scenarios', 'sectors', 'sub1sectors', 'fuels', 'subfuels']).sum().reset_index()
    base_year_df_plus_one = base_year_df_plus_one.groupby(['economy', 'scenarios', 'sectors', 'sub1sectors', 'fuels', 'subfuels']).sum().reset_index()
    

    # Merge the base year and base_year+1 data.
    base_year_df = base_year_df.merge(
        base_year_df_plus_one, 
        on=['economy', 'scenarios', 'sectors', 'sub1sectors', 'fuels', 'subfuels'], 
        suffixes=('_base', '_base_plus_one')
    )
    base_year_df[base_year_plus_one] = base_year_df[base_year_plus_one].fillna(0)
    base_year_df[str_OUTLOOK_BASE_YEAR] = base_year_df[str_OUTLOOK_BASE_YEAR].fillna(0)
    
    # Drop rows where both year values are zero.
    base_year_df = base_year_df.loc[~(base_year_df[[str_OUTLOOK_BASE_YEAR, base_year_plus_one]] == 0).all(axis=1)]
    
    # Calculate the percentage difference between the two years.
    base_year_df['percent_diff'] = (
        (((base_year_df[base_year_plus_one] - base_year_df[str_OUTLOOK_BASE_YEAR])
        / base_year_df[str_OUTLOOK_BASE_YEAR])*100).round(0)
    )
    big_differences = base_year_df[base_year_df['percent_diff'] > threshold*100]
    
    # Load expected differences from a YAML file.
    expected_yaml_path = f"config/expected_differences_by_economy_between_{str_OUTLOOK_BASE_YEAR}_{base_year_plus_one}.yaml"
    
    try:
        with open(expected_yaml_path, 'r') as file:
            expected_differences_raw = yaml.safe_load(file)
        if expected_differences_raw is None:
            expected_differences_raw = {}
    except FileNotFoundError:
        expected_differences_raw = {}
        
    # Convert expected differences keys to human-readable strings.
    # Assume the top-level keys are tuples representing (economy, scenarios, sectors, sub1sectors, fuels) and the value is the reason which is often empty.
    expected_differences = {}
    for key, val in expected_differences_raw.items():
        if not isinstance(key, str):
            key_str = ", ".join(map(str, key))
        else:
            key_str = key
        expected_differences[key_str] = val
    
    # Create an error dictionary with keys as readable strings.
    error_dict = {}
    for idx, row in big_differences.iterrows():
        # Create a composite key: "economy, scenarios, sectors, sub1sectors, fuels"
        key = ", ".join([str(row['economy']),
                              str(row['scenarios']),
                              str(row['sectors']),
                              str(row['sub1sectors']),
                              str(row['fuels']),
                              str(row['subfuels'])])
        
        if key in expected_differences:
            big_differences.drop(idx, inplace=True)
            continue
        error_dict[key] = ""
    #################################
    #do some quick manipulations to make the data more readable:
    #calc the numerical difference
    big_differences['diff'] = big_differences[base_year_plus_one] - big_differences[str_OUTLOOK_BASE_YEAR]
    #drop the subtotal cols
    big_differences = big_differences.drop(columns = ['subtotal_layout',	'subtotal_results'], errors='ignore')
    #calc portion of total demand. first calc total demand
    
    #where the sector is 18_electricity_output_in_gwh, 19_heat_output_in_pj then set the portion of total demand to 0 so we dont include them in the total
    total_demand = base_year_df[~base_year_df['sectors'].isin(['18_electricity_output_in_gwh', '19_heat_output_in_pj'])].copy()
    total_demand = total_demand.groupby(['economy', 'scenarios']).sum().reset_index()
    
    #join total demand to big differences on economy and scenario
    big_differences = big_differences.merge(total_demand, on=['economy', 'scenarios'], suffixes=('', '_total_demand'))
    
    #calc the portion of total demand
    big_differences['portion_of_total_demand'] = big_differences['diff'] / big_differences[str_OUTLOOK_BASE_YEAR + '_total_demand']
    
    #set the portion of total demand to 0 and str_OUTLOOK_BASE_YEAR + _total_demand to 0 for sectors 18_electricity_output_in_gwh, 19_heat_output_in_pj
    big_differences.loc[big_differences['sectors'].isin(['18_electricity_output_in_gwh', '19_heat_output_in_pj']), str_OUTLOOK_BASE_YEAR + '_total_demand'] = 0
    big_differences.loc[big_differences['sectors'].isin(['18_electricity_output_in_gwh', '19_heat_output_in_pj']), 'portion_of_total_demand'] = 0
    
    #drop everything ending with _total demand except str_OUTLOOK_BASE_YEAR +1 + _total_demand	and portion_of_total_demand
    big_differences = big_differences.drop(columns = [col for col in big_differences.columns if col.endswith('_total_demand') and col != str_OUTLOOK_BASE_YEAR + '_total_demand' and col != 'portion_of_total_demand'], errors='ignore')
    #and drop subtotal cols
    big_differences = big_differences.drop(columns = ['subtotal_layout',	'subtotal_results'], errors='ignore')
    
    #round all of the nuber cols to 1 dp at least
    cols_to_round = [str_OUTLOOK_BASE_YEAR, base_year_plus_one, 'percent_diff', 'diff', f'{str_OUTLOOK_BASE_YEAR}_total_demand', 'portion_of_total_demand']
    big_differences[cols_to_round] = big_differences[cols_to_round].round(1)
    #################################
    # If there are any unexpected differences, write them out as YAML.
    if len(error_dict) > 0:
        # Determine file paths based on the current working directory.
        # economy = base_year_df['economy'].iloc[0]
        
        diff_csv_path = f'data/temp/error_checking/big_differences_{economy}_{str_OUTLOOK_BASE_YEAR}_{base_year_plus_one}.csv'
        error_yaml_path = f'data/temp/error_checking/{economy}_error_dict_{str_OUTLOOK_BASE_YEAR}_{base_year_plus_one}.yaml'
        
        big_differences.to_csv(diff_csv_path, index=False)
        with open(error_yaml_path, 'w') as file:
            # Dump using block style for readability.
            yaml.dump(error_dict, file, default_flow_style=False)
        # breakpoint()
        ERROR = True
        error_text = f'Found big differences in the data. See the error files for details. {diff_csv_path}, {error_yaml_path}'
    else:
        ERROR = False
        error_text = None
    return ERROR, error_text
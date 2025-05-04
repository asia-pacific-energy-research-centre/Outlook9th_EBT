# Grab the latest energy, emissions, and capacity data files and aggregate economies according to the economy code

import pandas as pd
import os
from datetime import datetime
from utility_functions import *

def aggregate_economies(SINGLE_ECONOMY_ID):


    # Check if the SINGLE_ECONOMY_ID is in the dictionary
    if SINGLE_ECONOMY_ID not in AGGREGATE_ECONOMY_MAPPING:
        raise ValueError(f'SINGLE_ECONOMY_ID {SINGLE_ECONOMY_ID} not found in AGGREGATE_ECONOMY_MAPPING dictionary')

    # Get the list of economies for the specified economy code
    economies = AGGREGATE_ECONOMY_MAPPING[SINGLE_ECONOMY_ID]

    # Initialize DataFrames to empty DataFrames
    all_energy_data = pd.DataFrame()
    all_emissions_co2_data = pd.DataFrame()
    all_emissions_no2_data = pd.DataFrame()
    all_emissions_ch4_data = pd.DataFrame()
    all_emissions_co2e_data = pd.DataFrame()
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

            # Read in the latest emissions data files
            emissions_data_file, date_id = find_most_recent_file_date_id(f'{folder_path}/emissions/', RETURN_DATE_ID=True)
            if date_id is None:
                raise FileNotFoundError(f"No emissions data files found for economy {economy}")
            # Read in the emissions data files
            emissions_co2_data = pd.read_csv(f'{folder_path}/emissions/emissions_co2_{economy}_{date_id}.csv')
            emissions_no2_data = pd.read_csv(f'{folder_path}/emissions/emissions_no2_{economy}_{date_id}.csv')
            emissions_ch4_data = pd.read_csv(f'{folder_path}/emissions/emissions_ch4_{economy}_{date_id}.csv')
            emissions_co2e_data = pd.read_csv(f'{folder_path}/emissions/emissions_co2e_{economy}_{date_id}.csv')            

            # Read in the latest capacity data file
            capacity_data_file = find_most_recent_file_date_id(f'{folder_path}/capacity/')
            capacity_data = pd.read_csv(f'{folder_path}/capacity/{capacity_data_file}')

            #because we have data that has different base years (i.e. russia has base year 2021, not 2022) we cannot easily use the subtotal_layout, subtotal_results columns, otehrwise we end up having to specify whether the subtotal_layout is for X economies base year or Y ecoomies base year.. and so on so we will filter for subtotal_layout = false in all historical years (where eyar is less than base year, and subtoal_results = false where year is greater than base year, then concat back the data.
            energy_data = adjsut_economy_subtotals_where_base_year_is_different(economy, energy_data)
            emissions_co2_data = adjsut_economy_subtotals_where_base_year_is_different(economy, emissions_co2_data)
            emissions_no2_data = adjsut_economy_subtotals_where_base_year_is_different(economy, emissions_no2_data)
            emissions_ch4_data = adjsut_economy_subtotals_where_base_year_is_different(economy, emissions_ch4_data)
            emissions_co2e_data = adjsut_economy_subtotals_where_base_year_is_different(economy, emissions_co2e_data)
            
            # Concatenate the data files for each economy
            all_energy_data = pd.concat([all_energy_data, energy_data], ignore_index=True)
            all_emissions_co2_data = pd.concat([all_emissions_co2_data, emissions_co2_data], ignore_index=True)
            all_emissions_no2_data = pd.concat([all_emissions_no2_data, emissions_no2_data], ignore_index=True)
            all_emissions_ch4_data = pd.concat([all_emissions_ch4_data, emissions_ch4_data], ignore_index=True)
            all_emissions_co2e_data = pd.concat([all_emissions_co2e_data, emissions_co2e_data], ignore_index=True)
            all_capacity_data = pd.concat([all_capacity_data, capacity_data], ignore_index=True)

        except (FileNotFoundError, pd.errors.EmptyDataError) as e:
            print(f"Skipping economy {economy} due to missing files")
            skipped_economies.append(economy)

    # Print the list of skipped economies
    if skipped_economies:
        print("The following economies were skipped due to missing files:", skipped_economies)
        
    breakpoint()#want to check whats happening with transport subtotals. 
    
    # Rename all economy codes to the SINGLE_ECONOMY_ID
    all_energy_data['economy'] = SINGLE_ECONOMY_ID
    all_emissions_co2_data['economy'] = SINGLE_ECONOMY_ID
    all_emissions_no2_data['economy'] = SINGLE_ECONOMY_ID
    all_emissions_ch4_data['economy'] = SINGLE_ECONOMY_ID
    all_emissions_co2e_data['economy'] = SINGLE_ECONOMY_ID
    all_capacity_data['economy'] = SINGLE_ECONOMY_ID

    ####
    #remove the stocks data from all_capacity and then add in spearately calcuated stocks data sicne it needs to be claculated differently to a sum (you need the origianl stocks data)
    all_capacity_data = all_capacity_data[all_capacity_data['sheet'] != 'transport_stock_shares']
    
    #load it in from  data/processed/{SINGLE_ECONOMY_ID}/capacity_data/
    apec_stocks_file = find_most_recent_file_date_id(f'data/processed/{SINGLE_ECONOMY_ID}/capacity_data/', filename_part = 'transport_stock_shares')
    try:
        apec_stocks = pd.read_csv(f'data/processed/{SINGLE_ECONOMY_ID}/capacity_data/{apec_stocks_file}')
    except FileNotFoundError:
        raise FileNotFoundError(f"No transport_stock_shares data files found for economy {SINGLE_ECONOMY_ID}. Ths data is provided separately by the transport modeller since it needs to be calculated using the stocks rather than a sum of shares")
    apec_stocks['sheet'] = 'transport_stock_shares'   
    all_capacity_data = pd.concat([all_capacity_data, apec_stocks], ignore_index=True)
    ####
    
    # Group by all columns except year columns and sum the values
    def group_and_sum(df):
        year_cols = [col for col in df.columns if str(col).isnumeric()]
        non_year_cols = [col for col in df.columns if col not in year_cols]
        #make any nas 0in the year cols
        df[year_cols] = df[year_cols].fillna(0)
        return df.groupby(non_year_cols).sum().reset_index()
    all_energy_data = group_and_sum(all_energy_data)
    all_emissions_co2_data = group_and_sum(all_emissions_co2_data)
    all_emissions_no2_data = group_and_sum(all_emissions_no2_data)
    all_emissions_ch4_data = group_and_sum(all_emissions_ch4_data)
    all_emissions_co2e_data = group_and_sum(all_emissions_co2e_data)
    all_capacity_data = group_and_sum(all_capacity_data)

    # Function to save the aggregated data and handle old files
    def save_aggregated_data(all_data, data_type, file_name_id):
        # breakpoint()
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
                if file.startswith(f'{file_name_id}_{SINGLE_ECONOMY_ID}') and file.endswith('.csv'):
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
            all_data.to_csv(f'{folder_path}/{file_name_id}_{SINGLE_ECONOMY_ID}_{date_today}.csv', index=False)
        else:
            all_data.to_csv(f'results/{file_name_id}_{date_today}.csv', index=False)
            
    # Save the aggregated data
    save_aggregated_data(all_energy_data, 'merged', 'merged_file_energy')
    save_aggregated_data(all_emissions_co2_data, 'emissions', 'emissions_co2')
    save_aggregated_data(all_emissions_no2_data, 'emissions', 'emissions_no2')
    save_aggregated_data(all_emissions_ch4_data, 'emissions', 'emissions_ch4')
    save_aggregated_data(all_emissions_co2e_data, 'emissions', 'emissions_co2e')
    save_aggregated_data(all_capacity_data, 'capacity', 'capacity')


##########alternative method:
# def filter_for_subtotal_false_in_all_economy_data():
    
# years_cols = [x for x in energy_w_subtotals.columns if re.match(r'\d\d\d\d', x)]

# #make all values after the base year 0 in historical data
# historical_data = emissions_co2_w_subtotals.copy()
# historical_data.drop(columns=PROJ_YEARS_str, inplace=True)
# proj_data = emissions_co2_w_subtotals.copy()
# proj_data.drop(columns=HIST_YEARS_str, inplace=True)

# #filter for only not subtotal_layout in historical data and not subtotal_results in proj data
# historical_data = historical_data[(historical_data.subtotal_layout==False)]
# proj_data = proj_data[(proj_data.subtotal_results==False)].copy()
# #merge them and sum
# emissions_co2 = pd.concat([historical_data, proj_data], axis=0)
# emissions_co2.fillna(0, inplace=True)

# historical_data = energy_w_subtotals.copy()
# historical_data.drop(columns=PROJ_YEARS_str, inplace=True)
# proj_data = energy_w_subtotals.copy()
# proj_data.drop(columns=HIST_YEARS_str, inplace=True)
# #filter for only not subtotal_layout in historical data and not subtotal_results in proj data
# historical_data = historical_data[(historical_data.subtotal_layout==False)]
# proj_data = proj_data[(proj_data.subtotal_results==False)].copy()
# #merge them and sum
# energy = pd.concat([historical_data, proj_data], axis=0)

# #set any nas in the energy data to 0
# energy.fillna(0, inplace=True)
##########
#orrr we can go inot the datasets that have different base eyars and recategorise the years that are base years for other economies so their subtotal_layout/subtoal_results columns match that expected structure instead:
#i.e.
def adjsut_economy_subtotals_where_base_year_is_different(economy, df):
    ECONOMIES_WITH_DIFFERENT_BASE_YEARS_DICT = {
        '16_RUS':2021
    }
    for economy_, economy_base_year in ECONOMIES_WITH_DIFFERENT_BASE_YEARS_DICT.items():
        if economy == economy_:
            # breakpoint()
            if economy_base_year < OUTLOOK_BASE_YEAR:
                #we grab the years after ECONOMY BASE YEAR that are <= to OUTLOOK+BASE YEAR and set their subtotal_results column to True where subtotal_layout is true, and to fasle where subtotal_layout is false. then cocnat them back in to df 
                year_cols = [col for col in df.columns if re.match(r'^\d{4}$', str(col))]
                different_years = [str(year) for year in range(economy_base_year+1, OUTLOOK_BASE_YEAR+1)]
                non_year_cols = [col for col in df.columns if col not in year_cols]
                df_different_years = df[non_year_cols + different_years]
                # breakpoint()
                df_different_years['old_subtotal_results'] = df_different_years['subtotal_results']
                df_different_years['old_subtotal_layout'] = df_different_years['subtotal_layout']
                #set the subtotal_results to True where subtotal_layout is True and set the subtotal_results to False where subtotal_layout is False
                df_different_years.loc[df_different_years['subtotal_results'] == True, 'subtotal_layout'] = True
                df_different_years.loc[df_different_years['subtotal_results'] == False, 'subtotal_layout'] = False
                
                #now we need to merge these back into the original dataframe but only for the years that are different so we can just drop those years from the original dataframe and then concat them back in.
                df.drop(columns=different_years, inplace=True)
                df_different_years.drop(columns=['old_subtotal_results', 'old_subtotal_layout'], inplace=True)
                
                #concat them back together
                df = pd.concat([df, df_different_years], axis=0)
                
                #make sure column order is as before, non_year_cols + year cols
                df = df[non_year_cols + year_cols]
                return df
            elif economy_base_year > OUTLOOK_BASE_YEAR:
                #we grab the years after ECONOMY BASE YEAR that are <= to OUTLOOK+BASE YEAR and set their subtotal_layout column to True where subtotal_results is true, and to fasle where subtotal_results is false. then cocnat them back in to df 
                year_cols = [col for col in df.columns if re.match(r'^\d{4}$', str(col))]
                different_years = [str(year) for year in range(economy_base_year+1, OUTLOOK_BASE_YEAR+1)]
                non_year_cols = [col for col in df.columns if col not in year_cols]
                df_different_years = df[non_year_cols + different_years]
                
                df_different_years['old_subtotal_results'] = df_different_years['subtotal_results']
                df_different_years['old_subtotal_layout'] = df_different_years['subtotal_layout']
                #set the subtotal_layout to True where subtotal_results is True and set the subtotal_layout to False where subtotal_results is False
                df_different_years.loc[df_different_years['subtotal_layout'] == True, 'subtotal_results'] = True
                df_different_years.loc[df_different_years['subtotal_layout'] == False, 'subtotal_results'] = False
                
                #now we need to merge these back into the original dataframe but only for the years that are different so we can just drop those years from the original dataframe and then concat them back in.
                df.drop(columns=different_years, inplace=True)
                df_different_years.drop(columns=['old_subtotal_results', 'old_subtotal_layout'], inplace=True)
                
                #concat them back together
                df = pd.concat([df, df_different_years], axis=0)
                
                #make sure column order is as before, non_year_cols + year cols
                df = df[non_year_cols + year_cols]
                return df
    return df
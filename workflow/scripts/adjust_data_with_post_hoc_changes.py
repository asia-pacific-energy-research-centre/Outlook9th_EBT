
#%%
#create funciton to take in a dictionary of changes to make to the layout fiel. the dictionary will contain keys as column names (and some other keys which will be things like 'KEEP_CHANGES_IN_FINAL_OUTPUT'=True) and the values will be the values to filter for in the column within the layout file. So for example we have the follwing extract from the df that we need to change values for:
# economy	scenarios	fuels	subfuels	sectors	sub1sectors	sub2sectors	sub3sectors	sub4sectors
# 04_CHL	reference	07_petroleum_products	07_08_fuel_oil	04_international_marine_bunkers	x
# 04_CHL	reference	07_petroleum_products	07_x_jet_fuel	05_international_aviation_bunkers	x
# 04_CHL	reference	07_petroleum_products	07_x_jet_fuel	15_transport_sector	15_01_domestic_air_transport
# 04_CHL	reference	07_petroleum_products	07_08_fuel_oil	15_transport_sector	15_04_domestic_navigation
#Year = 2022
#new values by row:
# -4
# -32
# 25
# 5.103430214

#this ictionary will be stored as a yaml file and read in by the function.
# changes_dictionary = {
#     'CHL_bunkers': [
#         {
#             'economy':'04_CHL',
#             'fuels':'07_petroleum_products',
#             'subfuels':'07_08_fuel_oil',
#             'sectors':'04_international_marine_bunkers',
#             'sub1sectors':'x',
#             'sub2sectors':'x',
#             'sub3sectors':'x',
#             'sub4sectors':'x',
#             'Year':2022,
#             'new_value':-4,
#             'KEEP_CHANGES_IN_FINAL_OUTPUT':True,
#             'change_id':'1237',
#             'old_values': {
#                 '00APEC_2024_new_rus_prc_data': -4, 
#
#               }    
#         },
#         {
#             'economy':'04_CHL',
#             'fuels':'07_petroleum_products',
#             'subfuels':'07_x_jet_fuel',
#             'sectors':'05_international_aviation_bunkers',
#             'sub1sectors':'x',
#             'sub2sectors':'x',
#             'sub3sectors':'x',
#             'sub4sectors':'x',
#             'Year':2022,
#             'new_value':-32,
#             'KEEP_CHANGES_IN_FINAL_OUTPUT':True,
#             'change_id':'1236',
#             'old_values': {
#                 '00APEC_2024_new_rus_prc_data': -4, 
#
#               }    
#         },
#         {
#             'economy':'04_CHL',
#             'fuels':'07_petroleum_products',
#             'subfuels':'07_x_jet_fuel',
#             'sectors':'15_transport_sector',
#             'sub1sectors':'15_01_domestic_air_transport',
#             'sub2sectors':'x',
#             'sub3sectors':'x',
#             'sub4sectors':'x',
#             'Year':2022,
#             'new_value':25,
#             'KEEP_CHANGES_IN_FINAL_OUTPUT':True,
#             'change_id':'1235'
#             'old_values': {
#                 '00APEC_2024_new_rus_prc_data': -4, 
#
#               }    
#         },
#         {
#             'economy':'04_CHL',
#             'fuels':'07_petroleum_products',
#             'subfuels':'07_08_fuel_oil',
#             'sectors':'15_transport_sector',
#             'sub1sectors':'15_04_domestic_navigation',
#             'sub2sectors':'x',
#             'sub3sectors':'x',
#             'sub4sectors':'x',
#             'Year':2022,
#             'new_value':5.103430214,
#             'KEEP_CHANGES_IN_FINAL_OUTPUT':True,
#             'change_id':'1234',
#             'old_values': {
#                 '00APEC_2024_new_rus_prc_data': -4, 
#
#               }    
#         }
#     ]
#   
# }

#this function will take in the layout file and the changes dictionary and return the layout file with the changes made. 
# However, so that we can keep track of the changes made, we will also return a dictionary of the changes made. This dictionary will have the same structure as the changes dictionary, but the values will be the old values that were replaced. This will allow us to revert the changes if KEET_DATA_IN_FINAL_OUTPUT is False. This will be done after running D_merging_results, so that we can keep the original layout file and the original data in the final energy df if we dont want to show the changes in the final output. 
# - maybe the above point can be done only if MAJOR_SUPPLY_DATA_AVAILABLE is True, as that indicates that we are at the final stage of the pipeline, where supply results are being merged.

import pandas as pd
import yaml
import copy
import numpy as np
from datetime import datetime
from utility_functions import *

import yaml
from itertools import product
#########################

def load_changes_dictionary(changes_file):
    # Read in the changes file
    with open(f'config/{changes_file}', 'r') as file:
        changes_dictionary = yaml.safe_load(file)
    return changes_dictionary

def save_changes_dictionary(changes_dictionary, changes_file):
    # Save the changes dictionary

    #save a archive copy of the file:
    if not os.path.exists('config/archive'):
        os.makedirs('config/archive')
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    changes_file_x = changes_file.split('.')[0]
    shutil.copy(f'config/{changes_file}', f'config/archive/{changes_file_x}_{timestamp}.yml')
    
    with open(f'config/{changes_file}', 'w') as file:
        yaml.dump(changes_dictionary, file)
        
def adjust_layout_file_with_post_hoc_changes(SINGLE_ECONOMY_ID, layout_df):
    layout_df_copy = layout_df.copy()
    changes_dictionary = load_changes_dictionary(CHANGES_FILE)
    changes_dictionary_copy = copy.deepcopy(changes_dictionary)
    if changes_dictionary is None:
        return layout_df
    
    for key, changes_list in changes_dictionary.items():
        # Double check the economy is the same as SINGLE_ECONOMY_ID
        if changes_list[0]['economy'] != SINGLE_ECONOMY_ID:
            continue
        
        # Iterate through the changes list
        for change in changes_list:
            
            if SINGLE_ECONOMY_ID != change['economy']:
                breakpoint()
                raise ValueError(f'Error in adjust_layout_file: economy in changes dictionary does not match SINGLE_ECONOMY_ID. it is probably a typo in the changes dictionary')
            # Get the row to change
            row = layout_df.copy()
            
            for column, filter_value in change.items():
                if column == 'fuels' or column == 'sectors':
                    row = row[row[column] == filter_value]
            
            ##########
            #if theres insert_new_row and its true then insert the row as a new row instead of replacing anything
            if change.get('insert_new_row', False) and len(row) == 0:
                # breakpoint()
                # original_value = np.nan
                #if the row is not already in the data add it, if it is we just update it:
                years = [col for col in layout_df.columns if re.match(r'^\d{4}$', str(col))]
                # Insert the new row into the layout_df
                new_row = {'sectors': change['sectors'], 'fuels': change['fuels']}
                for year in years:
                    new_row[year] = 0
                #set the year we have in the current change
                new_row[change['Year']] = change['new_value']
                #insert
                # breakpoint()
                layout_df = pd.concat([layout_df, pd.DataFrame([new_row])], ignore_index=True)
            else:
                ##########
                # Get the old value
                if len(row) != 1:
                    breakpoint()
                    raise ValueError(f'Error in adjust_layout_file: row not found for {key} in model_df_clean_wide. change is {change}')
                
                #get the original value and set as float
                original_value = float(row[change['Year']].values[0].copy())
                # Change the value in the layout file
                layout_df.loc[row.index, change['Year']] = change['new_value']
                
                # Append the change back into the YAML for record keeping
                if 'old_values' not in change:
                    changes_dictionary_copy[key][changes_list.index(change)]['old_values'] = {}
                if ESTO_DATA_FILENAME not in changes_dictionary_copy[key][changes_list.index(change)]['old_values'].keys():
                    changes_dictionary_copy[key][changes_list.index(change)]['old_values'][ESTO_DATA_FILENAME] = original_value
                elif changes_dictionary_copy[key][changes_list.index(change)]['old_values'][ESTO_DATA_FILENAME] != original_value:
                    breakpoint()
                    raise ValueError(f'Error in adjust_layout_file: old value in changes dictionary does not match value in layout file for {key}')
    # breakpoint()
    save_changes_dictionary(changes_dictionary_copy, CHANGES_FILE)
    # Return the layout file with the changes made
    return layout_df

# def revert_changes_to_merged_file_where_KEEP_CHANGES_IN_FINAL_OUTPUT_is_False(SINGLE_ECONOMY_ID, model_df_clean_wide):
#     #this hasnt been used since its creation since we moved from layout file to ESTO data file manipulation.
#     #note that this can be run even if the values have already been changed, so running this twice in a row by running merging_results twice will not screw up the data
#     changes_dictionary = load_changes_dictionary(CHANGES_FILE)
#     model_df_clean_wide_copy = model_df_clean_wide.copy()
#     for scenario in model_df_clean_wide_copy['scenarios'].unique():
#         # Iterate through the changes dictionary
#         for key, changes_list in changes_dictionary.items():
#             # Double check the economy is the same as SINGLE_ECONOMY_ID
#             if changes_list[0]['economy'] != SINGLE_ECONOMY_ID:
#                 continue
            
#             # Iterate through the changes list
#             for change in changes_list:
#                 change_copy = copy.deepcopy(change)  # Create a deep copy
                
#                 # Get the row to change
#                 row = model_df_clean_wide[model_df_clean_wide['scenarios'] == scenario].copy()
                
#                 for column, filter_value in change_copy.items():
#                     if column != 'fuels' or column != 'sectors':
#                         row = row[row[column] == filter_value]
                
#                 if len(row) != 1:
#                     breakpoint()
#                     raise ValueError(f'Error in revert_changes_to_merged_file_where_KEEP_CHANGES_IN_FINAL_OUTPUT_is_False: row not found for {change_copy} in model_df_clean_wide')
                
#                 # Find old value for the current ESTO_DATA_FILENAME
#                 if ESTO_DATA_FILENAME in change_copy['old_values']:
#                     old_value = change_copy['old_values'][ESTO_DATA_FILENAME]
#                 else:
#                     breakpoint()
#                     raise ValueError(f'Error in revert_changes_made_where_KEEP_CHANGES_IN_FINAL_OUTPUT_is_False: old value not found for {change_copy}')
                
#                 # Change the value in the merged file
#                 if not change_copy['KEEP_CHANGES_IN_FINAL_OUTPUT']:
#                     model_df_clean_wide.loc[row.index, change_copy['Year']] = old_value
    
#     return model_df_clean_wide



#%%
def generate_yaml(years=None, economies=None, fuels=None, values=None, sectors=None, output_file='output.yaml'):
    """
    Generate a YAML file with all combinations of the provided lists.
    
    Parameters:
        years (list): List of year values.
        economies (list): List of economy identifiers.
        fuels (list): List of fuels.
        values (list): List of new_value numbers.
        sectors (list): List of sector descriptions.
        output_file (str): Filename for the output YAML file.
        
    The function writes a YAML file with each combination as an item.
    """
    
    # Updated lists
    years = [
        2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022
    ]

    fuels = [
        # "1.2 Other bituminous coal",
        # "1.5 Lignite",
        # "2.1 Coke oven coke",
        # "2.3 Coke oven gas",
        # "2.4 Blast furnace gas",
        # "6.1 Crude oil",
        # "16.2 Industrial waste",
        # "12.1 of which: Photovoltaics",
        # "7.07 Gas/diesel oil",
        # "7.08 Fuel oil",
        # "7.09 LPG",
        # "7.1 Refinery gas (not liquefied)",
        # "8.1 Natural gas",
        # "15.5 Other biomass"
        '17 Electricity'
    ]

    sectors = [
        '16.1 Commercial and public services'
        # "16.1.4 Traditional data centres"
    ]
    
    insert_new_rows = []

    economies = ["17_SGP"]
    values = [
        round(value, 2) for value in [
            38.13479791, 39.71160122, 41.9327991, 41.10480079, 42.60239821, 
            47.07216001, 49.62852001, 50.79316001, 51.01256001, 48.76236001, 
            49.01092001, 48.87468001, 48.77796001, 49.82496001, 50.69976001, 
            52.04160001, 52.55856001, 53.70000001, 55.41768001, 57.78204001, 
            55.12536001, 59.23956001, 63.88428001
        ]
    ]
    # values = [1.481, 3.004, 4.518, 6.68, 8.28, 10.14, 10.362, 10.668, 10.89, 11.16, 11.316, 11.484, 11.766, 11.538, 12.174, 12.39]

    # # Generate the YAML file and data list
    # data = generate_yaml(years, economies, fuels, values, sectors)
    if len(values)>1:
        #we need to zip the values and years instead of findin their products:
        years_values_zip = zip(years, values)
    else:
        years_values_zip = zip(years, values*len(years))
        
    data = []
    if len(insert_new_rows)>0:
        for (year, value) in years_values_zip:
            for economy, fuel, sector,insert_new_row in product(economies, fuels, sectors, insert_new_rows):
                data.append({
                    "Year": year,
                    "economy": economy,
                    "fuels": fuel,
                    "new_value": value,
                    "sectors": sector,
                    "insert_new_row": insert_new_row,
                })
    else:
        for (year, value) in years_values_zip:
            for economy, fuel, sector in product(economies, fuels, sectors):
                data.append({
                    "Year": year,
                    "economy": economy,
                    "fuels": fuel,
                    "new_value": value,
                    "sectors": sector,
                })
    
    # Dump the list of dictionaries to a YAML file
    with open(output_file, 'w') as f:
        yaml.dump(data, f, sort_keys=False)
        
    # Print the generated YAML content to the console
    print(yaml.dump(data, sort_keys=False))


# generate_yaml(years=None, economies=None, fuels=None, values=None, sectors=None, output_file='output.yaml')

# %%
# reference	17_SGP	16_other_sector	x
# target	17_SGP	16_other_sector	x
# reference	17_SGP	16_other_sector	16_01_buildings
# target	17_SGP	16_other_sector	16_01_buildings
# 17_electricity	x
#set these to:

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
            
            # Get the old value
            if len(row) != 1:
                breakpoint()
                raise ValueError(f'Error in adjust_layout_file: row not found for {key} in model_df_clean_wide')
            
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
    
    save_changes_dictionary(changes_dictionary_copy, CHANGES_FILE)
    # Return the layout file with the changes made
    return layout_df

def revert_changes_to_merged_file_where_KEEP_CHANGES_IN_FINAL_OUTPUT_is_False(SINGLE_ECONOMY_ID, model_df_clean_wide):
    #this hasnt been used since its creation since we moved from layout file to ESTO data file manipulation.
    #note that this can be run even if the values have already been changed, so running this twice in a row by running merging_results twice will not screw up the data
    changes_dictionary = load_changes_dictionary(CHANGES_FILE)
    model_df_clean_wide_copy = model_df_clean_wide.copy()
    for scenario in model_df_clean_wide_copy['scenarios'].unique():
        # Iterate through the changes dictionary
        for key, changes_list in changes_dictionary.items():
            # Double check the economy is the same as SINGLE_ECONOMY_ID
            if changes_list[0]['economy'] != SINGLE_ECONOMY_ID:
                continue
            
            # Iterate through the changes list
            for change in changes_list:
                change_copy = copy.deepcopy(change)  # Create a deep copy
                
                # Get the row to change
                row = model_df_clean_wide[model_df_clean_wide['scenarios'] == scenario].copy()
                
                for column, filter_value in change_copy.items():
                    if column != 'fuels' or column != 'sectors':
                        row = row[row[column] == filter_value]
                
                if len(row) != 1:
                    breakpoint()
                    raise ValueError(f'Error in revert_changes_to_merged_file_where_KEEP_CHANGES_IN_FINAL_OUTPUT_is_False: row not found for {change_copy} in model_df_clean_wide')
                
                # Find old value for the current ESTO_DATA_FILENAME
                if ESTO_DATA_FILENAME in change_copy['old_values']:
                    old_value = change_copy['old_values'][ESTO_DATA_FILENAME]
                else:
                    breakpoint()
                    raise ValueError(f'Error in revert_changes_made_where_KEEP_CHANGES_IN_FINAL_OUTPUT_is_False: old value not found for {change_copy}')
                
                # Change the value in the merged file
                if not change_copy['KEEP_CHANGES_IN_FINAL_OUTPUT']:
                    model_df_clean_wide.loc[row.index, change_copy['Year']] = old_value
    
    return model_df_clean_wide

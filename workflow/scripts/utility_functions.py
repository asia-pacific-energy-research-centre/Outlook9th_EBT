

import pandas as pd 
import re
import os
from datetime import datetime
import os
import shutil

#when we have new data from ESTO you will want to set this to False or None and run the data through the whole pipeline to get results/model_df_wide_' + date_today +'.csv' for modellers to use as an input
SINGLE_ECONOMY_ID_VAR = '17_SGP' #'19_THA'# '19_THA' #20_USA 03_CDA
# SINGLE_ECONOMY_ID = '19_THA' # '19_THA' #20_USA 03_CDA

EBT_EARLIEST_YEAR = 1980
OUTLOOK_BASE_YEAR = 2021
OUTLOOK_LAST_YEAR = 2070

SECTOR_LAYOUT_SHEET = 'sector_layout_20230719'
FUEL_LAYOUT_SHEET = 'fuel_layout_20230329'

# ESTO_DATA_FILENAME = '00APEC_May2023'

ESTO_DATA_FILENAME = '00APEC_January2024'

ALL_ECONOMY_IDS = ["01_AUS", "02_BD", "03_CDA", "04_CHL", "05_PRC", "06_HKC", "07_INA", "08_JPN", "09_ROK", "10_MAS", "11_MEX", "12_NZ", "13_PNG", "14_PE", "15_PHL", "16_RUS", "17_SGP", "18_CT", "19_THA", "20_USA", "21_VN"]

def set_working_directory():
    # Change the working drive
    wanted_wd = 'Outlook9th_EBT'
    os.chdir(re.split(wanted_wd, os.getcwd())[0] + wanted_wd)

def find_most_recent_file_date_id(directory_path):
    """Find the most recent file in a directory based on the date ID in the filename."""
    # List all files in the directory
    files = os.listdir(directory_path)

    # Initialize variables to keep track of the most recent file and date
    most_recent_date = datetime.min
    most_recent_file = None

    # Define a regex pattern for the date ID (format YYYYMMDD)
    date_pattern = re.compile(r'(\d{8})')
    
    # Loop through the files to find the most recent one
    for file in files:
        # Use regex search to find the date ID in the filename
        match = date_pattern.search(file)
        if match:
            date_id = match.group(1)
            # Parse the date ID into a datetime object
            try:
                file_date = datetime.strptime(date_id, '%Y%m%d')
                # If this file's date is more recent, update the most recent variables
                if file_date > most_recent_date:
                    most_recent_date = file_date
                    most_recent_file = file
            except ValueError:
                # If the date ID is not in the expected format, skip this file
                continue

    # Output the most recent file
    if most_recent_file:
        print(f"The most recent file is: {most_recent_file} with the date ID {most_recent_date.strftime('%Y%m%d')}")
    else:
        print("No files found with a valid date ID.")
    return most_recent_file
    
def move_files_to_archive_for_economy(LOCAL_FILE_PATH, economy):
    #create archive folder if not there
    if not os.path.exists(f'{LOCAL_FILE_PATH}/Integration/{economy}/00_LayoutTemplate/'):
        raise Exception(f'{LOCAL_FILE_PATH}/Integration/{economy}/00_LayoutTemplate/ does not exist')#this is important so we dont accidentally move files to the wrong place
    
    if not os.path.exists(f'{LOCAL_FILE_PATH}/Integration/{economy}/00_LayoutTemplate/archive'):
        os.makedirs(f'{LOCAL_FILE_PATH}/Integration/{economy}/00_LayoutTemplate/archive')
        
    #move files to archive
    files = os.listdir(f'{LOCAL_FILE_PATH}/Integration/{economy}/00_LayoutTemplate')
    
    for file in files:
        if file.startswith('model_df_wide'):
            breakpoint()
            shutil.move(f'{LOCAL_FILE_PATH}/Integration/{economy}/00_LayoutTemplate/{file}', f'{LOCAL_FILE_PATH}/Integration/{economy}/00_LayoutTemplate/archive/{file}')


def compare_values_in_final_energy_dfs(old_final_energy_df, final_energy_df):
    #join the dfs on the index cols and then compare rows
    joined_df = old_final_energy_df.merge(final_energy_df, on=['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels', 'subtotal_layout', 'subtotal_results'], suffixes=('_old', '_new'), how='outer', indicator=True)
    #compare the values in the rows as well as looking at the indicator column
    not_both = joined_df[joined_df['_merge'] != 'both']
    not_both.to_csv('not_both.csv')
    #compare vallues by calcuating difference between the two values. keep rows where any of the values are different then keep those rows with all the cols
    diff_cols = []
    for col in joined_df.columns:
        if col.endswith('_old'):
            col_root = col[:-4]
            joined_df[col_root + '_diff'] = joined_df[col] - joined_df[col_root + '_new']
            diff_cols.append(col_root + '_diff')
    #drop the rows that are not needed
    joined_df = joined_df.drop(columns=['_merge'])
    joined_df = joined_df[joined_df[diff_cols].sum(axis=1) != 0]
    #reorder the columns so the diff cols are next to the cols ['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels', 'subtotal_layout', 'subtotal_results']
    cols = joined_df.columns.tolist()
    cols = cols[:10] + diff_cols + cols[10:len(cols)-len(diff_cols)]
    joined_df = joined_df[cols]
    joined_df.to_csv('joined_df.csv')
    

def run_main_up_to_merging_for_every_economy(LOCAL_FILE_PATH, MOVE_OLD_FILES_TO_ARCHIVE=False):
    """
    This is really just meant for moving every economy's model_df_clean_wide df into {LOCAL_FILE_PATH}\Modelling\Integration\{ECONOMY_ID}\00_LayoutTemplate so the modellers can use it as a starting point for their modelling.
    
    it will remove the original files from the folder and move them to an archive folder in the same directory using the function utils.move_files_to_archive_for_economy(LOCAL_FILE_PATH, economy) if MOVE_OLD_FILES_TO_ARCHIVE is True
    """
    from main import main
    file_date_id = datetime.now().strftime('%Y%m%d')
    for economy in ALL_ECONOMY_IDS:
        
        if MOVE_OLD_FILES_TO_ARCHIVE:
            move_files_to_archive_for_economy(LOCAL_FILE_PATH, economy)
        final_energy_df, emissions_df, capacity_df, model_df_clean_wide = main(ONLY_RUN_UP_TO_MERGING = True, SINGLE_ECONOMY_ID=economy)
        model_df_clean_wide.to_csv(f'{LOCAL_FILE_PATH}/Integration/{economy}/00_LayoutTemplate/model_df_wide_{economy}_{file_date_id}.csv', index=False)
        
        reference_df = model_df_clean_wide[model_df_clean_wide['scenarios'] == 'reference'].copy().reset_index(drop = True)
        target_df = model_df_clean_wide[model_df_clean_wide['scenarios'] == 'target'].copy().reset_index(drop = True)
        
        reference_df.to_csv(f'{LOCAL_FILE_PATH}/Integration/{economy}/00_LayoutTemplate/model_df_wide_ref_{economy}_{file_date_id}.csv', index=False)
        target_df.to_csv(f'{LOCAL_FILE_PATH}/Integration/{economy}/00_LayoutTemplate/model_df_wide_tgt_{economy}_{file_date_id}.csv', index=False)
        print('Done run_main_up_to_merging_for_every_economy for ' + economy)
        
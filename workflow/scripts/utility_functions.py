#%%

import pandas as pd 
import re
import os
from datetime import datetime
import os
import shutil

INDUSTRY_CCS_ABATEMENT_RATE = 0.2
POWER_CCS_ABATEMENT_RATE = 0.1
#TEMPORARY WHILE WE WAIT FOR ESTO FINALISED DATA:
ECONOMYS_WITH_INITIAL_NAS_AND_THEIR_COLS = {
    '16_RUS': ['value'],
    '21_VN': ['value'],
    '06_HKC': ['fuels', 'sectors'],
}
ERRORS_DAIJOUBU = True
# If merging supply results (i.e. the final stage of merging results from the modellers), set this to True and calculate TPES top down instead of bottom up
MAJOR_SUPPLY_DATA_AVAILABLE = True    
CHECK_DATA = True
# Set to True if you want to run time consuming checks on data, assuming MAJOR_SUPPLY_DATA_AVAILABLE is True 

EBT_EARLIEST_YEAR = 1980
OUTLOOK_LAST_YEAR = 2070
OUTLOOK_BASE_YEAR = 2022
OUTLOOK_BASE_YEAR_RUSSIA = 2021
#to incorporate russia as a single economy with a different base year to the others, we ran the main.py script with SINGLE_ECONOMY_ID = False, then in the output, ie. results/model_df_wide_20250122.csv we set values to 0 in 2022 for russia only. then we also ran the main.py script with SINGLE_ECONOMY_ID = '16_RUS' and OUTLOOK_BASE_YEAR set to what OUTLOOK_BASE_YEAR_RUSSIA is. This is what it will need to be set to when ever merging results for russia too.

SECTOR_LAYOUT_SHEET = 'sector_layout_20230719'
FUEL_LAYOUT_SHEET = 'fuel_layout_20250212'#these are the fuels that we expectto be used in the output. it has kind of been forgotten what they are useful for but we keep them updated anyway

# ESTO_DATA_FILENAME = '00APEC_May2023'

ESTO_DATA_FILENAME = '00APEC_2024_20250312'
NEW_YEARS_IN_INPUT = False#ONLY SET ME IF YOU ARE USING NEW DATA FROM ESTO WHICH SHOULD ONLY HAPPEN ONCE A YEAR. If you do this then you will wantto set Single_ECONOMY_ID_VAR to False and run the whole pipeline to get the results/model_df_wide_' + date_today +'.csv for modellers to use as an input

CHANGES_FILE = 'changes_to_ESTO_data.yml'
SCENARIOS_list = ['reference', 'target']

ALL_ECONOMY_IDS = ["01_AUS", "02_BD", "03_CDA", "04_CHL", "05_PRC", "06_HKC", "07_INA", "08_JPN", "09_ROK", "10_MAS", "11_MEX", "12_NZ", "13_PNG", "14_PE", "15_PHL", "16_RUS", "17_SGP", "18_CT", "19_THA", "20_USA", "21_VN"]

AGGREGATE_ECONOMIES = ['00_APEC', '22_SEA', '23_NEA', '23b_ONEA', '24_OAM', '25_OCE', '24b_OOAM', '26_NA']

AGGREGATE_ECONOMY_MAPPING = {
    '00_APEC': ['01_AUS', '02_BD', '03_CDA', '04_CHL', '05_PRC', '06_HKC', '07_INA', '08_JPN', '09_ROK', '10_MAS', '11_MEX', '12_NZ', '13_PNG', '14_PE', '15_PHL', '16_RUS', '17_SGP', '18_CT', '19_THA', '20_USA', '21_VN'],
    '22_SEA': ['02_BD', '07_INA', '10_MAS', '15_PHL', '17_SGP', '19_THA', '21_VN'],
    '23_NEA': ['06_HKC', '08_JPN', '09_ROK', '18_CT'],
    '23b_ONEA': ['06_HKC', '09_ROK', '18_CT'],
    '24_OAM': ['03_CDA', '04_CHL', '11_MEX', '14_PE'],
    '24b_OOAM': ['04_CHL', '11_MEX', '14_PE'],
    '25_OCE': ['01_AUS', '12_NZ', '13_PNG'],
    '26_NA': ['03_CDA', '20_USA'],
}

# Specify the shared category columns in the desired order
shared_categories = ['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels']

def set_working_directory():
    # Change the working drive
    wanted_wd = 'Outlook9th_EBT'
    os.chdir(re.split(wanted_wd, os.getcwd())[0] + '/' + wanted_wd)

def find_most_recent_file_date_id(directory_path, filename_part = None,RETURN_DATE_ID = False, PRINT=False):
    """Find the most recent file in a directory based on the date ID in the filename."""
    # List all files in the directory
    files = os.listdir(directory_path)

    # Initialize variables to keep track of the most recent file and date
    most_recent_date = datetime.min
    most_recent_file = None
    date_id = None
    # Define a regex pattern for the date ID (format YYYYMMDD)
    date_pattern = re.compile(r'(\d{8})')
    
    # Loop through the files to find the most recent one
    for file in files:
        if filename_part is not None:
            if filename_part not in file:
                continue
        # Use regex search to find the date ID in the filename
        if os.path.isdir(os.path.join(directory_path, file)):
            continue
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
        if PRINT:
            print(f"The most recent file is: {most_recent_file} with the date ID {most_recent_date.strftime('%Y%m%d')}")
    else:
        if PRINT:
            print("No files found with a valid date ID.")
    if RETURN_DATE_ID:
        return most_recent_file, date_id
    else:
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
        
        

def shift_output_files_to_visualisation_input(economy_ids = ["01_AUS", "02_BD", "03_CDA", "04_CHL", "05_PRC", "06_HKC", "07_INA", "08_JPN", "09_ROK", "10_MAS", "11_MEX", "12_NZ", "13_PNG", "14_PE", "15_PHL", "16_RUS", "17_SGP", "18_CT", "19_THA", "20_USA", "21_VN"], results_path = r'C:\Users\finbar.maunsell\github\Outlook9th_EBT\results', visualisation_input_path = r'C:\Users\finbar.maunsell\github\9th_edition_visualisation\input_data',file_date_id = '20241029'):
    #create method to shift the files in C:\Users\finbar.maunsell\github\Outlook9th_EBT\results\ECONOMY_ID to C:\Users\finbar.maunsell\github\9th_edition_visualisation\input_data\ECONOMY_ID
    #files are:
    # capacity_12_NZ_20241023.csv
    # emissions_ch4_12_NZ_20241025
    # emissions_co2_12_NZ_20241025
    # emissions_co2e_12_NZ_20241025
    # emissions_no2_12_NZ_20241025
    # merged_file_energy_12_NZ_20241023
    #where 12_NZ is the economy id and 20241023 is the dateid
   
    import os
    import shutil
    import datetime
    
    files_to_retrieve = ['capacity', 'emissions_ch4', 'emissions_co2', 'emissions_co2e', 'emissions_no2', 'merged_file_energy']
    
    #loop over the economy ids
    for economy_id in economy_ids:
        #set the path to the economy id folder
        economy_id_path = os.path.join(results_path, economy_id)
        
        #frst put the old files in a archive folder
        if not os.path.exists(os.path.join(visualisation_input_path, 
        economy_id, 'archive')):
            os.makedirs(os.path.join(visualisation_input_path, economy_id, 'archive'))
        #move the files to the archive folder
        for file in os.listdir(os.path.join(visualisation_input_path, economy_id)):
            if os.path.isfile(os.path.join(visualisation_input_path, economy_id, file)):
                shutil.move(os.path.join(visualisation_input_path, economy_id, file), os.path.join(visualisation_input_path,  economy_id, 'archive', file))
                
        #loop over the files in the economy id folder. you will need to do a walk to get the files in the subfolders
        for file in files_to_retrieve:
            file = f'{file}_{economy_id}_{file_date_id}.csv'
            #now walk thourgh the files in the economy id folder
            for root, dirs, files in os.walk(economy_id_path):
                for file_ in files:
                    if file_ == file:
                        #get the path to the file
                        file_path = os.path.join(root, file_)
                        #get the new file path
                        new_file_path = os.path.join(visualisation_input_path, economy_id, file)
                        #move the file to the new file path
                        if not os.path.exists(os.path.join(visualisation_input_path, economy_id)):
                            os.makedirs(os.path.join(visualisation_input_path, economy_id))
                        shutil.copy(file_path, new_file_path)
#%%
def compare_layout_files_for_latest_year(path1, path2, economy_ids,folder_location, latest_year):
    """
    This function is used to compare the layout files for the latest year. Can be done using an economy id or on all economies. It will join on the non year columns and copare the year columns. It will then output a csv with the differences as well as where indicator for join is not both
        
    path1 = 'results/model_df_wide_tgt_20250221.csv'
    path2 = 'results/model_df_wide_tgt_20250204.csv'
    economy_ids= None
    utils.compare_layout_files_for_latest_year(path1, path2, economy_ids,  latest_year=2022)
    """
    file1 = pd.read_csv(folder_location+path1)
    file2 = pd.read_csv(folder_location+path2)
    if economy_ids is not None:
        file1 = file1[file1['economy'].isin(economy_ids)]
        file2 = file2[file2['economy'].isin(economy_ids)]
    
    if file1.columns.tolist() != file2.columns.tolist():
        raise Exception('The columns in the two files are not the same')
    #make key cols those that arent 4 digit years
    key_cols = [col for col in file1.columns if not re.match(r'\d{4}', col)]
    year_cols = [col for col in file1.columns if re.match(r'\d{4}', col)]
    #make sure that the type of the year cols (the names themselves) is int
    if type(year_cols[0]) == str:
        year_cols = [int(col) for col in year_cols]
        file1.columns = key_cols + year_cols
        file2.columns = key_cols + year_cols
    #keep only latest year
    file1 = file1[key_cols + [latest_year]]
    file2 = file2[key_cols + [latest_year]]
    
    #join the dfs on the index cols and then compare rows
    joined_df = file1.merge(file2, on=key_cols, suffixes=('_PATH1', '_PATH2'), how='outer', indicator=True)
    
    #compare the values in the rows as well as looking at the indicator column
    joined_df['diff'] = joined_df[str(latest_year) + '_PATH1'].astype(float) - joined_df[str(latest_year) + '_PATH2'].astype(float)
    #drop where the diff is 0 or where the indicator is both
    joined_df  = joined_df[(joined_df['diff'] != 0) | (joined_df['_merge'] != 'both')]
    
    #save and print stats
    joined_df.to_csv(f'{folder_location}layout_diff_{economy_ids}_{latest_year}.csv')
    print(joined_df['_merge'].value_counts())
    print(joined_df['diff'].describe())
    return joined_df   


def identify_if_major_supply_data_is_available(SINGLE_ECONOMY_ID):
    #this will be used for when we are running the system and need to know if it is at the stage where there is all the input data we need and so we should set utility_funtions.MAJOR_SUPPLY_DATA_AVAILABLE to True. If so, it will check it is already set to True and if not it will raise an error. If it is not at the stage where all the data is available it will check that it is set to False and if not it will raise an error.
    #the telltale sign that supply data is available will be that there is a file in data\modelled_data\{SINGLE_ECONOMY_ID} and in there i a file with rows for 01_production 02_imports 03_exports of 01_coal 06_crude_oil_and_ngl 08_gas in the sectors and fuels columns respectively..
    #this is important because there are many steps that can only be done once we are sure we have all the data we need before finalising the results/running time consuming checking and adjustment funcitons and so on.
    
    #NOTE. THIS WILL GET CONFUSED IF ANOTHER DATA RETURN CONTAINS ALL THE ROWS FOR SUPPLY DATA AND AT LEAST ONE OF THOSE ROWS HAS A NON ZERO IN BASE YEAR +1. 
    folder_path = f'./data/modelled_data/{SINGLE_ECONOMY_ID}'
    supply_fuels = ['01_coal', '06_crude_oil_and_ngl', '08_gas']
    supply_sectors = ['01_production', '02_imports', '03_exports']
    
    FOUND = False  # Initially assume no file meets the criteria
    if not isinstance(SINGLE_ECONOMY_ID, str):
        return

    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        if os.path.isdir(file_path):
            continue

        if file.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        else:
            raise ValueError(f"File {file} is not a CSV or Excel file.")

        # Check that the required columns exist
        if 'sectors' not in df.columns or 'fuels' not in df.columns:
            raise ValueError(f"File {file} does not have 'sectors' and 'fuels' columns.")

        # Verify that every fuel/sector combination is present in the DataFrame
        all_found = True
        supply_df = pd.DataFrame()
        for fuel in supply_fuels:
            for sector in supply_sectors:
                if df[(df['sectors'] == sector) & (df['fuels'] == fuel)].empty:
                    all_found = False
                    break  # Break out of inner loop if a combination is missing
                else:
                    supply_df = pd.concat([supply_df, df[(df['sectors'] == sector) & (df['fuels'] == fuel)]])
            if not all_found:
                break  # Break out of outer loop if a combination is missing

        if all_found:
            #double check that when you sum up the values for OUTLOOK_BASE_YEAR+1, they are not all 0
            #make all cols strs 
            supply_df.columns = supply_df.columns.astype(str)
            if supply_df[str(OUTLOOK_BASE_YEAR + 1)].sum() == 0:
                breakpoint()
                raise ValueError(f"Supply data seems to be available but all values for {OUTLOOK_BASE_YEAR + 1} are 0. We dont want to be receiving data with supply values but which are all 0s, whether it is from supply modellers or another model. The file in question is {file_path}")
            FOUND = True
            break  # Stop checking further files as one file meets the criteria

    # At this point, FOUND is True if at least one file has all combinations.
    if FOUND:
        #check that MAJOR_SUPPLY_DATA_AVAILABLE is set to True
        if not MAJOR_SUPPLY_DATA_AVAILABLE:
            breakpoint()
            raise ValueError(f"Supply results are available but MAJOR_SUPPLY_DATA_AVAILABLE is not set to True.")
    else:
        #check that MAJOR_SUPPLY_DATA_AVAILABLE is set to False
        if MAJOR_SUPPLY_DATA_AVAILABLE:
            breakpoint()
            raise ValueError(f"Supply results are not available but MAJOR_SUPPLY_DATA_AVAILABLE is set to True.")
    return

def check_russia_base_year(SINGLE_ECONOMY_ID):
    if SINGLE_ECONOMY_ID == '16_RUS':
        #check that the OUTLOOK_BASE_YEAR in utils is == to OUTLOOK_BASE_YEAR_russia in utils, else raise an error
        #and if SINGLE_ECONOMY_ID != '16_RUS' then check that OUTLOOK_BASE_YEAR in utils != OUTLOOK_BASE_YEAR_russia in utils, else raise an error
        if OUTLOOK_BASE_YEAR != OUTLOOK_BASE_YEAR_RUSSIA:
            raise ValueError('OUTLOOK_BASE_YEAR in utils is not equal to OUTLOOK_BASE_YEAR_RUSSIA in utils')
    elif SINGLE_ECONOMY_ID != '16_RUS':
        if OUTLOOK_BASE_YEAR == OUTLOOK_BASE_YEAR_RUSSIA:
            raise ValueError('OUTLOOK_BASE_YEAR in utils is equal to OUTLOOK_BASE_YEAR_RUSSIA in utils')


def move_files_by_templates(economy_id, file_templates, base_path="data/modelled_data"):
    """
    Scans files in data/modelled_data/{economy_id} and moves files that match any
    of the provided file_templates into the common archive folder.
    
    The templates can contain the following placeholders:
      - {ECONOMY_ID}: Will be replaced by the given economy_id.
      - {SCENARIO}: Matches one of "target", "tgt", "reference", or "ref".
      - {YYYY_MM_DD}: Matches a date in either YYYY_MM_DD or YYYYMMDD format.
      
    Files matching any template will be moved to:
      data/modelled_data/{ECONOMY_ID}/archive/<filename>
    
    :param economy_id: A string for the economy ID (e.g., "09_ROK")
    :param file_templates: A list of file template strings.
    :param base_path: The base folder containing economy folders.
    """
    
    economy_folder = os.path.join(base_path, economy_id)
    if not os.path.exists(economy_folder):
        print(f"Economy folder not found: {economy_folder}")
        return

    # Allowed scenario values (case-insensitive)
    scenario_regex = r"(target|tgt|reference|ref)"
    
    # Build a list of tuples: (compiled_regex, expects_date)
    patterns = []
    for template in file_templates:
        expects_date = "{YYYY_MM_DD}" in template

        # Use temporary tokens for the placeholders.
        token_econ = "<<<ECONOMY_ID>>>"
        token_scenario = "<<<SCENARIO>>>"
        token_date = "<<<DATE>>>"
        
        temp_template = template.replace("{ECONOMY_ID}", token_econ)\
                                .replace("{SCENARIO}", token_scenario)\
                                .replace("{YYYY_MM_DD}", token_date)
        # Escape literal parts.
        escaped = re.escape(temp_template)
        # Replace tokens with regex parts.
        escaped = escaped.replace(re.escape(token_econ), re.escape(economy_id))
        escaped = escaped.replace(re.escape(token_scenario), scenario_regex)
        if expects_date:
            # This group matches either YYYY_MM_DD or YYYYMMDD.
            date_pattern = r"(?P<date>\d{4}_?\d{2}_?\d{2})"
            escaped = escaped.replace(re.escape(token_date), date_pattern)
        # Compile the regex for an exact match (case-insensitive).
        pattern = re.compile(f"^{escaped}$", re.IGNORECASE)
        patterns.append((pattern, expects_date))
    
    # Common archive folder: data/modelled_data/{economy_id}/archive
    archive_folder = os.path.join(economy_folder, "archive")
    os.makedirs(archive_folder, exist_ok=True)
    
    # Loop over files in the economy folder.
    for file_name in os.listdir(economy_folder):
        file_path = os.path.join(economy_folder, file_name)
        if not os.path.isfile(file_path):
            continue

        for pattern, _ in patterns:
            if pattern.match(file_name):
                dest_path = os.path.join(archive_folder, file_name)
                shutil.move(file_path, dest_path)
                print(f"Moved '{file_name}' to '{dest_path}'")
                break  # Stop checking other patterns once a match is found.

#%%
# Example usage:
# base_path = "../../data/modelled_data"

# file_templates = [
#     "{ECONOMY_ID}_biofuels_{SCENARIO}_{YYYY_MM_DD}.csv",
#     "{ECONOMY_ID}_biomass_others_supply_{SCENARIO}_{YYYY_MM_DD}.csv",
#     "{ECONOMY_ID}_non_specified_{SCENARIO}_{YYYY_MM_DD}.csv",
#     "{ECONOMY_ID}_other_own_use_{SCENARIO}_{YYYY_MM_DD}.csv",
#     "{ECONOMY_ID}_other_transformation_{SCENARIO}_{YYYY_MM_DD}.csv",
#     "{ECONOMY_ID}_pipeline_transport_{SCENARIO}_{YYYY_MM_DD}.csv"
# ]

# for economy in ALL_ECONOMY_IDS:
#     if economy not in os.listdir(base_path):
#         continue
#     move_files_by_templates(economy, file_templates, base_path)
# move_files_by_templates(economy_id, file_templates, base_path)

#%%
# folder_location = '../../'
# path1 = 'merged_file_energy_05_PRC_20250303.csv'#./../results
# path2 = 'merged_file_energy_05_PRC_20250317.csv'#./../results
# output_location = folder_location
# economy_ids= ['05_PRC']
# compare_layout_files_for_latest_year(path1, path2, economy_ids, folder_location, latest_year=2030)
#%%

# #filter out all non agirculture datafrom all garicultre data rweturns:
# def filter_out_non_agriculture_data_from_agriculture_data(economy):
#     #search in economy modelled data folder and find the sheet that has agriculture in its file name. then filter out all non agriculture data from it. then save again:
#     if not os.path.exists(f'../../data/modelled_data/{economy}'):
#         return
#     for file in os.listdir(f'../../data/modelled_data/{economy}'):
#         if 'agriculture' in file or 'Agriculture' in file:
#             if '.csv' in file:
#                 agriculture_data = pd.read_csv(f'../../data/modelled_data/{economy}/{file}')
#             elif '.xlsx' in file:
#                 raise ValueError('Excel files not supported. cant be sure what is in the other sheets in the file')
            
#             #filter out all non agirculture datafrom all garicultre data rweturns:
#             agriculture_data = agriculture_data[agriculture_data['sub1sectors']=='16_02_agriculture_and_fishing']
#             #double check there is still data in the file
#             if agriculture_data.empty:
#                 raise ValueError('No data in the agriculture file')
#             agriculture_data.to_csv(f'../../data/modelled_data/{economy}/{file}', index=False)
#     return
# #%%
# for economy in ALL_ECONOMY_IDS:
#     filter_out_non_agriculture_data_from_agriculture_data(economy)
# %%

# Initial read of EBT and saving as a CSV ready for additional processing

# Script created by Wu, Taiwan researcher, to take EBT EGEDA data and build a large dataset that will contain all possible variable combinations
# that will be populated by the 9th modelling. This involves removing some EBT fuels and sectors combos and adding in specific modelling combos
# that are not in the EBT (such as type of road transport)

import pandas as pd 
import re
import os
from utility_functions import *

set_working_directory()#from utility_functions.py

def initial_read_and_save():
    
    # read raw data
    # na_values: defines values that should be viewed as nan

    xlsx_raw = './data/00APEC_May2023.xlsx' #directory of raw data file

    sheet_names = pd.ExcelFile(xlsx_raw).sheet_names[:21] #making a list with the first 21 economies' worksheets 

    if USE_SINGLE_ECONOMY:#this will speed up the process a lot. Probably only useful for testing.
        #drop any sheets that arent the single economy. but remove any _'s form single economy name first
        s = SINGLE_ECONOMY.replace('_', '')
        sheet_names = [e for e in sheet_names if e.replace('_', '') == s]
        
    RawEGEDA = pd.read_excel(xlsx_raw,
                            sheet_name = sheet_names,
                            na_values = ['x', 'X', '']) # I don't think there's any x's or X's in the EGEDA xlsx file, but leaving as is (shouldn't make a difference)

    # Check the shape of read-in data
    # Save the economy name (key) in RawEGEDA. We will use it in the for-loop. 
    economies = RawEGEDA.keys()

    # Use for-loop to record the shape of all dataframes 
    shape_of_data = []
    for i in economies:
        dimension = RawEGEDA[i].shape
        shape_of_data.append(dimension)

    # Check the number of rows
    # save the numbers of row of all dataframe
    number_of_rows = []
    for i in shape_of_data:
        number_of_rows.append(i[0])

    # Use the number of row of first dataframe as reference, and check if the others are the same 
    # as it via for-loop. 
    # Save the result in a list. 

    boolean_list = []
    for i in number_of_rows:
        boolean_list.append(i == number_of_rows[0])

    if all(boolean_list) == True: # all() returns True only if all elements in the list are True 
        print("The number of rows are all the same!")
    else:
        print("Number of rows are not the same for all economies.")

    # Check the number of columns

    # save the numbers of columns of all dataframe
    number_of_columns = []
    for i in shape_of_data:
        number_of_columns.append(i[1])

    # Use the number of column of first dataframe as reference, 
    # and check if the others are the same as it via for-loop. 
    # Save the result in a list.

    boolean_list = []
    for i in number_of_rows:
        boolean_list.append(i == number_of_columns[0])

    if all(boolean_list) == True: # all() returns True only if all elements in the list are True
        print("The number of columns are all the same!")
    else:
        print("Number of columns are not the same for all economies.")

    # Name the first two columns which are currently blank

    for i in economies:
        RawEGEDA[i].rename(columns = {'Unnamed: 0': 'fuels', 
                                    'Unnamed: 1': 'sectors'}, inplace = True)
        
    # Prepare lists for dataframe merging process later

    years = list(range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1, 1))

    economy_dict = pd.read_csv('./config/economy_dict.csv', 
                            header = None, 
                            index_col = 0).squeeze('columns').to_dict()

    # From dict to dataframe and From wide-df to long df 
    # - In the for-loop, we melted the wide-df into long-df and saved them in a list one after another. 
    # - Then we concat all df in the list into a large one.

    df_list = []

    for i in economies:
        temp_ebt_df_economy_wide = RawEGEDA[i]

        temp_ebt_df_economy_long = pd.melt(temp_ebt_df_economy_wide, 
                                        id_vars = ['fuels', 'sectors'], 
                                        value_vars = years, 
                                        var_name = 'year', 
                                        value_name = 'value')
        
        temp_ebt_df_economy_long['economy'] = economy_dict[i] # add a new col called 'economy', which is in the format that we preferred (e.g., 01_AUS).
        
        temp_ebt_df_economy_long = temp_ebt_df_economy_long.set_index(['economy', 'year']) # Use economy and year as index 
        
        df_list.append(temp_ebt_df_economy_long) # Save all df into the list 

    df = pd.concat(df_list) # vertical combine

    # Standardisation
    # - standarlize col names and variable names
    # - blank, special symbols are not allowed 
    # - use underscore if necessary. 
    # - notice that we replace '&' with 'and'.

    # And remove multiple spaces from variables

    df['fuels'] = df['fuels'].replace('\s+', ' ', regex = True)
    df['sectors'] = df['sectors'].replace('\s+', ' ', regex = True)

    # Move everything to lower case
    df['fuels'] = df['fuels'].str.lower()
    df['sectors'] = df['sectors'].str.lower()

    # fuels_code
    df['fuels'] = df['fuels'].str.replace(' ', '_', regex = False)\
                                        .str.replace('.', '_', regex = False)\
                                        .str.replace('/', '_', regex = False)\
                                        .str.replace('(', '', regex = False)\
                                        .str.replace(')', '', regex = False)\
                                        .str.replace('-', '', regex = False)\
                                        .str.replace(',', '', regex = False)\
                                        .str.replace('&', 'and', regex = False)\
                                        .str.replace('___', '_', regex = False)\
                                        .str.replace('__', '_', regex = False)\
                                        .str.replace(':', '', regex = False)\
                                        .str.replace('liqour', 'liquor', regex = False)\
                                        .str.rstrip('_')

    # sectors_code
    df['sectors'] = df['sectors'].str.replace(' ', '_', regex = False)\
                                    .str.replace('.', '_', regex = False)\
                                    .str.replace('/', '_', regex = False)\
                                    .str.replace('(', '', regex = False)\
                                    .str.replace(')', '', regex = False)\
                                    .str.replace('-', '', regex = False)\
                                    .str.replace(',', '', regex = False)\
                                    .str.replace('&', 'and', regex = False)\
                                    .str.replace('___', '_', regex = False)\
                                    .str.replace('__', '_', regex = False)\
                                    .str.replace(':', '', regex = False)\
                                    .str.rstrip('_')

    # Transfer item_number into two digits 

    df['fuels'] = df['fuels'].apply(lambda x: re.sub(r'\d+', lambda y: y.group(0).zfill(2), x))
    df['sectors'] = df['sectors'].apply(lambda x: re.sub(r'\d+', lambda y: y.group(0).zfill(2), x))

    ########################################################################################################################
    # Comparison between current layout and your self-defined layout (比較 WU_layout 跟迄今的資整結果)
    # - The above process convert the EGEDA raw data into a dataframe in a well-accepted format (df).
    # - However, the ultimate purpose of this file is to prepare a modeller-friendly historical data and layout for 
    # modeller to use and fill in their model result. 
    # - We probably need to make some new definition or add new rows to the energy balance table (EBT). Let's call it 
    # sd_layout here.
    #     - We created a new datafolder in data folders and put the sd_layout of fuel and sector in it.
    #         - .\data\fuel_list.xlsx
    #         - .\data\sector_list.xlsx
    # - The features of sd_layout
    #     - multiple cols for sector(5) and fuels(2) (df only have one sector col and one fuel col)
    #     - if we use the last_col for sector and fuel of the sd_layout, the layout would be similar to current df.
    #     - the format generally follow the above rules
    #     - **item number should not be the same as the current df** because we probably do aggregation or add new 
    # fuels/sectors already.
    # - So the idea here would be  
    #     1. (**Do it in python**) export the current df to excel file  
    #         - .\temp\clean_egeda_fuel_name.xlsx
    #         - .\temp\clean_egeda_sector_name.xlsx
    #     2. create new files 
    #         - .\data\reference_table_fuel.xlsx 
    #         - .\data\reference_table_sector.xlsx
    #     3. copy the clean_egeda cols and the sd_layout, and then paste them to the excel files that you just created  
    #     4. align the name of fuels and sector, and mark the differences in other cols.
    #     5. (**Do it in python**) Do final check for the remaining inconsistent parts in python 
    #       (**notice that we do not revise in python but in excel**)
    #         - read the excel files, drop the item numbers, and do the comparison in this file.  
    #         - You may find the cases like
    #             - exactly the same
    #             - minor difference
    #             - exist in current df but not sd_layout (aggregation)
    #             - exist in sd_layout but not current df (new-defined or minus)
    #     4. fix the minor difference in excel and save it as new excel file (e.g., reference_table_fuel_revised.xlsx).
    #     5. import the file (except for item number, there should not be minor difference anymore)
    # - (Important) Paste the revised fule and sector cols to these files which record almost every edition of the 
    # fuel and sector name. Remember to revise the multiple cols and make it the same data format, we will use this file 
    # later.
    #     - EBT_column_fuels.xlsx
    #     - EBT_row_sectors.xlsx
    ########################################################################################################################

    # Export the fuel and sector columns from df

    unique_fuels = df['fuels'].unique()
    unique_sectors = df['sectors'].unique()

    unique_fuels_df = pd.DataFrame({'clean_egeda_fuel_name': unique_fuels})

    unique_sectors_df = pd.DataFrame({'clean_egeda_sector_name': unique_sectors})

    folder_path = './data/temp'
    os.makedirs(folder_path, exist_ok = True)

    result_path = os.path.join(folder_path, 'clean_egeda_fuel_name.xlsx')
    unique_fuels_df.to_excel(result_path, index = False)

    result_path = os.path.join(folder_path, 'clean_egeda_sector_name.xlsx')
    unique_sectors_df.to_excel(result_path, index = False)

    # (Do this in excel) Compare the self-defined layout 
    # - create new files 
    #     - .\manual_adjust\reference_table_fuel.xlsx 
    #     - .\manual_adjust\reference_table_sector.xlsx
    # - copy the clean_egeda cols and the sd_layout, and then paste them to the excel files that you just created  
    # - align the name of fuels and sector, and mark the differences in other cols.

    # Check if there are still differences in the fuel name (ignore the item number)
    fuel_reference_table = pd.read_excel('./config/reference_table_fuel.xlsx')

    # drop the item number 
    for i in range(0, 2, 1):
        fuel_reference_table['clean_egeda_fuel_name'] = fuel_reference_table['clean_egeda_fuel_name'].str.replace(r'^\d*_', '', regex = True)
        fuel_reference_table['unique_the_end_of_fuels'] = fuel_reference_table['unique_the_end_of_fuels'].str.replace(r'^\d*_', '', regex = True)

    # list the differences
    fuel_reference_table['not_equal'] = fuel_reference_table['clean_egeda_fuel_name'] != fuel_reference_table['unique_the_end_of_fuels']

    diff_fuel = fuel_reference_table[fuel_reference_table['not_equal'] == True].sort_values(['how', 'unique_the_end_of_fuels'])

    # # format difference that should be revise in excel
    # diff_fuel[(diff_fuel['clean_egeda_fuel_name'].notnull()) & (diff_fuel['unique_the_end_of_fuels'].notnull())]

    # Check if there are still differences in the sector name (ignore the item number)

    sector_reference_table = pd.read_excel('./config/reference_table_sector.xlsx')

    # drop the item number 
    for i in range(0, 3, 1):
        sector_reference_table['clean_egeda_sector_name'] = sector_reference_table['clean_egeda_sector_name'].str.replace(r'^\d*_', '', regex = True)
        sector_reference_table['unique_the_end_of_sectors'] = sector_reference_table['unique_the_end_of_sectors'].str.replace(r'^\d*_', '', regex = True)

    # list the differences
    sector_reference_table['not_equal'] = sector_reference_table['clean_egeda_sector_name'] != sector_reference_table['unique_the_end_of_sectors']

    diff_sector = sector_reference_table[sector_reference_table['not_equal'] == True].sort_values(['how', 'unique_the_end_of_sectors'])

    # (Do it in excel) Fix the format difference
    # - Ideally, the item names of cleaned EGEDA and self-defined layout should be the same.
    # - This section gives you a change to check and revise accidental difference.
    # - ```diff_fule```and ```diff_sector``` allow you to check the difference.
    # ---
    # - "format difference that should be revise in excel" shows data format you should revise in excel
    # - Afetr revision, except for the item number, the data format should be the same.
    # - Save them as new files 
    #     - .\data\reference_table_fuel_revised.xlsx 
    #     - .\data\reference_table_sector_revised.xlsx 
        
    # ---
    # FYI, the differences in data format
    # 1. Replace & with
    #     - Wu: _
    #     - Mat: and
    # 2. unnecessary commas
    # 3. typos from original EGEDA dataset: It should be liquor instead of liqour.

    # Connect the sd_layout with the EGEDA historical data (df)
    # 1. Improt the revised reference table
    # 2. "clean_egeda_fuel_name" and "fuels" have the same values but different data points. We use this property to 
    # connect the historical data (df) to sd_layout.
    # 3. Create rows (new rows, minus) and aggregate rows (create new ones and discard old ones).
    # 4. Merge the multiple cols (indicate different levels) to the EGEDA historical data (df).

    # Replace the data in "fuels" series with "unique_the_end_of_fuels" series

    # We don't need the multiple index (ecnomy, year)
    df_no_year_econ_index = df.reset_index()

    # # interim save
    # interim_path = './data/interim/'
    # os.makedirs(interim_path, exist_ok = True)
    # if USE_SINGLE_ECONOMY:
    #     df_no_year_econ_index.to_csv(interim_path + f'EBT_long_{SINGLE_ECONOMY}.csv', index = False)
    # else:
    #     df_no_year_econ_index.to_csv(interim_path + 'EBT_long.csv', index = False)

    return df_no_year_econ_index

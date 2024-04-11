"""A script to merge the layout file and the demand output results files."""
#%%
import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from utility_functions import *
import merging_functions
import warnings
    
def merging_results(original_layout_df, SINGLE_ECONOMY_ID, previous_merged_df_filename=None):
    """Takes the layout file and the results files, and merges them together.  The results files are generated from the model runs in the demand model, and the layout file is generated from the historical data file and the layout file template. 
    The process is messy and in some places doesnt seem to take the simplest way forwards. Most of the time this is because of compromises that are made because of the different level of detail between inputs, and the layout file. So the code here is complex, but relatively flexible.
    Some issues with the structure of the data that we need to deal with:
    - subtotals are not consistently calcaulted in any of the layout or results data so we label them, remove them and then recalcautle them always. 
    - layout file contains less specific data than the categories are for, so for example, even if there is a possible category for passenger air transprot, the layout file will only have data for air transport. So this is partly why calculate subtotals in results and layout data separately, and also why the merge is an outer join.

    """
    # layout_df = pd.read_csv(layout_file)
    layout_df = original_layout_df.copy()

    #extract unique economies:
    economies = layout_df['economy'].unique()

    # Specify the shared category columns in the desired order
    shared_categories = ['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels']
    years_to_keep_in_results = [str(year) for year in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1)]

    # Create an empty concatted_results_df with the shared_categories
    concatted_results_df = pd.DataFrame(columns=shared_categories)

    if (isinstance(SINGLE_ECONOMY_ID, str)):
        # Define the path pattern for the results data files
        results_data_path = 'data/demand_results_data/'+SINGLE_ECONOMY_ID+'/*'
        print(results_data_path)
    else:
        print("Not implemented yet.")

    # Define the path pattern for the results data files
    #results_data_path = 'data/demand_results_data/*'
    # Get a list of all matching results data file paths
    results_data_files = [f for f in glob.glob(results_data_path) if os.path.isfile(f)]
    # Check if results_data_files is empty
    if not results_data_files:
        print("No files found in the specified path.")
        # Exit the function
        return None


    #FIRST, LOAD IN ALL THE RESULTS FILES AND CONCAT THEM TOGETHER. THEN WE CAN MERGE THEM WITH THE LAYOUT FILE AND IDENTIFY ANY STRUCTURAL ISSUES
    # Iterate over the results files
    for file in results_data_files:
        print(f"Start processing {file}...")
        # Check if the file is an Excel file
        if file.endswith('.xlsx'):
            xls = pd.ExcelFile(file)

            # Check if 'agriculture' is in the file name (case-insensitive)
            if 'agriculture' in file.lower():
                # Check if the last sheet is either 'output' or 'fishing output' or 'agriculture output'
                last_sheet_name = xls.sheet_names[-1].lower()
                if last_sheet_name == 'output' or last_sheet_name == 'fishing output' or last_sheet_name == 'agriculture output':
                    results_df = merging_functions.process_agriculture(file, shared_categories, economies, OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR)
                else:
                    results_df = pd.read_excel(file, sheet_name=-1)  # Processing for already transformed agriculture files
            else:
                results_df = pd.read_excel(file, sheet_name=-1)  # Processing for non-agriculture files

            print(f"Processing Excel file {file}...")

        # Process CSV files
        elif file.endswith('.csv'):
            # Check if 'pipeline' or 'buildings' is in the file name (case-insensitive)
            if 'pipeline' in file.lower(): # or 'buildings' in file.lower():
                results_df = merging_functions.split_subfuels(file, layout_df, shared_categories, OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR) # Split subfuels for pipeline and buildings files
                print(f"Processing CSV file {file} with split subfuels...")
            else:
                results_df = pd.read_csv(file)
                print(f"Processing CSV file {file}...")

        # Handle unsupported file formats
        else:
            print(f"Unsupported file format: {file}")
            continue

        # Reorder the shared categories in the results DataFrame
        results_df = results_df[shared_categories + list(results_df.columns.difference(shared_categories))]

        # Convert columns to string type
        results_df.columns = results_df.columns.astype(str)

        #Keep columns from outlook_base_year to outlook_last_year only
        results_df.drop(columns=[str(col) for col in results_df.columns if any(str(year) in col for year in range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1))], inplace=True)
        
        #filter for only economies in the layout file:
        results_df = results_df[results_df['economy'].isin(economies)]
        #drop where all vlaues in a row are 0. these arent needed as the rows will come from the layout file
        results_df = merging_functions.remove_all_zeros(results_df, years_to_keep_in_results)
        
        #TEMP# 
        #buildings file currently includes all sectors historical data, just like the layout file, not just buildings. This is a temporary fix to remove the non-buildings sectors from the results file.
        results_df = merging_functions.filter_for_only_buildings_data_in_buildings_file(results_df)
        results_df = merging_functions.filter_out_solar_with_zeros_in_buildings_file(results_df)
        results_df = merging_functions.power_move_x_in_chp_and_hp_to_biomass(results_df)
        #TEMP#
        
        # find sectors where there are null values for all the years base_year->end_year. This will help to identify where perhaps the results file is missing data or has been incorrectly formatted.
        null_sectors = results_df.loc[results_df.isnull().all(axis=1), 'sectors'].unique().tolist()#[years_to_keep]
        if null_sectors:
            print(f"Full rows of null values found in {file} for the following sectors:")
            print(null_sectors)
        filtered_results_df = results_df[~results_df['sectors'].isin(null_sectors)].copy()
        
        #########RUN COMMON CHECKS ON THE RESULTS FILE.#########
        merging_functions.check_bunkers_are_negative(filtered_results_df, file)
        merging_functions.check_for_differeces_between_layout_and_results_df(layout_df, filtered_results_df, shared_categories, file)
        #########RUN COMMON CHECKS ON THE RESULTS FILE OVER.#########
        basename = os.path.basename(file)
        filtered_results_df['origin'] = basename.split('.')[0]
        #the origin col is used because some data will come from two different results files, yet have the same sector and fuels columns but different levels of detail. This means that after we remove subtotals and then try to recreate them in calculate_subtotals, we might end up with duplicate rows. So we need to be able to identify that these rows came from different origin files so the duplicates can be removed by being summed together.
        
        filtered_results_df_subtotals_labelled = merging_functions.label_subtotals(filtered_results_df, shared_categories + ['origin'])
        # Combine the results_df with all the other results_dfs we have read so far
        concatted_results_df = pd.concat([concatted_results_df, filtered_results_df_subtotals_labelled])

    # Define the range of years to keep
    years_to_keep = set(range(EBT_EARLIEST_YEAR, OUTLOOK_LAST_YEAR + 1))

    # Filter out columns beyond OUTLOOK_LAST_YEAR
    # Convert column names to strings for comparison, as some might be integers
    concatted_results_df = concatted_results_df[[col for col in concatted_results_df.columns if str(col).isdigit() and int(col) in years_to_keep or not str(col).isdigit()]]

    #ONLY CALCUALTE SUBTOTALS ONCE WE HAVE CONCATTED ALL RESULTS TOGETHER, SO WE CAN GENERATE SUBTOTALS ACROSS RESUTLS. I.E. 09_total_transformation_sector
    concatted_results_df = merging_functions.calculate_subtotals(concatted_results_df, shared_categories + ['origin'], DATAFRAME_ORIGIN='results')
    concatted_results_df.to_csv('data/temp/error_checking/concatted_results_df.csv')
    ##############################
    
    ###NOW WE HAVE THE concatted RESULTS DF, WITH SUBTOTALS CALCAULTED. WE NEED TO MERGE IT WITH THE LAYOUT FILE TO IDENTIFY ANY STRUCTURAL ISSUES####
    layout_df = layout_df[layout_df['economy'].isin(economies)].copy()
    #drop years in range(OUTLOOK_BASE_YEAR, OUTLOOK_BASE_YEAR+1) as we dont need it. This will help to speed up the process. 
    
    layout_df.drop(columns=[col for col in layout_df.columns if any(str(year) in str(col) for year in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1))], inplace=True)
    # layout_df_subtotals_labelled = merging_functions.label_subtotals(layout_df, shared_categories) #now has been moved to C_subset_data.py
    layout_df_subtotals_recalculated = merging_functions.calculate_subtotals(layout_df, shared_categories, DATAFRAME_ORIGIN='layout')
    
    ############################## 
    trimmed_layout_df, missing_sectors_df = merging_functions.trim_layout_before_merging_with_results(layout_df_subtotals_recalculated,concatted_results_df)
    trimmed_concatted_results_df = merging_functions.trim_results_before_merging_with_layout(concatted_results_df, shared_categories)
    #rename subtotal columns before merging:
    trimmed_concatted_results_df.rename(columns={'is_subtotal': 'subtotal_results'}, inplace=True)
    trimmed_layout_df.rename(columns={'is_subtotal': 'subtotal_layout'}, inplace=True)
    missing_sectors_df.rename(columns={'is_subtotal': 'subtotal_layout'}, inplace=True)
    # Merge the new_layout_df with the concatted_results_df based on shared_categories using outer merge (so we can see which rows are missing/extra from the results_df)
    merged_df = pd.merge(trimmed_layout_df, trimmed_concatted_results_df, on=shared_categories, how="outer", indicator=True)
    
    results_layout_df = merging_functions.format_merged_layout_results_df(merged_df, shared_categories, trimmed_layout_df, trimmed_concatted_results_df,missing_sectors_df)
    
    results_layout_df.to_csv('results_layout_df_before_drop.csv')
    
    #add subtotals to shared_categories now its in all the dfs
    shared_categories_w_subtotals = shared_categories + ['subtotal_layout', 'subtotal_results']
    #########################
    #NOW CALCAULTE THE AGGREGATES WHICH ARE COMBINATIONS OF SECTORS FROM DIFFERENT MODELLERS RESULTS. EVEN THOUGH THE LAYOUT DATA ALREADY CONTAINS THESE AGGREGATES, WE WILL RECALCULATE THEM AS A WAY OF TESTING THAT THE MERGES DONE UNTIL NOW ARE CORRECT. 
    #now, in case they are there, drop the aggregate sectors (except total transformation) from the merged data so we can recalculate them
    new_aggregate_sectors = ['12_total_final_consumption', '13_total_final_energy_consumption', '07_total_primary_energy_supply']
    results_layout_df = results_layout_df.loc[~results_layout_df['sectors'].isin(new_aggregate_sectors)].copy()
    #and drop aggregate fuels since we will recalculate them
    results_layout_df = results_layout_df.loc[~results_layout_df['fuels'].isin(['19_total', '21_modern_renewables', '20_total_renewables'])].copy()
    
    results_layout_df.to_csv('results_layout_df_after_drop.csv')

    # Define a dictionary that maps each sector group to its corresponding total column
    sector_mappings = [
        (['14_industry_sector', '15_transport_sector', '16_other_sector', '17_nonenergy_use'], '12_total_final_consumption'),
        (['14_industry_sector', '15_transport_sector', '16_other_sector'], '13_total_final_energy_consumption'),
        (['01_production','02_imports', '03_exports', '06_stock_changes', '04_international_marine_bunkers','05_international_aviation_bunkers', '08_transfers',  '09_total_transformation_sector', '10_losses_and_own_use', '11_statistical_discrepancy', '14_industry_sector', '15_transport_sector', '16_other_sector', '17_nonenergy_use'], '07_total_primary_energy_supply'),#'09_total_transformation_sector', #TODO IM NOT SURE IF IVE CALCUALTED TPES RIGHT 
        (['01_production', '09_total_transformation_sector'], '01_production')
        #'10_losses_and_own_use', '11_statistical_discrepancy',#,
        # (['09_total_transformation_sector'], '09_total_transformation_sector')
    ]

    # Initialize an empty dictionary to store the resulting DataFrames
    sector_aggregates_df = pd.DataFrame()
    # Loop over the sector mappings and process the data for each sector group
    for (sectors, aggregate_sector) in sector_mappings:
        sector_df = merging_functions.calculate_sector_aggregates(results_layout_df, sectors, aggregate_sector, shared_categories, shared_categories_w_subtotals)
        sector_aggregates_df = pd.concat([sector_aggregates_df, sector_df])
    
    # Drop '01_production' from results_layout_df as it is included in sector_aggregates_df
    results_layout_df = results_layout_df[results_layout_df['sectors'] != '01_production'].copy()
    
    # Calculate the subtotals in the sector aggregates
    sector_aggregates_df = merging_functions.calculating_subtotals_in_sector_aggregates(sector_aggregates_df, shared_categories_w_subtotals)
    
    # Label the subtotals in the sector aggregates
    sector_aggregates_df = merging_functions.label_subtotals(sector_aggregates_df, shared_categories)
    sector_aggregates_df.rename(columns={'is_subtotal': 'subtotal_layout'}, inplace=True)
    sector_aggregates_df['subtotal_results'] = sector_aggregates_df['subtotal_layout']
    
    # Ensure the index is consistent after concatenation if needed
    sector_aggregates_df.reset_index(drop=True, inplace=True)
    fuel_aggregates_df = merging_functions.calculate_fuel_aggregates(sector_aggregates_df, results_layout_df, shared_categories)
    
    final_df = merging_functions.create_final_energy_df(sector_aggregates_df, fuel_aggregates_df,results_layout_df, shared_categories)
    #now check for issues with the new aggregates and subtotals by using the layout file as the reference    
    merging_functions.check_for_issues_by_comparing_to_layout_df(final_df, shared_categories_w_subtotals, new_aggregate_sectors, layout_df, REMOVE_LABELLED_SUBTOTALS=False)
    #######################################
    #FINALISE THE DATA
    
    #set up the order of columns to be shared_cateogires, subtotal_results, subtotal_layout, then the years in order
    final_df = final_df[shared_categories_w_subtotals +  [col for col in final_df.columns if col not in shared_categories_w_subtotals]]
    
    # final_df with rows ordered in the same sequence as layout_df based on the columns in shared_categories
    final_df = layout_df[shared_categories].merge(final_df, on=shared_categories, sort=False)
    
    #############################
    # Temp fix for 01_production 15_solid_biomass and 16_others subtotal label
    # Change TRUE to FALSE under 'subtotal_results' column if it's '01_production' in 'sectors' and '15_solid_biomass' or '16_others' in 'fuels'
    final_df.loc[(final_df['sectors'] == '01_production') & (final_df['fuels'].isin(['15_solid_biomass', '16_others'])), 'subtotal_results'] = False
    #############################
    
    # Define the folder path where you want to save the file
    folder_path = f'results/{SINGLE_ECONOMY_ID}/merged'
    # Check if the folder already exists
    if not os.path.exists(folder_path) and (isinstance(SINGLE_ECONOMY_ID, str)):
        # If the folder doesn't exist, create it
        os.makedirs(folder_path)
    
    merging_functions.compare_to_previous_merge(final_df, shared_categories_w_subtotals, results_data_path=folder_path,previous_merged_df_filename=previous_merged_df_filename, new_subtotal_columns=['subtotal_layout', 'subtotal_results'], previous_subtotal_columns=['subtotal_historic','subtotal_predicted','subtotal'])

    #save the combined data to a new Excel file
    #layout_df.to_excel('../../tfc/combined_data.xlsx', index=False, engine='openpyxl')
    date_today = datetime.now().strftime('%Y%m%d')
    if (isinstance(SINGLE_ECONOMY_ID, str)):
        final_df.to_csv(f'{folder_path}/merged_file_energy_{SINGLE_ECONOMY_ID}_{date_today}.csv', index=False)
    else:
        final_df.to_csv(f'results/merged_file_energy_{date_today}.csv', index=False)
        
    return final_df


# #%%
# a = merging_results()
# a.to_csv('new.csv')
#%%




# %%

"""A script to merge the layout file and the demand output results files."""
#%%
import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from utility_functions import *
import merging_functions

def merging_results(original_layout_df, previous_merged_df_filename=None):
    """todo explain why this is so big and what it does. defintiely want to explain the issues with the x's. 
    
    Some useful definitiions:
    catergories are the names of the columns in the layout file that arent years or values. eg.['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels']

    Returns:
        _type_: _description_
    """
    # #read the layout file
    # if USE_SINGLE_ECONOMY:
    #     layout_file = glob.glob(f'../../results/model_df_wide_{SINGLE_ECONOMY}_202*.csv')
    # else:
    #     layout_file = glob.glob('../../results/model_df_wide_202*.csv')
    
    # try:
    #     original_layout_df = pd.read_csv('a.csv')
    # except:
    #     os.chdir('../../')
    #     original_layout_df = pd.read_csv('a.csv')
    # if len(layout_file) == 0:
    #     print("Layout file not found.")
    #     exit()

    # layout_file = layout_file[0]

    # layout_df = pd.read_csv(layout_file)
    layout_df = original_layout_df.copy()

    #convert all col names to str since loaded csvs will have int columns
    layout_df.columns = layout_df.columns.astype(str)
    
    #extract unique economies:
    economies = layout_df['economy'].unique()


    # Specify the shared category columns in the desired order
    shared_categories = ['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels']

    # # Create a list of column names from 2021 to 2070
    # columns_2021_to_2070 = [str(year) for year in range(2021, 2071)]

    # # Combine the shared_categories and year columns
    # columns = shared_categories + columns_2021_to_2070

    # Create an empty concatted_results_df with the shared_categories
    concatted_results_df = pd.DataFrame(columns=shared_categories)

    if USE_SINGLE_ECONOMY:
        # Define the path pattern for the results data files
        results_data_path = 'data/demand_results_data/'+SINGLE_ECONOMY+'/*'
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

    # Iterate over the results files
    for file in results_data_files:
        # Read the results file
        if file.endswith('.xlsx'):
            results_df = pd.read_excel(file, sheet_name=-1)
        elif file.endswith('.csv'):
            results_df = pd.read_csv(file)
        else:
            print(f"Unsupported file format: {file}")
            continue
        print(f"Processing {file}...")
        # Reorder the shared categories in the results DataFrame
        results_df = results_df[shared_categories + list(results_df.columns.difference(shared_categories))]

        # Convert columns to string type
        results_df.columns = results_df.columns.astype(str)

        # # Keep columns from oulook_base_year to outlook_last_year
        # years_to_keep = [str(year) for year in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1)]
        # results_df = results_df[shared_categories + years_to_keep]
        
        #filter for only economies in the layout file:
        results_df = results_df[results_df['economy'].isin(economies)]

        #TEMP# 
        #buildings file currently includes all sectors historical data, just like the layout file, not just buildings. This is a temporary fix to remove the non-buildings sectors from the results file.
        results_df = merging_functions.filter_for_only_buildings_data_in_buildings_file(results_df)
        #TEMP#
        # find sectors where there are null values for all the years base_year->end_year. This will help to identify where perhaps the results file is missing data or has been incorrectly formatted.
        null_sectors = results_df.loc[results_df.isnull().all(axis=1), 'sectors'].unique().tolist()#[years_to_keep]
        if null_sectors:
            print(f"Full rows of null values found in {file} for the following sectors:")
            print(null_sectors)
        filtered_results_df = results_df[~results_df['sectors'].isin(null_sectors)].copy()
        
        # # Drop columns with years within 1980 and 2020 in the results DataFrame
        # filtered_results_df.drop(columns=[col for col in filtered_results_df.columns if any(str(year) in col for year in range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1))], inplace=True)

        #########RUN COMMON CHECKS ON THE RESULTS FILE.#########
        merging_functions.check_bunkers_are_negative(filtered_results_df, file)
        merging_functions.check_for_differeces_between_layout_and_results_df(layout_df, filtered_results_df, shared_categories, file)#TODO WILL THIS STILL WORK IF WE KEEP ALL YEARS IN THE RESULTS FILE? I THINK SO.
        #########RUN COMMON CHECKS ON THE RESULTS FILE OVER.#########
        #TESTING, WILL THIS HELP WITH AVOIDING CALCULATING SUBTOTALS USING PREEXISTING SUBTOTALS? IF THERE IS ALREADY A SUBTOTAL SHOULD WE RECALCAULTE IT? PERHAPS WE ALSO NEED TO HAVE A COLUMN FOR SPECIFIYING THE MOST SPECIFIC DATA LEVEL FOR EACH ROW
        filtered_results_df = merging_functions.label_subtotals_handler(filtered_results_df, shared_categories)
        
        aggregated_x_df = merging_functions.calculate_subtotals(filtered_results_df, shared_categories)
        # Combine the results_df with all the other results_dfs we have read so far
        concatted_results_df = pd.concat([concatted_results_df, aggregated_x_df])
    
    ###NOW WE HAVE THE concatted RESULTS DF, WITH SUBTOTALS CALCAULTED. WE NEED TO MERGE IT WITH THE LAYOUT FILE TO IDENTIFY ANY STRUCTURAL ISSUES####
    layout_df = layout_df[layout_df['economy'].isin(economies)].copy()
    
    layout_df = merging_functions.label_subtotals_handler(layout_df, shared_categories)
    layout_df = merging_functions.calculate_subtotals(layout_df, shared_categories) 
    
    trimmed_layout_df, missing_sectors_df = merging_functions.trim_layout_before_merging_with_results(layout_df,concatted_results_df)
    trimmed_concatted_results_df = merging_functions.trim_results_before_merging_with_layout(concatted_results_df, shared_categories)
    
    #rename subtotal columns before merging:
    trimmed_concatted_results_df.rename(columns={'subtotal': 'subtotal_results'}, inplace=True)
    trimmed_layout_df.rename(columns={'subtotal': 'subtotal_layout'}, inplace=True)
    
    # Merge the new_layout_df with the concatted_results_df based on shared_categories using outer merge (so we can see which rows are missing/extra from the results_df)
    
    merged_df = pd.merge(trimmed_layout_df, trimmed_concatted_results_df, on=shared_categories, how="outer", indicator=True)
    
    merged_df = merging_functions.run_checks_on_merged_layout_results_df(merged_df, shared_categories, trimmed_layout_df, trimmed_concatted_results_df)
    
    # Combine the remainign rows form the original layout_df with the merged_df
    results_layout_df = pd.concat([missing_sectors_df, merged_df])
    # #### FIND SUBTOALS HERE. ITS IMPORTANT BECAUSE WE DONT WANT TO INCLUDE THEM IN CALCUALTIONS WITH NON SUBTOTALS###
    
    # results_layout_df = merging_functions.identify_and_label_subtotals_handler(results_layout_df, shared_categories)
    
    # #label where there is a subtotal in either layout or results, or the value is in the 17_nonenergy_use sector. this is so we can calcualte the energy aggregates next.
    # # results_layout_df['not_included_in_energy_aggregate'] = ~((results_layout_df['subtotal_historic'] == False) & (results_layout_df['subtotal_predicted'] == False))# | (results_layout_df['sectors'] == '17_nonenergy_use')
    #add subtotals to shared_categories now its in all the dfs
    shared_categories = shared_categories + ['subtotal_layout', 'subtotal_results']#, 'not_included_in_energy_aggregate']
    #########################

    # Create a list of columns to drop
    aggregating_columns_to_drop = ['sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors'] + [year for year in range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1) if year in layout_df.columns]

    # Drop the specified columns and year columns from the layout_df DataFrame
    aggregates_df = results_layout_df.drop(aggregating_columns_to_drop, axis=1).copy()

    #separate results_layout_df into the layout and results dfs before calculating the aggregates. this is because they have different detail levels
    
    
    # Define a dictionary that maps each sector group to its corresponding total column
    sector_mappings = [
        (['14_industry_sector', '15_transport_sector', '16_other_sector', '17_nonenergy_use'], '12_total_final_consumption'),
        (['14_industry_sector', '15_transport_sector', '16_other_sector'], '13_total_final_energy_consumption'),
        (['09_total_transformation_sector', '10_losses_and_own_use', '11_statistical_discrepancy'], '07_total_primary_energy_supply'),
        (['09_total_transformation_sector'], '09_total_transformation_sector')
    ]

    # Initialize an empty dictionary to store the resulting DataFrames
    concatted_grouped_df = pd.DataFrame()
    # Loop over the sector mappings and process the data for each sector group
    for (sectors, aggregate_sector) in sector_mappings:
        sector_df = merging_functions.calculate_sector_aggregates(aggregates_df, sectors, aggregate_sector, shared_categories)
        concatted_grouped_df = pd.concat([concatted_grouped_df, sector_df])
        
    # concatted_grouped_df.drop(columns=['subtotal'], inplace=True)
    # Ensure the index is consistent after concatenation if needed
    concatted_grouped_df.reset_index(drop=True, inplace=True)
    
    concatted_grouped_df = merging_functions.aggregate_19_total(concatted_grouped_df, shared_categories)
    concatted_grouped_df = merging_functions.aggregate_aggregates(concatted_grouped_df, shared_categories)
    
    #######################################
    #finalise the data by merging the layout and results dataframes. 

    # Get the unique sectors and sub1sectors
    aggregate_sectors_list = concatted_grouped_df['sectors'].unique().tolist()
    aggregate_sub1sectors_list = concatted_grouped_df['sub1sectors'].unique().tolist()

    # Create conditions for checking both 'sectors' and 'sub1sectors'
    sector_condition = results_layout_df['sectors'].isin(aggregate_sectors_list)
    sub1sector_condition = results_layout_df['sub1sectors'].isin(aggregate_sub1sectors_list)

    # Create a new DataFrame with rows that match both the sectors and sub1sectors from the results DataFrame
    new_aggregate_results_layout_df = results_layout_df[sector_condition & sub1sector_condition].copy()

    # Drop the rows that were updated in the new DataFrame from the original layout DataFrame
    non_aggregates_results_layout_df = results_layout_df[~(sector_condition & sub1sector_condition)].copy()#TODO CHECK THAT THERE ARE NO AGGREGATES HERE

    # Drop columns outllook_base_year to outlook_last_year from  the new aggregated DataFrame
    new_aggregate_layout_df = new_aggregate_results_layout_df.drop(columns=[year for year in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1)])#.copy()
    #i think we drop pre base year years from concatted_grouped_df now
    concatted_grouped_df.drop(columns=[year for year in range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1)], inplace=True)
    # Merge the DataFrames based on the shared category columns
    aggregate_merged_df = new_aggregate_layout_df.merge(concatted_grouped_df, on=shared_categories, how='left')

    # Combine the original layout_df with the merged_df
    new_results_layout_df = pd.concat([non_aggregates_results_layout_df, aggregate_merged_df])
    
    # Merge onto the categorical columns of our original Layout_df with rows ordered in the same sequence as it, based on the columns in shared_categories (except subtotal). Also do an outer with indicator to identify any extra rows coming from the new_results_layout_df. this shouldnt happen
    shared_categories.remove('subtotal_layout')
    shared_categories.remove('subtotal_results')
    final_df = original_layout_df[shared_categories].merge(new_results_layout_df, on=shared_categories, how='outer', indicator=True)
    right_only = final_df[final_df['_merge'] == 'right_only']#WE NEED TO GET RID OF UNNAMED 0 COLUMNS HERE
    if right_only.shape[0] > 0:
        print(right_only)
        breakpoint()
        raise Exception("There are extra rows in the new_results_layout_df that arent in the layout df. This should not happen.")
    # keep everything else:
    final_df = final_df[final_df['_merge'] != 'right_only'].copy()
    # Drop the _merge column
    final_df.drop(columns=['_merge'], inplace=True)

    #set up the order of columns to be shared_cateogires, subtotal_results, subtotal_layout, then the years in order
    final_df = final_df[shared_categories + ['subtotal_layout', 'subtotal_results'] + [col for col in final_df.columns if col not in shared_categories + ['subtotal_layout', 'subtotal_results']]]
    
    # Define the folder path where you want to save the file
    folder_path = f'results/{SINGLE_ECONOMY}/merged'
    # Check if the folder already exists
    if not os.path.exists(folder_path) and USE_SINGLE_ECONOMY:
        # If the folder doesn't exist, create it
        os.makedirs(folder_path)
    
    merging_functions.compare_to_previous_merge(final_df, shared_categories, results_data_path=folder_path,previous_merged_df_filename=previous_merged_df_filename, new_subtotal_columns=['subtotal_layout', 'subtotal_results'], previous_subtotal_columns=['subtotal_historic','subtotal_predicted','subtotal'])

    #save the combined data to a new Excel file
    #layout_df.to_excel('../../tfc/combined_data.xlsx', index=False, engine='openpyxl')
    date_today = datetime.now().strftime('%Y%m%d')
    if USE_SINGLE_ECONOMY:
        final_df.to_csv(f'{folder_path}/merged_file_{SINGLE_ECONOMY}_{date_today}.csv', index=False)
    else:
        final_df.to_csv(f'results/merged_file{date_today}.csv', index=False)
        
    return final_df


# #%%
# a = merging_results()
# a.to_csv('new.csv')
#%%




# %%

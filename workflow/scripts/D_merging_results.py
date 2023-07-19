"""A script to merge the layout file and the demand output results files."""

import pandas as pd
import numpy as np
import os
import glob
import sys
from utility_functions import *

def merging_results(merged_df_clean_wide):
    # #read the layout file
    # if USE_SINGLE_ECONOMY:
    #     layout_file = glob.glob(f'../../results/model_df_wide_{SINGLE_ECONOMY}_202*.csv')
    # else:
    #     layout_file = glob.glob('../../results/model_df_wide_202*.csv')

    # if len(layout_file) == 0:
    #     print("Layout file not found.")
    #     exit()

    # layout_file = layout_file[0]

    # layout_df = pd.read_csv(layout_file)
    layout_df = merged_df_clean_wide.copy()
    
    #extract unqiue economies:
    economies = layout_df['economy'].unique()

    # Define the path pattern for the results data files
    results_data_path = 'data/demand_results_data/*'

    # Get a list of all matching results data file paths
    results_data_files = glob.glob(results_data_path)

    # Specify the shared category columns in the desired order
    shared_categories = ['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels']

    # # Store the extracted economy DataFrames
    # economy_dataframes = {}#TO DO THIS ISNT BEING SUED. SHOULD I MAKE IT WORK?

    # Iterate over the results files
    for file in results_data_files:
        # Read the results file
        if file.endswith('.xlsx'):
            results_df = pd.read_excel(file)
        elif file.endswith('.csv'):
            results_df = pd.read_csv(file)
        else:
            print(f"Unsupported file format: {file}")
            continue

        # Reorder the shared categories in the results DataFrame
        results_df = results_df[shared_categories + list(results_df.columns.difference(shared_categories))]

        #filter for only economies in the results file:
        results_df = results_df[results_df['economy'].isin(economies)]
        
        # Convert columns to string type
        results_df.columns = results_df.columns.astype(str)

        # Drop columns with years within 1980 and 2020 in the results DataFrame
        results_df.drop(columns=[col for col in results_df.columns if any(str(year) in col for year in range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1))], inplace=True)

        # Compare the shared categories between layout and results DataFrame
        layout_shared_categories = layout_df[shared_categories]
        results_shared_categories = results_df[shared_categories]

        # Create a list to store the differences
        differences = []

        # Check if there are differences between the layout DataFrame and the results DataFrame
        for category in shared_categories:
            diff_variables = results_shared_categories.loc[~results_shared_categories[category].isin(layout_shared_categories[category]), category].unique()
            for variable in diff_variables:
                differences.append((variable, category))

        # Extract the file name from the file path
        file_name = os.path.basename(file)

        # Check if there are any differences
        if len(differences) > 0:
            print(f"Differences found between layout and results in file: {file_name}")
            for variable, category in differences:
                print(f"There is no '{variable}' in '{category}'")

            # Stop the code
            print("Stopping the code due to differences found.")
            sys.exit()

        # Set the index for both DataFrames using the shared category columns
        layout_df.set_index(shared_categories, inplace=True)
        results_df.set_index(shared_categories, inplace=True)

        # Update the layout DataFrame with the results DataFrame
        layout_df.update(results_df)

        # Reset the index of the layout DataFrame
        layout_df.reset_index(inplace=True)

    #save the combined data to a new Excel file
    #layout_df.to_excel('../../tfc/combined_data.xlsx', index=False, engine='openpyxl')
    if USE_SINGLE_ECONOMY:
        layout_df.to_csv(f'results/tfc/merged_file_{SINGLE_ECONOMY}.csv', index=False)
    else:
        layout_df.to_csv('results/tfc/merged_file.csv', index=False)
        
    return layout_df

"""A script to merge the layout file and the demand output results files."""

import pandas as pd
import numpy as np
import os
import glob

#read the layout file
layout_file = glob.glob('../../results/model_df_wide_202*.csv')

if len(layout_file) == 0:
    print("Layout file not found.")
    exit()

layout_file = layout_file[0]

layout_df = pd.read_csv(layout_file)

#get the column names of the predicted years
#years = layout_df.columns[50:]

# Define the path pattern for the results data files
results_data_path = '../../demand_results_data/*'

# Get a list of all matching results data file paths
results_data_files = glob.glob(results_data_path)

# Specify the shared category columns
shared_categories = ['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels']

# Merge each results file with the layout file
for file in results_data_files:
    # Check the file extension
    if file.endswith('.xlsx'):
        # Read Excel file
        results_data = pd.read_excel(file)
    elif file.endswith('.csv'):
        # Read CSV file
        results_data = pd.read_csv(file)
    else:
        print(f"Unsupported file format: {file}")
        continue
    
    # Drop the year columns from 1980 to 2020 in the results dataframe
    year_columns = [str(year) for year in range(1980, 2021)]
    results_data.drop(columns=year_columns, inplace=True, errors='ignore')
    
    # Merge the layout dataframe with the results dataframe based on the shared categories
    layout_df.set_index(shared_categories, inplace=True)
    results_data.set_index(shared_categories, inplace=True)
    layout_df.update(results_data)
    layout_df.reset_index(inplace=True)

#save the combined data to a new Excel file
#layout_df.to_excel('../../tfc/combined_data.xlsx', index=False, engine='openpyxl')
layout_df.to_csv('../../tfc/merged_file.csv', index=False)
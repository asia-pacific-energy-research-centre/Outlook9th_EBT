"""A script to merge the layout file and the demand output results files."""

import pandas as pd
import numpy as np
import os
import glob

#read the layout file
layout_file = glob.glob('../../results/model_df_test_l*.csv')

if len(layout_file) == 0:
    print("Layout file not found.")
    exit()

layout_file = layout_file[0]

layout_df = pd.read_csv(layout_file)

#get the column names of the predicted years
years = layout_df.columns[50:]

# Define the path pattern for the results data files
results_data_path = '../../demand_results_data/*'

# Get a list of all matching results data file paths
results_data_files = glob.glob(results_data_path)

# Iterate over the results data files
for result_file in results_data_files:
    # Determine the file format based on the file extension
    if result_file.endswith('.csv'):
        results_data = pd.read_csv(result_file)
    elif result_file.endswith('.xlsx') or result_file.endswith('.xls'):
        results_data = pd.read_excel(result_file)
    else:
        print(f"Unsupported file format: {result_file}")
        continue

    #iterate over the columns representing the predicted years
    for year in years:
        rows_with_nan = layout_df[year].isnull().tolist() #find the rows with NaN values in the current year column

        #get the corresponding values from the results data based on the categories
        categories = layout_df.iloc[rows_with_nan, :9]
        result_values = results_data.loc[results_data.iloc[:, :9].isin(categories.values.flatten()).all(axis=1), year].values

        layout_df.iloc[rows_with_nan, layout_df.columns.get_loc(year)] = result_values #replace the NaN values in the layout df with the result values for the current year

#save the combined data to a new Excel file
layout_df.to_excel('../../tfc/combined_data.xlsx', index=False)

"""A script to merge the layout file and the demand output results files."""

import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime

#read the layout file
layout_file = glob.glob('../../results/model_df_wide_202*.csv')

if len(layout_file) == 0:
    print("Layout file not found.")
    exit()

layout_file = layout_file[0]

layout_df = pd.read_csv(layout_file)

# Define the path pattern for the results data files
results_data_path = '../../demand_results_data/*'

# Get a list of all matching results data file paths
results_data_files = glob.glob(results_data_path)

# Specify the shared category columns in the desired order
shared_categories = ['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels']

# Store the extracted economy DataFrames
economy_dataframes = {}

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

    # Reorder the shared categories in the results DataFrame
    results_df = results_df[shared_categories + list(results_df.columns.difference(shared_categories))]

    # Convert columns to string type
    results_df.columns = results_df.columns.astype(str)

    # Drop columns with years within 1980 and 2020 in the results DataFrame
    results_df.drop(columns=[col for col in results_df.columns if any(str(year) in col for year in range(1980, 2021))], inplace=True)

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

    # Use assert to check if there are any differences
    assert len(differences) == 0, f"Differences found in results file: {file_name}\n\nDifferences:\n" + '\n'.join([f"There is no '{variable}' in '{category}'" for variable, category in differences])

    # Set the index for both DataFrames using the shared category columns
    layout_df.set_index(shared_categories, inplace=True)
    results_df.set_index(shared_categories, inplace=True)

    # Update the layout DataFrame with the results DataFrame
    layout_df.update(results_df)

    # Reset the index of the layout DataFrame
    layout_df.reset_index(inplace=True)

# Define the year range to drop
year_range = range(1980, 2021)

# Create a list of columns to drop
columns_to_drop = ['sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors'] + [str(year) for year in year_range if str(year) in layout_df.columns]

# Drop the specified columns and year columns from the layout_df DataFrame
filtered_df = layout_df.drop(columns_to_drop, axis=1)

# Filter the 'sectors' column to include only the desired sectors
tfc_desired_sectors = ['14_industry_sector', '15_transport_sector', '16_other_sector', '17_nonenergy_use']
tfc_filtered_df = filtered_df[filtered_df['sectors'].isin(tfc_desired_sectors)].copy()

# Filter the 'sectors' column to include only the desired sectors
tfec_desired_sectors = ['14_industry_sector', '15_transport_sector', '16_other_sector']
tfec_filtered_df = filtered_df[filtered_df['sectors'].isin(tfec_desired_sectors)].copy()

# Group by 'scenarios', 'economy', 'fuels', and 'subfuels' and sum the values in '12_total_final_consumption'
tfc_grouped_df = tfc_filtered_df.groupby(['scenarios', 'economy', 'fuels', 'subfuels']).sum().reset_index()

# Group by 'scenarios', 'economy', 'fuels', and 'subfuels' and sum the values in '13_total_final_energy_consumption'
tfec_grouped_df = tfec_filtered_df.groupby(['scenarios', 'economy', 'fuels', 'subfuels']).sum().reset_index()

# Add the missing columns with 'x' as values in the same order as shared_categories
for col in shared_categories:
    if col not in tfc_grouped_df.columns:
        tfc_grouped_df[col] = 'x'
        
# Add the missing columns with 'x' as values in the same order as shared_categories
for col in shared_categories:
    if col not in tfec_grouped_df.columns:
        tfec_grouped_df[col] = 'x'

# Reorder the columns to match the order of shared_categories
tfc_ordered_columns = shared_categories + [col for col in tfc_grouped_df.columns if col not in shared_categories]

# Reorder the columns to match the order of shared_categories
tfec_ordered_columns = shared_categories + [col for col in tfec_grouped_df.columns if col not in shared_categories]

# Reorder the columns in grouped_df using the ordered_columns list
tfc_grouped_df = tfc_grouped_df[ordered_columns]

# Reorder the columns in grouped_df using the ordered_columns list
tfec_grouped_df = tfec_grouped_df[ordered_columns]

# Add the 'sectors' column with value '12_total_final_consumption'
tfc_grouped_df['sectors'] = '12_total_final_consumption'

# Add the 'sectors' column with value '13_total_final_energy_consumption'
tfec_grouped_df['sectors'] = '13_total_final_energy_consumption'

# Set the index for both DataFrames using the shared category columns
layout_df.set_index(shared_categories, inplace=True)
tfc_grouped_df.set_index(shared_categories, inplace=True)
tfec_grouped_df.set_index(shared_categories, inplace=True)

# Update the layout_df with the values from grouped_df
layout_df.update(tfc_grouped_df)
layout_df.update(tfec_grouped_df)

# Reset the index of the layout DataFrame
layout_df.reset_index(inplace=True)

#save the combined data to a new Excel file
#layout_df.to_excel('../../tfc/combined_data.xlsx', index=False, engine='openpyxl')
date_today = datetime.now().strftime('%Y%m%d')
layout_df.to_csv('../../tfc/tfc_df_wide_'+date_today+'.csv', index=False)

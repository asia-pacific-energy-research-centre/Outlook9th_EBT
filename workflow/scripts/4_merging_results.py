"""A script to merge the layout file and the demand output results files."""

import pandas as pd
import os
import glob

folder_path = '../../demand_results_data/'

#get a list of all CSV and Excel file paths in the folder
file_pattern = os.path.join(folder_path, '*')
file_paths = glob.glob(file_pattern)

demand_dfs = {} #dictionary to store the demand results files

#process each file
for file_path in file_paths:
    file_name = os.path.basename(file_path)
    if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
        demand_df = pd.read_excel(file_path)
    elif file_path.endswith('.csv'):
        demand_df = pd.read_csv(file_path)
    else:
        print("Unsupported file format:", file_path)
        continue
    demand_dfs[file_name] = demand_df

'''
#read the layout template into a df
tfc_df = pd.read_csv(data_files[0])

#separate input data and output data
category_variables = tfc_df[['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels']]

year_list = list(map(str, list(range(1980, 2070 + 1)))) #making a list of years from 1980 to 2070

output_data = tfc_df[year_list]
'''

#iterate over the remaining excel files and merge them with the initial df
"""
for file in excel_files[1:]:
    df = pd.read_excel(file)
    tfc_df = pd.concat([tfc_df, df], ignore_index=True)
"""
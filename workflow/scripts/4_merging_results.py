"""A script to merge the layout file and the demand output results files."""

import pandas as pd

#list of csv/excel files to merge
data_files = [r"c:\Users\hyuga.kasai\APERC\Outlook-9th - Modelling\Integration\00_LayoutTemplate\model_df_wide_20230710.csv", 
               r"c:\Users\hyuga.kasai\APERC\Outlook-9th - Modelling\Integration\01_Demand\01_03_DT\model_output_years_2017_to_2100_20230703.csv",
               ]

#read the layout template into a df
tfc_df = pd.read_csv(data_files[0])

#separate input data and output data
category_variables = tfc_df[['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels']]

year_list = list(map(str, list(range(1980, 2070 + 1)))) #making a list of years from 1980 to 2070

output_data = tfc_df[year_list]

#iterate over the remaining excel files and merge them with the initial df
"""
for file in excel_files[1:]:
    df = pd.read_excel(file)
    tfc_df = pd.concat([tfc_df, df], ignore_index=True)
"""
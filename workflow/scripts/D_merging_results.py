"""A script to merge the layout file and the demand output results files."""

import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
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

    # Create an empty merged_results_df with the shared_categories
    merged_results_df = pd.DataFrame()


    # Define the path pattern for the results data files
    results_data_path = 'data/demand_results_data/*'

    # Get a list of all matching results data file paths
    results_data_files = glob.glob(results_data_path)

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

        #filter for only economies in the layout file:
        results_df = results_df[results_df['economy'].isin(economies)]

        # Check for non-null values under the year column '2070'
        has_2070_values = results_df['2070'].notnull()

        # Create a list of unique sectors where '2070' has non-null values with no duplicates
        unique_sectors_2070 = results_df.loc[has_2070_values, 'sectors'].unique().tolist()

        # Check if 'sectors' column contains '16_other_sector'
        if '16_other_sector' in unique_sectors_2070:
            # Create a list of unique sub1sectors where '2070' is not null and 'sectors' is '16_other_sector'
            unique_sub1sectors = results_df.loc[has_2070_values & (results_df['sectors'] == '16_other_sector'), 'sub1sectors'].unique().tolist()

            # Filter 'results_df' to keep only the rows with '16_other_sector' in 'sectors' and unique sub1sectors
            filtered_results_df = results_df[results_df['sectors'].isin(['16_other_sector']) & results_df['sub1sectors'].isin(unique_sub1sectors)].copy()
        else:
            # If '16_other_sector' is not present, just filter based on 'sectors' with non-null '2070' values
            filtered_results_df = results_df[results_df['sectors'].isin(unique_sectors_2070)].copy()

        # Drop columns with years within 1980 and 2020 in the results DataFrame
        filtered_results_df.drop(columns=[col for col in filtered_results_df.columns if any(str(year) in col for year in range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1))], inplace=True)


        # Compare the shared categories between layout and results DataFrame
        layout_shared_categories = layout_df[shared_categories]
        results_shared_categories = filtered_results_df[shared_categories]

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


        # Combine the results_df
        merged_results_df = pd.concat([merged_results_df, filtered_results_df])


    # Get the unique sectors from the results_df
    sectors_list = merged_results_df['sectors'].unique().tolist()

    # Create a new DataFrame with rows that match the sectors from the results DataFrame
    new_layout_df = layout_df[layout_df['sectors'].isin(sectors_list)].copy()

    # Drop the rows that were updated in the new DataFrame from the original layout DataFrame
    dropped_layout_df = layout_df[~layout_df['sectors'].isin(sectors_list)].copy()



    # Drop columns 2021 to 2070 from new_layout_df
    columns_to_drop = [str(year) for year in range(2021, 2071)]
    new_layout_df.drop(columns=columns_to_drop, inplace=True)

    # Merge the new_layout_df with the results_df based on shared_categories
    merged_df = pd.merge(new_layout_df, merged_results_df, on=shared_categories, how="left")


    # Combine the original layout_df with the merged_df
    results_layout_df = pd.concat([dropped_layout_df, merged_df])



    # Melt the DataFramew
    df_melted = results_layout_df.melt(id_vars=shared_categories, var_name='year', value_name='value')
    df_melted['year'] = df_melted['year'].astype(int)

    # Split the DataFrame into historic and predicted
    historic_df = df_melted[df_melted['year'] <= 2020].copy()
    predicted_df = df_melted[df_melted['year'] > 2020].copy()

    # Drop the 'year' column
    historic_df = historic_df.drop(columns='year')
    predicted_df = predicted_df.drop(columns='year')

    # Sum the 'value' for each unique combination of 'shared_categories'
    historic_df = historic_df.groupby(shared_categories)['value'].sum().reset_index()
    predicted_df = predicted_df.groupby(shared_categories)['value'].sum().reset_index()

    # Merge the two dataframes on 'shared_categories'
    years_aggregated_df = pd.merge(historic_df, predicted_df, on=shared_categories, suffixes=('_historic', '_predicted'))

    # Condition 1: 'value_historic' is not NA and not 0
    condition1 = years_aggregated_df['value_historic'].notna() & (years_aggregated_df['value_historic'] != 0)

    # Condition 3: There is at least one 'x' in the row
    condition3 = years_aggregated_df.apply(lambda row: row.astype(str).str.contains('x').any(), axis=1)

    # Overarching conditions
    overarching_conditions = condition1 & condition3
    print("Number of rows that meet the overarching conditions: ", overarching_conditions.sum())

    # Condition 2: The row has '19_total' in the 'fuels' column
    condition2 = years_aggregated_df['fuels'].str.contains('19_total')

    # Condition 4: The 'subfuels' column is exactly 'x'
    condition4 = (years_aggregated_df['subfuels'] == 'x') & \
                years_aggregated_df['value_historic'].notna() & \
                (years_aggregated_df['value_historic'] != 0) & \
                ~years_aggregated_df['fuels'].isin(['19_total', '20_total_renewables', '21_modern_renewables'])

    # # Condition 5: For each group of rows that have the same other_categories, check if there are multiple unique values in the 'subfuels' column
    other_categories = [cat for cat in shared_categories if cat != 'subfuels']


    # Apply condition 5 only to the rows that meet condition 4
    df_cond4 = years_aggregated_df[condition4].copy()

    # Print out the 'value_historic' values for each group of rows with the same combination of other_categories
    df_cond4.groupby(other_categories)['value_historic'].apply(lambda x: print(x))

    # Define a small tolerance
    tolerance = 1e-3

    # For each row, compare its 'value_historic' to the sum of 'value_historic' for the same combination of other_categories (excluding itself)
    df_cond4['sum_others'] = df_cond4.groupby(other_categories)['value_historic'].transform(lambda x: x.sum() - x)

    # Print out 'value_historic' and 'sum_others' for each row
    for idx, row in df_cond4.iterrows():
        print(f"Row {idx} - Value: {row['value_historic']}, Sum of Others: {row['sum_others']}")

    # Apply condition 5 
    condition5 = np.isclose(df_cond4['value_historic'], df_cond4['sum_others'], rtol=tolerance, atol=tolerance)


    # Print Condition 5
    print("\nCondition 5:")
    print(condition5)


    # Update 'condition5' column in the DataFrame
    #years_aggregated_df.loc[condition4, 'condition5'] = condition5

    # Condition 6: The 'value_historic' of rows where Condition 4 and Condition 5 are met equals the sum of 'value_historic' for the same combination of other_categories
    #sum_values = years_aggregated_df.groupby(other_categories)['value_historic'].transform('sum')
    # modified condition 6
    # Get the rows that meet condition 5
    # df_cond5 = years_aggregated_df[condition5]

    # # Apply condition 6 only to the rows that meet condition 5
    # condition6 = (df_cond5['subfuels'] == 'x') & (df_cond5.groupby(other_categories)['value_historic'].transform(lambda x: x[df_cond5['subfuels'] != 'x'].sum()) == df_cond5['value_historic'])
    # condition4_and_5_and_6 = condition4_and_5 & condition6

    # # Print the number of rows that meet Condition 6
    print("Number of rows that meet Condition 4: ", condition4.sum())
    print("Number of rows that meet Condition 5: ", condition5.sum())
    # condition4_and_5_and_6.to_csv('condition4_and_5_and_6.csv')

    # Count how many 'x' there are in 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors'
    subsectors = ['sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors']
    x_count = years_aggregated_df[subsectors].apply(lambda x: x.str.contains('x')).sum(axis=1)

    # Initialize a series to store the results for condition 9
    condition9_results = pd.Series(False, index=years_aggregated_df.index)

    # # For each x_count from 1 to 4, check for other 'subsectors' values and 'value_historic' equality
    # for i in range(1, 5):
    #     # Condition 7: There is exactly i 'x' in the subsectors
    #     condition7 = x_count == i

    #     # Condition 8: If there is exactly i 'x', check if there are other 'subsectors' values for the same combination of other categories
    #     other_categories_sub = [cat for cat in shared_categories if cat != subsectors[4 - i]]
    #     condition8 = years_aggregated_df[condition7].groupby(other_categories_sub)[subsectors[4 - i]].transform('nunique') > 1
    #     condition7_and_8 = condition7 & condition8

    #     # Condition 9: If Condition 7 and Condition 8 are met, check if the 'value_historic' is equal to the sum of the 'value_historic' for the same conditions
    #     condition9 = condition7_and_8 & (years_aggregated_df['value_historic'] == years_aggregated_df[condition7_and_8]['value_historic'].sum())

    #     # Store the results for condition 9
    #     condition9_results = condition9_results | condition9

    # Add the result to the 'subtotal' column
    years_aggregated_df['subtotal'] = condition4 #& (condition2 | condition4_and_5_and_6)

    # Print the number of rows where 'subtotal' is True
    print("Number of rows where 'subtotal' is True: ", years_aggregated_df['subtotal'].sum())



    years_aggregated_df.to_csv('years_aggregated_df.csv', index=False)






    #Check if new_layout_df and results_df have the same number of rows
    #assert new_layout_df.shape[0] == results_df.shape[0], f"Layout dataframe and {file} do not have the same number of rows.\nLayout dataframe rows: {new_layout_df.shape[0]}\n{file} rows: {results_df.shape[0]}"



    # # Define the year range to drop
    # year_range = range(1980, 2021)

    # # Create a list of columns to drop
    # aggregating_columns_to_drop = ['sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors'] + [str(year) for year in year_range if str(year) in layout_df.columns]

    # # Drop the specified columns and year columns from the layout_df DataFrame
    # filtered_df = results_layout_df.drop(aggregating_columns_to_drop, axis=1).copy()

    # # Filter the 'sectors' column to include only the desired sectors
    # tfc_desired_sectors = ['14_industry_sector', '15_transport_sector', '16_other_sector', '17_nonenergy_use']
    # tfc_filtered_df = filtered_df[filtered_df['sectors'].isin(tfc_desired_sectors)].copy()

    # # Filter the 'sectors' column to include only the desired sectors
    # tfec_desired_sectors = ['14_industry_sector', '15_transport_sector', '16_other_sector']
    # tfec_filtered_df = filtered_df[filtered_df['sectors'].isin(tfec_desired_sectors)].copy()

    # # Filter the 'sectors' column to include only the desired sectors
    # tpes_desired_sectors = ['9_total_transformation_sector', '10_losses_and_own_use', '11_statistical_discrepancy','12_total_final_consumption']
    # tpes_filtered_df = filtered_df[filtered_df['sectors'].isin(tpes_desired_sectors)].copy()

    # # Group by 'scenarios', 'economy', 'fuels', and 'subfuels' and sum the values in '12_total_final_consumption'
    # tfc_grouped_df = tfc_filtered_df.groupby(['scenarios', 'economy', 'fuels', 'subfuels']).sum().reset_index()

    # # Group by 'scenarios', 'economy', 'fuels', and 'subfuels' and sum the values in '13_total_final_energy_consumption'
    # tfec_grouped_df = tfec_filtered_df.groupby(['scenarios', 'economy', 'fuels', 'subfuels']).sum().reset_index()

    # # Filter numeric columns only in tpes_filtered_df
    # numeric_cols = tpes_filtered_df.select_dtypes(include=[np.number]).columns.tolist()

    # # Group by 'scenarios', 'economy', 'fuels', and 'subfuels' and sum the absolute values in numeric columns
    # tpes_grouped_df = tpes_filtered_df.groupby(['scenarios', 'economy', 'fuels', 'subfuels'])[numeric_cols].apply(lambda x: np.abs(x.sum())).reset_index()

    # # Add the missing columns with 'x' as values in the same order as shared_categories
    # for col in shared_categories:
    #     if col not in tfc_grouped_df.columns:
    #         tfc_grouped_df[col] = 'x'

    # # Add the missing columns with 'x' as values in the same order as shared_categories
    # for col in shared_categories:
    #     if col not in tfec_grouped_df.columns:
    #         tfec_grouped_df[col] = 'x'

    # # Add the missing columns with 'x' as values in the same order as shared_categories
    # for col in shared_categories:
    #     if col not in tpes_grouped_df.columns:
    #         tpes_grouped_df[col] = 'x'

    # # Reorder the columns to match the order of shared_categories
    # tfc_ordered_columns = shared_categories + [col for col in tfc_grouped_df.columns if col not in shared_categories]

    # # Reorder the columns to match the order of shared_categories
    # tfec_ordered_columns = shared_categories + [col for col in tfec_grouped_df.columns if col not in shared_categories]

    # # Reorder the columns to match the order of shared_categories
    # tpes_ordered_columns = shared_categories + [col for col in tpes_grouped_df.columns if col not in shared_categories]

    # # Reorder the columns in grouped_df using the ordered_columns list
    # tfc_grouped_df = tfc_grouped_df[tfc_ordered_columns]

    # # Reorder the columns in grouped_df using the ordered_columns list
    # tfec_grouped_df = tfec_grouped_df[tfec_ordered_columns]

    # # Reorder the columns in grouped_df using the ordered_columns list
    # tpes_grouped_df = tpes_grouped_df[tpes_ordered_columns]

    # # Add the 'sectors' column with value '12_total_final_consumption'
    # tfc_grouped_df['sectors'] = '12_total_final_consumption'

    # # Add the 'sectors' column with value '13_total_final_energy_consumption'
    # tfec_grouped_df['sectors'] = '13_total_final_energy_consumption'

    # # Add the 'sectors' column with value '07_total_primary_energy_supply'
    # tpes_grouped_df['sectors'] = '07_total_primary_energy_supply'


    # # Combine the grouped_df
    # merged_grouped_df = pd.concat([tfc_grouped_df, tfec_grouped_df, tpes_grouped_df])



    # # Get the unique sectors
    # aggregate_sectors_list = merged_grouped_df['sectors'].unique().tolist()

    # # Create a new DataFrame with rows that match the sectors from the results DataFrame
    # new_aggregate_layout_df = results_layout_df[results_layout_df['sectors'].isin(aggregate_sectors_list)].copy()

    # # Drop the rows that were updated in the new DataFrame from the original layout DataFrame
    # dropped_aggregate_layout_df = results_layout_df[~results_layout_df['sectors'].isin(aggregate_sectors_list)].copy()


    # new_aggregate_layout_df.drop(columns=columns_to_drop, inplace=True)

    # # Merge the DataFrames based on the shared category columns
    # aggregate_merged_df = new_aggregate_layout_df.merge(merged_grouped_df, on=shared_categories, how='left')


    # # Combine the original layout_df with the merged_df
    # layout_df = pd.concat([dropped_aggregate_layout_df, aggregate_merged_df])


    #save the combined data to a new Excel file
    #layout_df.to_excel('../../tfc/combined_data.xlsx', index=False, engine='openpyxl')
    # date_today = datetime.now().strftime('%Y%m%d')
    # if USE_SINGLE_ECONOMY:
    #     layout_df.to_csv(f'results/merged_file_{SINGLE_ECONOMY}_{date_today}.csv', index=False)
    # else:
    #     layout_df.to_csv(f'results/merged_file{date_today}.csv', index=False)
        
    return layout_df

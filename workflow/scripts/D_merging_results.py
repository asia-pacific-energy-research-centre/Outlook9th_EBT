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
    merged_results_df = pd.DataFrame(columns=shared_categories)

    if USE_SINGLE_ECONOMY:
        # Define the path pattern for the results data files
        results_data_path = 'data/demand_results_data/'+SINGLE_ECONOMY+'/*'
        print(results_data_path)
    else:
        print("Not implemented yet.")

    # Define the path pattern for the results data files
    #results_data_path = 'data/demand_results_data/*'

    # Get a list of all matching results data file paths
    results_data_files = glob.glob(results_data_path)

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

        # Reorder the shared categories in the results DataFrame
        results_df = results_df[shared_categories + list(results_df.columns.difference(shared_categories))]

        # Convert columns to string type
        results_df.columns = results_df.columns.astype(str)

        # Keep columns from '2021' to '2070'
        years_to_keep = [str(year) for year in range(2021, 2071)]
        results_df = results_df[shared_categories + years_to_keep]

        #filter for only economies in the layout file:
        results_df = results_df[results_df['economy'].isin(economies)]

        # Check for non-null values in each year column and combine them with the or operator
        has_non_null_values = results_df[years_to_keep].notnull().any(axis=1)

        # Create a list of unique sectors where there are non-null values for the years 2021-2070
        unique_sectors = results_df.loc[has_non_null_values, 'sectors'].unique().tolist()

        # Check if 'sectors' column contains '16_other_sector'
        if '16_other_sector' in unique_sectors:
            # Create a list of unique sub1sectors where there are non-null values for the years 2021-2070 and 'sectors' is '16_other_sector'
            unique_sub1sectors = results_df.loc[has_non_null_values & (results_df['sectors'] == '16_other_sector'), 'sub1sectors'].unique().tolist()

            # Filter 'results_df' to keep only the rows with '16_other_sector' in 'sectors' and unique sub1sectors
            filtered_results_df = results_df[results_df['sectors'].isin(['16_other_sector']) & results_df['sub1sectors'].isin(unique_sub1sectors)].copy()
        else:
            # If '16_other_sector' is not present, just filter based on 'sectors' with non-null values for the years 2021-2070
            filtered_results_df = results_df[results_df['sectors'].isin(unique_sectors)].copy()


        # Drop columns with years within 1980 and 2020 in the results DataFrame
        filtered_results_df.drop(columns=[col for col in filtered_results_df.columns if any(str(year) in col for year in range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1))], inplace=True)


        ###############TEMP################
        # Filter rows where the sector is '05_international_aviation_bunkers'
        filtered_rows = filtered_results_df['sectors'] == '05_international_aviation_bunkers'

        # For each year from 2021 to 2070, check if the value is positive, if yes, make it negative
        for year in range(2021, 2071):
            filtered_results_df.loc[filtered_rows, str(year)] = filtered_results_df.loc[filtered_rows, str(year)].apply(lambda x: -abs(x))
        ###################################


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

        #merged_results_df = merged_results_df.merge(filtered_results_df, on=shared_categories, how='left')


    # Get the unique sectors from the results_df
    sectors_list = merged_results_df['sectors'].unique().tolist()

    # Create a new DataFrame with rows that match the sectors from the results DataFrame
    new_layout_df = layout_df[layout_df['sectors'].isin(sectors_list)].copy()
    # print("Number of rows in new_layout_df:", new_layout_df.shape[0])
    # print("Number of rows in merged_results_df:", merged_results_df.shape[0])

    # Drop the rows that were updated in the new DataFrame from the original layout DataFrame
    dropped_layout_df = layout_df[~layout_df['sectors'].isin(sectors_list)].copy()



    # Drop columns 2021 to 2070 from new_layout_df
    columns_to_drop = [str(year) for year in range(2021, 2071)]
    new_layout_df.drop(columns=columns_to_drop, inplace=True)

    # Drop rows with NA or zeros in the year columns from merged_results_df
    year_columns = [str(year) for year in range(2021, 2071)]
    merged_results_df.dropna(subset=year_columns, how='all', inplace=True)
    merged_results_df = merged_results_df.loc[~(merged_results_df[year_columns] == 0).all(axis=1)]

    # Check for duplicate rows in merged_results_df based on shared_categories
    duplicates = merged_results_df[merged_results_df.duplicated(subset=shared_categories, keep=False)]

    # Remove the duplicate rows from merged_results_df
    merged_results_df = merged_results_df.drop_duplicates(subset=shared_categories, keep='first').copy()

    # Print the updated number of rows in merged_results_df
    print("Number of rows in merged_results_df after removing rows without year values and duplicates:", merged_results_df.shape[0])

    # Merge the new_layout_df with the merged_results_df based on shared_categories using left merge
    merged_df = pd.merge(new_layout_df, merged_results_df, on=shared_categories, how="left")

    # # Check for duplicate rows in merged_results_df
    # duplicates = merged_results_df[merged_results_df.duplicated(subset=shared_categories, keep=False)]

    # Check if there are any unexpected extra rows in merged_df
    unexpected_rows = merged_df[~merged_df.index.isin(new_layout_df.index)]

    # Print the number of rows in both dataframes
    print("Number of rows in new_layout_df:", new_layout_df.shape[0])
    print("Number of rows in merged_results_df:", merged_results_df.shape[0])
    print("Number of rows in merged_df:", merged_df.shape[0])

    # Print any duplicates and unexpected rows
    print("Duplicates in merged_results_df:")
    print(duplicates)
    #duplicates.to_csv("duplicates.csv")
    # print("Unexpected rows in merged_df:")
    # print(unexpected_rows)

    # Combine the original layout_df with the merged_df
    results_layout_df = pd.concat([dropped_layout_df, merged_df])




    # Melt the DataFrame
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
    condition1_predicted = years_aggregated_df['value_predicted'].notna() & (years_aggregated_df['value_predicted'] != 0)

    # Condition 3: There is at least one 'x' in the row
    condition3 = years_aggregated_df.apply(lambda row: row.astype(str).str.contains('x').any(), axis=1)

    # Overarching conditions
    overarching_conditions = condition1 & condition3
    overarching_conditions_predicted = condition1_predicted & condition3
    #print("Number of rows that meet the overarching conditions: ", overarching_conditions.sum())

    # Condition 2: The row has '19_total', '20_total_renewables', '21_modern_renewables' in the 'fuels' column
    condition2 = years_aggregated_df['fuels'].str.contains('19_total|20_total_renewables|21_modern_renewables')



    # Condition for subfuels
    condition_subfuels = (years_aggregated_df['subfuels'] == 'x') & \
                        years_aggregated_df['value_historic'].notna() & \
                        (years_aggregated_df['value_historic'] != 0) & \
                        ~years_aggregated_df['fuels'].isin(['19_total', '20_total_renewables', '21_modern_renewables'])
    condition_subfuels_predicted = (years_aggregated_df['subfuels'] == 'x') & \
                        years_aggregated_df['value_predicted'].notna() & \
                        (years_aggregated_df['value_predicted'] != 0) & \
                        ~years_aggregated_df['fuels'].isin(['19_total', '20_total_renewables', '21_modern_renewables'])

    # Condition for sub4sectors being 'x'
    condition_sub4sectors = (years_aggregated_df['sub4sectors'] == 'x') & \
                            years_aggregated_df['value_historic'].notna() & \
                            (years_aggregated_df['value_historic'] != 0) & \
                            ~years_aggregated_df['fuels'].isin(['19_total', '20_total_renewables', '21_modern_renewables'])
    condition_sub4sectors_predicted = (years_aggregated_df['sub4sectors'] == 'x') & \
                            years_aggregated_df['value_predicted'].notna() & \
                            (years_aggregated_df['value_predicted'] != 0) & \
                            ~years_aggregated_df['fuels'].isin(['19_total', '20_total_renewables', '21_modern_renewables'])

    # Condition for sub3sectors being 'x'
    condition_sub3sectors = (years_aggregated_df['sub3sectors'] == 'x') & \
                            years_aggregated_df['value_historic'].notna() & \
                            (years_aggregated_df['value_historic'] != 0) & \
                            ~years_aggregated_df['fuels'].isin(['19_total', '20_total_renewables', '21_modern_renewables'])
    condition_sub3sectors_predicted = (years_aggregated_df['sub3sectors'] == 'x') & \
                            years_aggregated_df['value_predicted'].notna() & \
                            (years_aggregated_df['value_predicted'] != 0) & \
                            ~years_aggregated_df['fuels'].isin(['19_total', '20_total_renewables', '21_modern_renewables'])

    # Condition for sub2sectors being 'x'
    condition_sub2sectors = (years_aggregated_df['sub2sectors'] == 'x') & \
                            years_aggregated_df['value_historic'].notna() & \
                            (years_aggregated_df['value_historic'] != 0) & \
                            ~years_aggregated_df['fuels'].isin(['19_total', '20_total_renewables', '21_modern_renewables'])
    condition_sub2sectors_predicted = (years_aggregated_df['sub2sectors'] == 'x') & \
                            years_aggregated_df['value_predicted'].notna() & \
                            (years_aggregated_df['value_predicted'] != 0) & \
                            ~years_aggregated_df['fuels'].isin(['19_total', '20_total_renewables', '21_modern_renewables'])

    # Condition for sub1sectors being 'x'
    condition_sub1sectors = (years_aggregated_df['sub1sectors'] == 'x') & \
                            years_aggregated_df['value_historic'].notna() & \
                            (years_aggregated_df['value_historic'] != 0) & \
                            ~years_aggregated_df['fuels'].isin(['19_total', '20_total_renewables', '21_modern_renewables'])
    condition_sub1sectors_predicted = (years_aggregated_df['sub1sectors'] == 'x') & \
                            years_aggregated_df['value_predicted'].notna() & \
                            (years_aggregated_df['value_predicted'] != 0) & \
                            ~years_aggregated_df['fuels'].isin(['19_total', '20_total_renewables', '21_modern_renewables'])


    # Function to check the totals for both 'value_historic' and 'value_predicted'
    def check_totals(column_name, group):
        total_row = group[group[column_name] == 'x']
        other_rows = group[group[column_name] != 'x']

        # Initialize the results
        check_historic = False
        check_predicted = False

        if not total_row.empty:
            # Adjust this tolerance if needed
            tolerance = 1e-3

            # Check 'value_historic'
            total_value_historic = total_row['value_historic'].values[0]
            sum_others_historic = other_rows['value_historic'].sum()
            check_historic = np.isclose(total_value_historic, sum_others_historic, rtol=tolerance)

            # Check 'value_predicted'
            total_value_predicted = total_row['value_predicted'].values[0]
            sum_others_predicted = other_rows['value_predicted'].sum()
            check_predicted = np.isclose(total_value_predicted, sum_others_predicted, rtol=tolerance)

        return check_historic, check_predicted



    # Checking for subfuels
    other_categories_subfuels = [cat for cat in shared_categories if cat != 'subfuels']
    grouped_data_subfuels = years_aggregated_df.groupby(other_categories_subfuels)
    results_subfuels = grouped_data_subfuels.apply(lambda group: check_totals('subfuels', group))
    subtotal_subfuels_historic = years_aggregated_df.merge(results_subfuels.reset_index(), on=other_categories_subfuels, how='left')[0].apply(lambda x: x[0]).fillna(False)
    subtotal_subfuels_predicted = years_aggregated_df.merge(results_subfuels.reset_index(), on=other_categories_subfuels, how='left')[0].apply(lambda x: x[1]).fillna(False)

    # For sub4sectors
    other_categories_sub4sectors = [cat for cat in shared_categories if cat != 'sub4sectors']
    grouped_data_sub4sectors = years_aggregated_df.groupby(other_categories_sub4sectors)
    results_sub4sectors = grouped_data_sub4sectors.apply(lambda group: check_totals('sub4sectors', group))
    subtotal_sub4sectors_historic = years_aggregated_df.merge(results_sub4sectors.reset_index(), on=other_categories_sub4sectors, how='left')[0].apply(lambda x: x[0]).fillna(False)
    subtotal_sub4sectors_predicted = years_aggregated_df.merge(results_sub4sectors.reset_index(), on=other_categories_sub4sectors, how='left')[0].apply(lambda x: x[1]).fillna(False)

    # For sub3sectors
    other_categories_sub3sectors = [cat for cat in shared_categories if cat != 'sub3sectors']
    grouped_data_sub3sectors = years_aggregated_df.groupby(other_categories_sub3sectors)
    results_sub3sectors = grouped_data_sub3sectors.apply(lambda group: check_totals('sub3sectors', group))
    subtotal_sub3sectors_historic = years_aggregated_df.merge(results_sub3sectors.reset_index(), on=other_categories_sub3sectors, how='left')[0].apply(lambda x: x[0]).fillna(False)
    subtotal_sub3sectors_predicted = years_aggregated_df.merge(results_sub3sectors.reset_index(), on=other_categories_sub3sectors, how='left')[0].apply(lambda x: x[1]).fillna(False)

    # For sub2sectors
    other_categories_sub2sectors = [cat for cat in shared_categories if cat != 'sub2sectors']
    grouped_data_sub2sectors = years_aggregated_df.groupby(other_categories_sub2sectors)
    results_sub2sectors = grouped_data_sub2sectors.apply(lambda group: check_totals('sub2sectors', group))
    subtotal_sub2sectors_historic = years_aggregated_df.merge(results_sub2sectors.reset_index(), on=other_categories_sub2sectors, how='left')[0].apply(lambda x: x[0]).fillna(False)
    subtotal_sub2sectors_predicted = years_aggregated_df.merge(results_sub2sectors.reset_index(), on=other_categories_sub2sectors, how='left')[0].apply(lambda x: x[1]).fillna(False)

    # For sub1sectors
    other_categories_sub1sectors = [cat for cat in shared_categories if cat != 'sub1sectors']
    grouped_data_sub1sectors = years_aggregated_df.groupby(other_categories_sub1sectors)
    results_sub1sectors = grouped_data_sub1sectors.apply(lambda group: check_totals('sub1sectors', group))
    subtotal_sub1sectors_historic = years_aggregated_df.merge(results_sub1sectors.reset_index(), on=other_categories_sub1sectors, how='left')[0].apply(lambda x: x[0]).fillna(False)
    subtotal_sub1sectors_predicted = years_aggregated_df.merge(results_sub1sectors.reset_index(), on=other_categories_sub1sectors, how='left')[0].apply(lambda x: x[1]).fillna(False)


    # Combine the results
    # Combining results based on whether any of the historic OR predicted subtotals are True
    years_aggregated_df['subtotal_historic'] = (
        (condition_subfuels & subtotal_subfuels_historic) |
        (condition_sub4sectors & subtotal_sub4sectors_historic) |
        (condition_sub3sectors & subtotal_sub3sectors_historic) |
        (condition_sub2sectors & subtotal_sub2sectors_historic) |
        (condition_sub1sectors & subtotal_sub1sectors_historic)
    ) & overarching_conditions | condition2

    years_aggregated_df['subtotal_predicted'] = (
        (condition_subfuels_predicted & subtotal_subfuels_predicted) |
        (condition_sub4sectors_predicted & subtotal_sub4sectors_predicted) |
        (condition_sub3sectors_predicted & subtotal_sub3sectors_predicted) |
        (condition_sub2sectors_predicted & subtotal_sub2sectors_predicted) |
        (condition_sub1sectors_predicted & subtotal_sub1sectors_predicted)
    ) & overarching_conditions_predicted | condition2

    # Subtotal column for aggregating
    # years_aggregated_df['subtotal'] = ~((years_aggregated_df['subtotal_historic'] == False) & (years_aggregated_df['subtotal_predicted'] == False))

    years_aggregated_df['subtotal'] = ~((years_aggregated_df['subtotal_historic'] == False) & 
                                        (years_aggregated_df['subtotal_predicted'] == False)) & \
                                    ~((years_aggregated_df['sectors'] == '17_nonenergy_use') & 
                                        (years_aggregated_df['subtotal_predicted'] == False))


    #years_aggregated_df.to_csv('years_aggregated_df.csv', index=False)
    
    # Merge the 'subtotal' column
    results_layout_df = pd.merge(results_layout_df, 
                                years_aggregated_df[shared_categories + ['subtotal_historic', 'subtotal_predicted', 'subtotal']], 
                                on=shared_categories, 
                                how='left')





    # Melt the DataFrame
    df_for_aggregating = results_layout_df.melt(id_vars=shared_categories + ['subtotal_historic', 'subtotal_predicted', 'subtotal'], var_name='year', value_name='value')
    df_for_aggregating['year'] = df_for_aggregating['year'].astype(int)

    # Drop the historic rows
    df_for_aggregating = df_for_aggregating.loc[~df_for_aggregating['year'].between(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR)]


    # # Function to check the totals for both 'value_historic' and 'value_predicted'
    # def calculate_and_update_totals(column_name, group):
    #     total_row = group[group[column_name] == True]
    #     other_rows = group[group[column_name] == False]

    #     if not total_row.empty:
    #         # Calculate the total using rows set to 'false' in 'subtotal' column
    #         sum_others = other_rows['value'].sum()

    #         # Update the total value into the row set to 'true' in 'subtotal' column
    #         group.loc[group['subtotal'] == True, 'value'] = sum_others

    #     return group


    # # Calculating for subfuels
    # other_categories_subfuels = [cat for cat in shared_categories if cat != 'subfuels'] + ['year']
    # grouped_data_subfuels = df_for_aggregating.groupby(other_categories_subfuels, group_keys=False)  # Add group_keys=False to maintain original indexing
    # df_for_aggregating = grouped_data_subfuels.apply(lambda group: calculate_and_update_totals('subfuels', group))


    # def calculate_totals(df):
    #     # Create a mask to filter out rows
    #     mask_true = (df['fuels'] == '19_total') & (df['subtotal'] == 'true')
    #     true_rows = df[mask_true].copy()
        
    #     # Aggregate values for rows where 'subtotal' is 'false'
    #     agg_values = df[df['subtotal'] == 'false'].groupby(shared_categories + ['year']).sum()
    #     agg_values.to_csv('agg_values.csv')
        
    #     # For each row where 'fuels' is '19_total' and 'subtotal' is 'true', determine the effective grouping columns
    #     for idx, row in true_rows.iterrows():
    #         effective_grouping_columns = [col for col in shared_categories if row[col] != 'x']
    #         effective_grouping_columns.append('year')
            
    #         # Fetch aggregated value based on effective grouping columns
    #         group_key = tuple(row[col] for col in effective_grouping_columns)
    #         if group_key in agg_values.index:
    #             for col in df.columns:
    #                 if col not in shared_categories + ['subtotal']:
    #                     df.at[idx, col] = agg_values.loc[group_key, col]

    #     return df

    # result = calculate_totals(df_for_aggregating)


    def aggregate_for_19_total(df, columns_to_exclude=[]):
        # Base columns to always exclude
        base_excluded_cols = ['fuels', 'subfuels']
        
        # Combine base excluded columns with the ones provided
        excluded_cols = base_excluded_cols + columns_to_exclude
        
        group_columns = [cat for cat in shared_categories if cat not in excluded_cols] + ['year']
        
        sum_df = df[df['subtotal'] == False].groupby(group_columns)['value'].sum().reset_index()
        
        # Drop rows from sum_df where 'value' is NaN or 0
        sum_df = sum_df.dropna(subset=['value'])
        sum_df = sum_df[sum_df['value'] != 0]
        
        # Add back the removed columns with specified values
        sum_df['fuels'] = '19_total'
        for col in columns_to_exclude+['subfuels']:
            sum_df[col] = 'x'
        
        # Create a mapper based on shared_categories+['year'] and 'value' for faster look-up
        value_mapper = sum_df.set_index(shared_categories+['year'])['value'].to_dict()

        # Update the original dataframe only if 'fuels' is '19_total' and the value is 0 or NaN
        mask = (df['fuels'] == '19_total') & df['value'].isin([0, None, np.nan])
        df.loc[mask, 'value'] = df[mask][shared_categories+['year']].apply(tuple, axis=1).map(value_mapper)
        
        return df

    # Using the function with various excluded columns
    df_for_aggregating = aggregate_for_19_total(df_for_aggregating, columns_to_exclude=['sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors'])
    df_for_aggregating = aggregate_for_19_total(df_for_aggregating, columns_to_exclude=['sub2sectors', 'sub3sectors', 'sub4sectors'])
    df_for_aggregating = aggregate_for_19_total(df_for_aggregating, columns_to_exclude=['sub3sectors', 'sub4sectors'])
    df_for_aggregating = aggregate_for_19_total(df_for_aggregating, columns_to_exclude=['sub4sectors'])
    df_for_aggregating = aggregate_for_19_total(df_for_aggregating)







    df_for_aggregating.to_csv('test.csv', index=False)



    # def aggregate_and_map(df, column):
    #     # Initial columns for grouping
    #     group_columns_init = [cat for cat in shared_categories if cat != column] + ['year']

    #     # Exclude specified values in the 'fuels' column
    #     exclude_fuels = ['19_total', '20_total_renewables', '21_modern_renewables']

    #     # Create a mask for columns that have 'x'
    #     x_mask = (df[group_columns_init] == 'x')

    #     # Find the columns that don't have 'x' for each row and store them
    #     df['dynamic_group'] = x_mask.apply(lambda row: ','.join([col for idx, col in enumerate(group_columns_init) if not row[idx]]), axis=1)

    #     # Compute sum based on the dynamic grouping
    #     valid_rows = df['subtotal'] == False
    #     for fuel in exclude_fuels:
    #         valid_rows &= df['fuels'] != fuel
    #     sum_df = df[valid_rows].groupby(['dynamic_group', 'year'])['value'].sum()

    #     # Map the sum to the subtotal rows using the dynamic group
    #     mask = df['subtotal'] & df['value'].isin([0, None, np.nan])
    #     for fuel in exclude_fuels:
    #         mask &= df['fuels'] != fuel
    #     df.loc[mask, 'value'] = df[mask].set_index(['dynamic_group', 'year']).index.map(sum_df).values

    #     # Drop the helper column
    #     df.drop('dynamic_group', axis=1, inplace=True)

    #     return df


    # def aggregate_for_19_total(df):
    #     # Determine columns for dynamic grouping (excluding 'fuels' and 'subtotal')
    #     consider_columns = [col for col in shared_categories if col not in ['fuels', 'subfuels', 'subtotal']]
        
    #     # Step 1: Determine dynamic grouping for subtotal rows
    #     subtotal_mask = (df['fuels'] == '19_total') & df['subtotal']
    #     non_x_mask = (df[consider_columns] != 'x')
    #     df['dynamic_grouping'] = non_x_mask.apply(lambda row: ','.join(row.index[row]), axis=1)
        
    #     # Only focus on rows where subtotal is False and not 19_total for Step 2
    #     non_subtotal_mask = ~df['subtotal'] & (df['fuels'] != '19_total')
        
    #     # Step 2: Aggregate values based on dynamic groupings
    #     aggregated_values = df[non_subtotal_mask].groupby(df['dynamic_grouping'].tolist() + ['year'])['value'].sum()
        
    #     # Step 3: Assign sums to the relevant rows
    #     df['group_year'] = df['dynamic_grouping'] + ',' + df['year'].astype(str)
    #     subtotal_rows_group_year = df.loc[subtotal_mask, 'group_year']

    #     df.loc[subtotal_mask, 'value'] = subtotal_rows_group_year.map(aggregated_values.to_dict())
        
    #     # Cleanup: Remove the helper columns
    #     df.drop(['dynamic_grouping', 'group_year'], axis=1, inplace=True)
            
    #     return df








    # # # Use the function for each set of categories
    # # df_for_aggregating = aggregate_and_map(df_for_aggregating, 'subfuels')
    # # df_for_aggregating = aggregate_and_map(df_for_aggregating, 'sub4sectors')
    # # df_for_aggregating = aggregate_and_map(df_for_aggregating, 'sub3sectors')
    # # df_for_aggregating = aggregate_and_map(df_for_aggregating, 'sub2sectors')
    # # df_for_aggregating = aggregate_and_map(df_for_aggregating, 'sub1sectors')

    # # Using the aggregate_for_19_total function to handle '19_total' calculations
    # df_for_aggregating = aggregate_for_19_total(df_for_aggregating)

    # df_for_aggregating.to_csv('df_for_aggregating.csv', index=False)



    pivoted_df = df_for_aggregating.pivot_table(index=shared_categories+['subtotal'], columns='year', values='value').reset_index()

    # Change columns to str
    pivoted_df.columns = pivoted_df.columns.astype(str)

    # Reorder columns
    pivoted_columns_order = shared_categories + [str(year) for year in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1)]# + ['subtotal']
    pivoted_df = pivoted_df[pivoted_columns_order]
    pivoted_df.to_csv('pivoted_df.csv', index=False)

    # Drop the projected year columns
    results_layout_df = results_layout_df.drop(columns=[str(year) for year in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1)], errors='ignore')
    
    
    results_layout_df = results_layout_df.merge(pivoted_df, on=shared_categories, how='left')
    layout_columns_order = shared_categories + [str(year) for year in range(EBT_EARLIEST_YEAR, OUTLOOK_LAST_YEAR+1)] + ['subtotal_historic', 'subtotal_predicted', 'subtotal']
    results_layout_df = results_layout_df[layout_columns_order]
    results_layout_df.to_csv('results_layout_df.csv', index=False)







    #Check if new_layout_df and results_df have the same number of rows
    #assert new_layout_df.shape[0] == results_df.shape[0], f"Layout dataframe and {file} do not have the same number of rows.\nLayout dataframe rows: {new_layout_df.shape[0]}\n{file} rows: {results_df.shape[0]}"



    # Define the year range to drop
    year_range = range(1980, 2021)

    # Create a list of columns to drop
    aggregating_columns_to_drop = ['sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors'] + [str(year) for year in year_range if str(year) in layout_df.columns]

    # Drop the specified columns and year columns from the layout_df DataFrame
    filtered_df = results_layout_df.drop(aggregating_columns_to_drop, axis=1).copy()


    # Create a function to streamline the process for each category
    def process_data(df, desired_sectors, sector_value):
        # Filter based on subtotal == False
        filtered_df_subtotal_false = df[df['subtotal'] == False]

        # Filter based on desired sectors
        desired_df = filtered_df_subtotal_false[filtered_df_subtotal_false['sectors'].isin(desired_sectors)].copy()

        # Group by necessary columns and aggregate
        grouped_df = desired_df.groupby(['scenarios', 'economy', 'fuels', 'subfuels']).sum().reset_index()

        # Add missing columns with 'x'
        for col in shared_categories:
            if col not in grouped_df.columns:
                grouped_df[col] = 'x'

        # Reorder columns to match shared_categories order
        ordered_columns = shared_categories + [col for col in grouped_df.columns if col not in shared_categories]
        grouped_df = grouped_df[ordered_columns]

        # Update the 'sectors' column
        grouped_df['sectors'] = sector_value

        return grouped_df

    # Apply the function for each of the categories
    tfc_grouped_df = process_data(filtered_df, ['14_industry_sector', '15_transport_sector', '16_other_sector', '17_nonenergy_use'], '12_total_final_consumption')
    tfec_grouped_df = process_data(filtered_df, ['14_industry_sector', '15_transport_sector', '16_other_sector'], '13_total_final_energy_consumption')
    tpes_grouped_df = process_data(filtered_df, ['9_total_transformation_sector', '10_losses_and_own_use', '11_statistical_discrepancy', '12_total_final_consumption'], '07_total_primary_energy_supply')

    # Combine the grouped DataFrames
    merged_grouped_df = pd.concat([tfc_grouped_df, tfec_grouped_df, tpes_grouped_df])
    merged_grouped_df.drop(columns=['subtotal_historic', 'subtotal_predicted', 'subtotal'], inplace=True)

    merged_grouped_df.to_csv('merged_grouped_df.csv')



    def aggregating_19_total(df):
        # Melt the dataframe
        df_melted = df.melt(id_vars=[col for col in df.columns if col not in df.columns[df.columns.str.isnumeric()]],
                            value_vars=[col for col in df.columns[df.columns.str.isnumeric()]],
                            var_name='year',
                            value_name='value')

        # List of columns to be excluded during aggregation
        excluded_cols = ['fuels', 'subfuels', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors']

        # Columns to be used for aggregation
        group_columns = [cat for cat in shared_categories if cat not in excluded_cols] + ['year']

        # Aggregate based on group_columns
        sum_df = df_melted.groupby(group_columns)['value'].sum().reset_index()

        # Add back the removed columns with specified values
        sum_df['fuels'] = '19_total'
        for col in excluded_cols[1:]:  # We start from 1 as 'fuels' is already addressed above
            sum_df[col] = 'x'

        # Concatenate the aggregation results back to the melted dataframe
        df_melted = pd.concat([df_melted, sum_df], ignore_index=True)

        # Pivot the dataframe back to its original format
        df_pivoted = df_melted.pivot_table(index=[col for col in df_melted.columns if col not in ['year', 'value']],
                                        columns='year',
                                        values='value',
                                        aggfunc='sum').reset_index()

        return df_pivoted

    # Using the function
    merged_grouped_df = aggregating_19_total(merged_grouped_df)

    merged_grouped_df.to_csv('merged_grouped_df3.csv', index=False)



    def aggregating_aggregates(df):
        # Melt the dataframe
        df_melted = df.melt(id_vars=[col for col in df.columns if col not in df.columns[df.columns.str.isnumeric()]],
                            value_vars=[col for col in df.columns[df.columns.str.isnumeric()]],
                            var_name='year',
                            value_name='value')

        excluded_cols = ['subfuels']

        group_columns = [cat for cat in shared_categories if cat not in excluded_cols] + ['year']

        sum_df = df_melted.groupby(group_columns)['value'].sum().reset_index()

        # Add back the removed columns with specified values
        for col in excluded_cols:
            sum_df[col] = 'x'

        # Filter out rows where 'value' is 0 before merging
        sum_df = sum_df[sum_df['value'] != 0]

        # Merge the aggregation results back to the melted dataframe
        df_melted = pd.merge(df_melted, sum_df, on=group_columns+['subfuels'], how='left', suffixes=('', '_aggregated'))

        # Only replace 'value' with 'value_aggregated' where 'value_aggregated' is not NaN
        mask = ~df_melted['value_aggregated'].isna()
        df_melted.loc[mask, 'value'] = df_melted.loc[mask, 'value_aggregated']

        # Drop the extra columns created during merge
        df_melted.drop(['value_aggregated', 'year_aggregated'] if 'year_aggregated' in df_melted else 'value_aggregated', axis=1, inplace=True)

        # Pivot the dataframe back to its original format
        df_pivoted = df_melted.pivot_table(index=[col for col in df_melted.columns if col not in ['year', 'value']],
                                        columns='year',
                                        values='value',
                                        aggfunc='sum').reset_index()

        return df_pivoted

    # Using the function
    merged_grouped_df = aggregating_aggregates(merged_grouped_df)

    
    merged_grouped_df.to_csv('merged_grouped_df2.csv', index=False)







    # Get the unique sectors
    aggregate_sectors_list = merged_grouped_df['sectors'].unique().tolist()

    # Create a new DataFrame with rows that match the sectors from the results DataFrame
    new_aggregate_layout_df = results_layout_df[results_layout_df['sectors'].isin(aggregate_sectors_list)].copy()

    # Drop the rows that were updated in the new DataFrame from the original layout DataFrame
    dropped_aggregate_layout_df = results_layout_df[~results_layout_df['sectors'].isin(aggregate_sectors_list)].copy()


    new_aggregate_layout_df.drop(columns=columns_to_drop, inplace=True)

    # Merge the DataFrames based on the shared category columns
    aggregate_merged_df = new_aggregate_layout_df.merge(merged_grouped_df, on=shared_categories, how='left')


    # Combine the original layout_df with the merged_df
    layout_df = pd.concat([dropped_aggregate_layout_df, aggregate_merged_df])

    # Define the folder path where you want to save the file
    folder_path = f'results/{SINGLE_ECONOMY}/merged'

    # Check if the folder already exists
    if not os.path.exists(folder_path):
        # If the folder doesn't exist, create it
        os.makedirs(folder_path)

    #save the combined data to a new Excel file
    #layout_df.to_excel('../../tfc/combined_data.xlsx', index=False, engine='openpyxl')
    date_today = datetime.now().strftime('%Y%m%d')
    if USE_SINGLE_ECONOMY:
        layout_df.to_csv(f'{folder_path}/merged_file_{SINGLE_ECONOMY}_{date_today}.csv', index=False)
    else:
        layout_df.to_csv(f'results/merged_file{date_today}.csv', index=False)
        
    return layout_df

import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from utility_functions import *
import warnings

def calculate_subtotals(df, shared_categories, df_name='results'):
    """
    Aggregates subtotals for each combination of categories. Then drops any of these calcalted subtotals that are already in the data. This is because the data that is already in there is actually the most specific data for each group, so we should keep it, rather than the subtotal.
    
    Please note that this function requires labelling of subtotals to have already been done. This is because it will look for any subtotals that are already in the data and remove them. If there arent any subtotals in the data, then this function will eventually throw an error.
    
    
    Args:
        df (pd.DataFrame): The input data frame.
        shared_categories (list): List of shared category column names.

    Returns:
        pd.DataFrame: DataFrame with aggregated subtotals.
    """
    #drop any subtotals already in the data, as we will recalcualte them soon. 
    df = df[df['is_subtotal'] == False].copy()
    df = df.drop(columns=['is_subtotal']).copy()
    
    ############################
    def calculate_subtotal_for_columns(melted_df, cols_to_sum):
        #gruop by the shared categories except the ones we are summing (in cols_to_sum) and sum the values. By doing this on each combination of the shared cateorgires, starting from the most specific ones, we can create subtotals for each combination of the shared categories.        
        group_cols = [col for col in melted_df.columns if col not in ['value'] and col not in cols_to_sum]
        
        agg_df = melted_df.copy()
        
        #ignore where all of the cols in cols_to_sum are 'x'. This is where  the data is at its most detailed level already. We will ignore these rows and tehrefor not create a subtotal for them.
        agg_df = agg_df[~(agg_df[cols_to_sum] == 'x').all(axis=1)].copy()#but what this will do is create duplcates?i think this is the falt of mixing subfuels and subsectors as they are independt. need to trawt them separately
        # agg_df = agg_df[~(agg_df[cols_to_sum] == 'x').any(axis=1)].copy()#i think this is causeing us to drop allpossible rows to gruop by. resultuiing in no new subtotals being created. TODO: investigate this.
        agg_df = agg_df.groupby(group_cols, as_index=False)['value'].sum().copy()
        for omit_col in cols_to_sum:
            agg_df[omit_col] = 'x'

        return agg_df
    ############################
    
    # Create a list to store each subset result
    subtotalled_results = pd.DataFrame()

    # Define different subsets of columns to find subtotals for. Should be every combination of cols when you start from the msot specific and add one level of detail each time. but doesnt include the least specifc categories since those cannot be subtotaled to a moer detailed level. 
    #if the columns change from the layout then this will need to be updated.
    sets_of_cols_to_sum = [
        ['sub4sectors'],
        ['sub3sectors', 'sub4sectors'],
        ['sub2sectors', 'sub3sectors', 'sub4sectors'],
        ['sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors']
    ]
    melted_df = df.melt(id_vars=shared_categories,
                            value_vars=[col for col in df.columns if str(col).isnumeric()],
                            var_name='year',
                            value_name='value')
    
    # Process the DataFrame with each cols_to_sum combination so you get a subtotal calculated for every level of detail.
    for cols_to_sum in sets_of_cols_to_sum:
        subtotalled_results = pd.concat([subtotalled_results,calculate_subtotal_for_columns(melted_df, cols_to_sum)], ignore_index=True)
    #then run it to calauclte a subtotl of each group, including the new subtotals for subsectors, where the subfuel is x. 
    subtotalled_results = pd.concat([subtotalled_results,calculate_subtotal_for_columns(pd.concat([subtotalled_results,melted_df]), ['subfuels'])], ignore_index=True)
    
        
    # Fill 'x' for the aggregated levels as they will just be nas
    for col in sets_of_cols_to_sum[-1]:
        #check for nas
        if subtotalled_results[col].isna().any():
            #fill nas with x
            breakpoint()
            raise Exception("WARNING: There are nas in the subtotaled DataFrame.")#uplifted to exception as this is unexpected and should be investigated.
            subtotalled_results[col] = subtotalled_results[col].fillna('x').copy()
            
    #check for duplicates in these new subtotals based on all the cols except value.. if there are duplicates then something perhaphs something went wrong with the subtotaling. But if the values are all 0 then just drop them.
    #Note: We are taking a risk of not catching potential errors if we assume we can drop subtotals that are duplciates and = 0...but if we dont do this it is quite complicated to work find the error anyway. It should only happen if there is not specific enough data in the layout file, so 0's aregetting subtotaled, and then being foudn asduplicates beside what is actually the most specific data for that group. So we will drop these by ideniftying if we are working on the layout file, otherwise throw an error.
    duplicates = subtotalled_results[subtotalled_results.duplicated(subset=[col for col in subtotalled_results.columns if col not in ['value']], keep=False)]
    if duplicates.shape[0] > 0:
        duplicates_non_zero = duplicates[duplicates['value'] != 0].copy()
        duplicates_non_zero = duplicates_non_zero[duplicates_non_zero.duplicated(subset=[col for col in duplicates_non_zero.columns if col not in ['value']], keep=False)]
        if duplicates_non_zero.shape[0] > 0 or df_name != 'layout':
            #sort them
            duplicates.sort_values(by=[col for col in duplicates.columns if col not in ['value']], inplace=True)
            if duplicates.shape[0] > 0:
                print(duplicates)
                breakpoint()   
                raise Exception("There are duplicates in the subtotaled DataFrame.")
        else:
            #sort by value and keep the last duplicate always since it will not be 0.
            subtotalled_results = subtotalled_results.sort_values(by=['value'], ascending=True).drop_duplicates(subset=[col for col in subtotalled_results.columns if col not in ['value']], keep='last').copy()
            
    ###################
    #now merge with the original df and drop where there are any subtotals that match rows in the origianl data. Since we removed all labelled subtotals at the start of the funciton we must assume that these matching rows are the most specific data points for their cateogires, and thereofre shouldnt be replaced with a subtotal! (as that would end up summing up to a subtotal of 0!). So we will keep the original data and remove the subtotalled data.
    merged_data = melted_df.merge(subtotalled_results, on=shared_categories+['year'], how='outer', suffixes=('_original', '_subtotalled'), indicator=True)
    
    most_specific_values = merged_data[(merged_data['_merge'] == 'both')].copy()
    subtotalled_values = merged_data[(merged_data['_merge'] == 'right_only')].copy()
    
    original_values = merged_data[(merged_data['_merge'] == 'left_only')].copy()
    most_specific_values['value'] = most_specific_values['value_original']
    subtotalled_values['value'] = subtotalled_values['value_subtotalled']
    original_values['value'] = original_values['value_original']
    
    most_specific_values['is_subtotal'] = False
    subtotalled_values['is_subtotal'] = True
    original_values['is_subtotal'] = False
    
    #concat all together
    final_df = pd.concat([most_specific_values, subtotalled_values, original_values], ignore_index=True)
    #drop merge and value_original and value_subtotalled
    final_df.drop(columns=['_merge', 'value_original', 'value_subtotalled'], inplace=True)
    
    # Check for any duplicates in (subset=[col for col in final_df.columns if col not in ['value']](they shouldn't exist)
    duplicates = final_df[final_df.duplicated(subset=[col for col in final_df.columns if col not in ['value']], keep=False)]
    if duplicates.shape[0] > 0:
        print("WARNING: There are duplicates in the subtotaled DataFrame.")
        print(duplicates)
        breakpoint()   
        raise Exception("There are duplicates in the subtotaled DataFrame.") 
    
    ###################
    #make final_df wide
    final_df = final_df.pivot(index=shared_categories+['is_subtotal'], columns='year', values='value').reset_index()

    return final_df


def remove_all_zeros(results_df, years_to_keep_in_results):
    #since the layout file contains all possible rows (once we run calculate_subtotals on it), we can remove all rows where the values for a row are all zero in the results file, since when we merge we will still be able to keep those rows from the layout file in the final df. This wil help to significantly reduce the size of resutls files as well as ignore any issues that arent issues because they are all zeros. For example we had an issue where a subfuel was being sued for a subtotal where the sectors it was subtotaling didnt have that subfuel. But since the values were all zeros, it didnt matter.
    results_df = results_df.loc[~(results_df[years_to_keep_in_results] == 0).all(axis=1)].copy()
    return results_df

def check_for_differeces_between_layout_and_results_df(layout_df, filtered_results_df, shared_categories, file, CHECK_FOR_MISSING_DATA_IN_RESULTS_FILE=False):
    # Check if there are differences between the layout DataFrame and the results DataFrame. 
    differences = []
    
    # Compare the shared categories between layout and results DataFrame
    layout_shared_categories = layout_df[shared_categories]
    results_shared_categories = filtered_results_df[shared_categories]
    #It will check first for any categories in the results data that arent in the layout. 
    for category in shared_categories:
        diff_variables = results_shared_categories.loc[~results_shared_categories[category].isin(layout_shared_categories[category]), category].unique()
        for variable in diff_variables:
            differences.append((variable, category, 'results'))
    #then check for any categories in the layout data that arent in the results.This is pretty extreme but one day it could be useful for identifying where we could add more data to the models.
    if CHECK_FOR_MISSING_DATA_IN_RESULTS_FILE:
        for category in shared_categories:
            diff_variables = layout_shared_categories.loc[~layout_shared_categories[category].isin(results_shared_categories[category]), category].unique()
            for variable in diff_variables:
                differences.append((variable, category, 'layout'))
    # Extract the file name from the file path
    file_name = os.path.basename(file)

    # Use assert to check if there are any differences
    if len(differences) > 0:
        print(f"Differences found in results file: {file_name}\n\nDifferences:\n" + '\n'.join([f"There is no '{variable}' in '{category}' in the {df}" for variable, category, df in differences]))
        #save to a csv so we can check it later. in Outlook9th_EBT\data\temp\error_checking
        pd.DataFrame(differences, columns=['variable', 'category', 'df']).to_csv(f'data/temp/error_checking/{file_name}_differences.csv', index=False)
        print(f"Differences saved to data/temp/error_checking{file_name}_differences.csv")

def check_bunkers_are_negative(filtered_results_df, file):
    
    # Filter rows where the sector is '05_international_aviation_bunkers'
    sectors_to_check = ['05_international_aviation_bunkers', '04_international_marine_bunkers']
    filtered_rows = filtered_results_df['sectors'].isin(sectors_to_check)

    # For each year from 2021 to 2070, check if the value is positive, if yes, throw an error
    for year in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1):
        # Check if the value is positive
        if (filtered_results_df.loc[filtered_rows, str(year)] > 0).any():
            breakpoint()
            raise Exception(f"{file} has positive values for {sectors_to_check} in {year}.")
            
def filter_for_only_buildings_data_in_buildings_file(results_df):
    #this is only because the buildings file is being given to us with all the other data in it. so we need to filter it to only have the buildings data in it so that nothing unexpected happens.
    #check for data in the end year where sub1sectors is 16_01_buildings
    if results_df.loc[results_df['sub1sectors'] == '16_01_buildings', str(OUTLOOK_BASE_YEAR+1):str(OUTLOOK_LAST_YEAR)].notnull().any().any():
        cols = [str(i) for i in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1)]
        #set 0's to null for now, as we dont mind if there are 0's in the buildings file. we just dont want any other data.
        bad_rows = results_df.copy()
        bad_rows[cols] = bad_rows[cols].replace(0, np.nan)
        #search for any rows where there is non na or 0 data in the years we are interested in, and where sub1sectors is not 16_01_buildings
        bad_rows = bad_rows.loc[(bad_rows[cols].notnull().any(axis=1)) & (bad_rows['sub1sectors'] != '16_01_buildings')].copy()
        if bad_rows.shape[0] > 0:
            print(bad_rows)            
            breakpoint()
            raise Exception("There is data in the buildings file that is not in the buildings sector. This is unexpected. Please check the buildings file.")
            
        results_df = results_df[results_df['sub1sectors'] == '16_01_buildings'].copy()
    return results_df

def trim_layout_before_merging_with_results(layout_df, concatted_results_df):
    # Get the unique sectors from the results_df
    sectors_list = concatted_results_df['sectors'].unique().tolist()

    # Create a new DataFrame with rows that match the sectors from the results DataFrame. This allows us to run the merge on a results DataFrame that doesnt contain all the sectors. This is expected to happen because we will run this code multiple times, with only demand sectors, demand and transformation and so on.
    new_layout_df = layout_df[layout_df['sectors'].isin(sectors_list)].copy()

    #Drop the rows that were updated in the new DataFrame from the original layout DataFrame
    missing_sectors_df = layout_df[~layout_df['sectors'].isin(sectors_list)].copy()
    #Show user what sectors are missing:
    if missing_sectors_df.shape[0] > 0:
        print("The following sectors are missing from the results files:")
        print(missing_sectors_df['sectors'].unique().tolist())
        
    # Drop columns outllook_base_year to outlook_last_year from new_layout_df
    columns_to_drop = [year for year in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1)]
    new_layout_df.drop(columns=columns_to_drop, inplace=True)
    
    return new_layout_df, missing_sectors_df

def trim_results_before_merging_with_layout(concatted_results_df,shared_categories):
    #drop years not in the outlook (they are in the layout file)
    years_to_keep = [year for year in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1)]
    # concatted_results_df = concatted_results_df[shared_categories + ['is_subtotal'] + years_to_keep].copy()#dont think this is necessary
    # Drop rows with NA or zeros in the year columns from concatted_results_df
    concatted_results_df.dropna(subset=years_to_keep, how='all', inplace=True)
    concatted_results_df = concatted_results_df.loc[~(concatted_results_df[years_to_keep] == 0).all(axis=1)]

    # Check for duplicate rows in concatted_results_df based on shared_categories
    duplicates = concatted_results_df[concatted_results_df.duplicated(subset=shared_categories, keep=False)]
    # if there are duplcaites we should let the user know.
    if duplicates.shape[0] > 0:
        print("Duplicates found in concatted_results_df based on shared_categories:")
        print(duplicates)
        
    # Remove the duplicate rows from concatted_results_df
    concatted_results_df = concatted_results_df.drop_duplicates(subset=shared_categories, keep='first').copy()

    # Print the updated number of rows in concatted_results_df
    print("Number of rows in concatted_results_df after removing rows without year values and duplicates:", concatted_results_df.shape[0])
    
    return concatted_results_df

def run_checks_on_merged_layout_results_df(merged_df, shared_categories, trimmed_layout_df, trimmed_concatted_results_df):
    #Check for duplicate rows in concatted_results_df. if there are something is wrong as
    if merged_df[merged_df.duplicated(subset=shared_categories, keep=False)].shape[0] > 0:
        breakpoint()
        raise Exception("Duplicate rows found in concatted_results_df. Check the results files.")

    # Check if there were any unexpected or extra rows in concatted_results_df
    unexpected_rows = merged_df[merged_df['_merge'] != 'both']
    if unexpected_rows.shape[0] > 0:
        missing_from_results_df = unexpected_rows[unexpected_rows['_merge'] == 'left_only']
        extra_in_results_df = unexpected_rows[unexpected_rows['_merge'] == 'right_only']
        print(f"Unexpected rows found in concatted_results_df. Check the results files.\nMissing from results_df: {missing_from_results_df.shape[0]}\nExtra in results_df: {extra_in_results_df.shape[0]}")#TODO IS THIS GOING TO BE TOO STRICT? WILL IT INCLUDE FUEL TYPES THAT SHOULD BE MISSING EG. CRUDE IN TRANSPORT, or even where there is no subttotals to create in the layout file since it is not specific enough (eg breaking domestic air into freight/passenger in transport sector)
        # breakpoint()
        # raise Exception("Unexpected rows found in concatted_results_df. Check the results files.")
    
    # Print the number of rows in both dataframes
    print("Number of rows in new_layout_df:", trimmed_layout_df.shape[0])
    print("Number of rows in merged_results_df:", trimmed_concatted_results_df.shape[0])
    print("Number of rows in merged_df:", merged_df.shape[0])

    #drop the _merge column
    merged_df.drop(columns=['_merge'], inplace=True)
    return merged_df

def calculate_sector_aggregates(df, sectors, aggregate_sector, shared_categories):
    """
    Calculate common aggregates for groups of sectors such as total transformation,
    total primary energy supply, etc. This is based on filtering out subtotal entries 
    and grouping by scenarios, economy, fuels, and subfuels.

    Args:
        df (pd.DataFrame): The dataframe containing energy data.
        sectors (list): The sectors to include in the aggregation.
        aggregate_sector (str): The specific aggregate to calculate.

    Returns:
        pd.DataFrame: The aggregated data for the desired sectors.
    """
    # Filter out subtotals and select the desired sectors
    df_filtered = df[(df['subtotal_layout'] == False) & (df['subtotal_results'] == False) & (df['sectors'].isin(sectors))].copy()

    # If calculating total primary energy supply, make all TFC components negative first, then add them to the other components.
    if aggregate_sector == '07_total_primary_energy_supply':
        #TPES needs to be calauclate as ??? (TODO write the process for calcualting tpes so the context for below is explained)
        # Define TFC sectors and negate their values
        tfc_sectors = ['14_industry_sector', '15_transport_sector', '16_other_sector', '17_nonenergy_use']
        df_tfc = df[(df['subtotal_layout'] == False) & (df['subtotal_results'] == False) & (df['sectors'].isin(tfc_sectors))]
        grouped_tfc = df_tfc.groupby(['scenarios', 'economy', 'fuels', 'subfuels']).sum(numeric_only=True).reset_index()
        
        # Make the values in all numeric columns negative
        numeric_cols = grouped_tfc.select_dtypes(include=[np.number]).columns
        grouped_tfc[numeric_cols] *= -1
        
        # Append the negated TFC to the filtered DataFrame and sum all up to get  the final aggregation
        aggregated_df = pd.concat([df_filtered, grouped_tfc], ignore_index=True)
        aggregated_df = aggregated_df.groupby(['scenarios', 'economy', 'fuels', 'subfuels']).sum(numeric_only=True).reset_index()
        
        #we also need a tpes for subtotals of fuels. to do this we can just do a sum of grouped_tfc, after dropping where subfuels = x, on .groupby(['scenarios', 'economy', 'fuels']) and set subfuels to x. 
        grouped_tfc_fuels_subtotals = grouped_tfc.loc[grouped_tfc.subfuels!='x']
        df_filtered_fuels_subtotals  = df_filtered[df_filtered.subfuels!='x']
        aggregated_df_fuels_sutotals = pd.concat([grouped_tfc_fuels_subtotals, df_filtered_fuels_subtotals]).groupby(['scenarios', 'economy', 'fuels']).sum(numeric_only=True).reset_index()
        aggregated_df_fuels_sutotals['subfuels'] = 'x'
        aggregated_df_fuels_sutotals['subtotal_layout'] = True
        aggregated_df_fuels_sutotals['subtotal_results'] = True
        
        aggregated_df = pd.concat([aggregated_df, aggregated_df_fuels_sutotals])
        
    elif aggregate_sector in ['13_total_final_energy_consumption', '12_total_final_consumption']:#these also need to ahve values calcualted for fuel subtotals, like TPES
        # If not calculating total primary energy supply, just perform the grouping and sum
        aggregated_df = df_filtered.groupby(['scenarios', 'economy', 'fuels', 'subfuels']).sum(numeric_only=True).reset_index()
        
        # do a sum of grouped_tfc, after dropping where subfuels = x, on .groupby(['scenarios', 'economy', 'fuels']) and set subfuels to x. 
        aggregated_df_fuels_subtotals = aggregated_df.loc[aggregated_df.subfuels!='x'].groupby(['scenarios', 'economy', 'fuels']).sum(numeric_only=True).reset_index().copy()
        aggregated_df_fuels_subtotals['subfuels'] = 'x'
        aggregated_df_fuels_subtotals['subtotal_layout'] = True
        aggregated_df_fuels_subtotals['subtotal_results'] = True
        
        aggregated_df = pd.concat([aggregated_df, aggregated_df_fuels_subtotals])
        
    else:
        raise Exception(f'Aggregate sector {aggregate_sector} not recognised')
    
    # Add missing columns with 'x'
    for col in shared_categories:
        if col not in aggregated_df.columns.to_list() + ['subtotal_layout', 'subtotal_results']:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="DataFrame is highly fragmented")
                aggregated_df[col] = 'x'

    # Reorder columns to match shared_categories order
    ordered_columns = shared_categories + [col for col in aggregated_df.columns if col not in shared_categories]
    aggregated_df = aggregated_df[ordered_columns]

    # Update the 'sectors' column
    aggregated_df['sectors'] = aggregate_sector
    return aggregated_df

def aggregate_19_total(df, shared_categories):
    # Melt the dataframe
    df_melted = df.melt(id_vars=[col for col in df.columns if col not in [year for year in range(EBT_EARLIEST_YEAR, OUTLOOK_LAST_YEAR+1)]],
                        value_vars=[col for col in df.columns if col in [year for year in range(EBT_EARLIEST_YEAR, OUTLOOK_LAST_YEAR+1)]],
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
    
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="DataFrame is highly fragmented")
        # Pivot the dataframe back to its original format
        df_pivoted = df_melted.pivot(index=[col for col in df_melted.columns if col not in ['year', 'value']], columns='year', values='value').reset_index()

    return df_pivoted

def aggregate_aggregates(df, shared_categories):
    # Melt the dataframe
    df_melted = df.melt(id_vars=[col for col in df.columns if col not in [year for year in range(EBT_EARLIEST_YEAR, OUTLOOK_LAST_YEAR+1)]],
                        value_vars=[col for col in df.columns if col in [year for year in range(EBT_EARLIEST_YEAR, OUTLOOK_LAST_YEAR+1)]],
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

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="DataFrame is highly fragmented")
        # Pivot the dataframe back to its original format
        df_pivoted = df_melted.pivot(index=[col for col in df_melted.columns if col not in ['year', 'value']], columns='year', values='value').reset_index()

    return df_pivoted

def label_subtotals(results_layout_df, shared_categories):  
    def label_subtotals_for_sub_col(df, sub_col, aggregate_categories):
        """
        This fucntion is run for each sub_col. It identifies and labels subtotal rows within the dataframe, based on the current subcol 
        
        If this row is not a subtotal for this sub_col then it wont change anythign, but if it is a subtotal for this sub_col then it will label it as a subtotal.
        
        (so, if the row is identified as a subtotal, then it will be labelled as a subtotal, but technically this means that the sub_col will be 'x' (see x_mask varaible), since it is a subtotal of all values in that sub_col, but for the category one level higher than that sub_col).

        This function goes through the dataframe to find subtotal rows based on the presence of 'x' in specific columns.
        
        It distinguishes between definite non-subtotals (where tehre is no data to aggregate) and potential subtotals (where there's a mix of detailed and aggregated data in that group). It then labels the potential subtotals accordingly.
        
        Parameters:
        - df (DataFrame): The dataframe to process.
        - sub_col (str): The name of the column to check for 'x' which indicates a subtotal.
        - totals (list): A list of values in the 'fuels' column that should be considered as total values and not subtotals.

        Returns:
        - DataFrame: The original dataframe with an additional column indicating whether a row is a subtotal.
        """
        #############################
        # the following section finds where all the values in a group for the sub_col are x. These are not subtotals since there must be no more specific values than them if there arent any non x values at all for this group in this sub_col. dont label them at all (since they will default to is_subtotal=False).
        # Create a mask for 'x' values in the sub_col 
        x_mask = (df[sub_col] == 'x')

        # Group by all columns except 'value' and sub_col and check if all values in sub_col are 'x' for that group 
        grouped = df.loc[x_mask, [col for col in df.columns if col not in ['value', sub_col]]].groupby([col for col in df.columns if col not in ['value', sub_col]]).size().reset_index(name='count')

        # Merge the original df with the grouped data to get the count of 'x' for each group
        merged = df.merge(grouped, on=[col for col in df.columns if col not in ['value', sub_col]], how='left')

        # Create a mask where the count is equal to the size of the group, indicating all values in sub_col were 'x' for that group
        non_subtotal_mask = merged['count'] == df.groupby([col for col in df.columns if col not in ['value', sub_col]]).transform('size').reset_index(drop=True)
        #############################
        
        df.reset_index(drop=True, inplace=True)
        
        #separate where all the values in the group are x. these are not subtotals since there are no more specific values than them. dont label them at all.
        df_definitely_not_subtotals = df[non_subtotal_mask].copy()#they could still be subtotals but those will be caught eventually
        df_maybe_subtotals = df[~non_subtotal_mask].copy()

        conditions = {
            # 'value_non_zero': df_maybe_subtotals['value'].notna() & (df_maybe_subtotals['value'] != 0),
            # 'is_aggregate_category': df_maybe_subtotals['fuels'].isin(aggregate_categories),
            'sub_col_is_x': df_maybe_subtotals[sub_col] == 'x'
        }
        # Combine conditions for subtotals
        conditions['is_subtotal'] =  conditions['sub_col_is_x'] #&conditions['value_non_zero'] & conditions['is_not_aggregate_category'] 
         
        # Apply the conditions to label subtotal rows. but only label if it is a subtotal. dont label with false if it is not a subtotal. this is because it could overwrite where we previously labelled it as a subtotal.
        df_maybe_subtotals['is_subtotal'] = np.where(conditions['is_subtotal'], True, df_maybe_subtotals['is_subtotal'])
        # Concatenate the two DataFrames back together
        df = pd.concat([df_definitely_not_subtotals, df_maybe_subtotals], ignore_index=True).reset_index(drop=True)
        
        return df
    
    # Melt the DataFrame
    df_melted = results_layout_df.melt(id_vars=shared_categories, var_name='year', value_name='value')
    df_melted['year'] = df_melted['year'].astype(int)

    # Define aggregate_categories. tehse wont besconsidered in any subtotals TODO I DONT KNOW WHY. ITS ADDED COMPLEXITY FOR WHAT?
    aggregate_categories = []# ['19_total', '20_total_renewables', '21_modern_renewables']

    #set is_subtotal to False. It'll be set to True, eventually, if it is a subtotal
    df_melted['is_subtotal'] = False
    
    for sub_col in ['subfuels', 'sub4sectors', 'sub3sectors', 'sub2sectors', 'sub1sectors']:
        df_melted = label_subtotals_for_sub_col(df_melted, sub_col, aggregate_categories)
    
    df_melted = df_melted.pivot(index=[col for col in df_melted.columns if col not in ['year', 'value']], columns='year', values='value').reset_index()
    
    return df_melted

def load_previous_merged_df(results_data_path, expected_columns, previous_merged_df_filename):
    
    if previous_merged_df_filename is None:
        #grab the most recent previous merge file
        previous_merged_df_filename = find_most_recent_file_date_id(results_data_path)
        
        if previous_merged_df_filename is None:
            print("No previous merged df found. Skipping comparison.")
            return None
        else:
            previous_merged_df = pd.read_csv(os.path.join(results_data_path, previous_merged_df_filename))
            #check the columns are as we expect:
            missing_cols = [col for col in expected_columns if col not in previous_merged_df.columns.tolist()]
            if len(missing_cols) > 0:
                breakpoint()
                print("WARNING: The previous merged df does not have the expected columns. Skipping comparison.")
                return None
            else:
                return previous_merged_df  
            
def compare_to_previous_merge(new_merged_df, shared_categories, results_data_path,previous_merged_df_filename=None, new_subtotal_columns=['subtotal_layout', 'subtotal_results'], previous_subtotal_columns=['subtotal_historic','subtotal_predicted','is_subtotal']):
    """Use this to compare to previously done merges. this is useful for checking if any changes to the code result in any changes to the ouput compared to a previous merge. it is a bit specific but possibly helpful in understanding how this complex process works!
    Args:
        new_merged_df (_type_): _description_
        shared_categories (_type_): _description_
        results_data_path (_type_): _description_
        previous_merged_df_filename (_type_, optional): _description_. Defaults to None.
        new_subtotal_columns (list, optional): _description_. Defaults to ['subtotal_layout', 'subtotal_results'].
        previous_subtotal_columns (list, optional): _description_. Defaults to ['subtotal_historic','subtotal_predicted','is_subtotal'].
    """
    #function that can be used to identify potential errors in new code or even just to check that the results are the same as before.
    if previous_merged_df_filename is None:
        expected_columns = previous_subtotal_columns + shared_categories
        previous_merged_df = load_previous_merged_df(results_data_path, expected_columns, previous_merged_df_filename)
        if previous_merged_df is None:
            return
    else:
        previous_merged_df = pd.read_csv(os.path.join(results_data_path, previous_merged_df_filename))
    
    #melt the years cols to make it easier to compare
    new_merged_df = new_merged_df.melt(id_vars=shared_categories+new_subtotal_columns, var_name='year', value_name='value')
    previous_merged_df = previous_merged_df.melt(id_vars=shared_categories + previous_subtotal_columns, var_name='year', value_name='value')
    
    #make sure both dataframes year values are ints
    new_merged_df['year'] = new_merged_df['year'].astype(int)
    previous_merged_df['year'] = previous_merged_df['year'].astype(int)
    
    #we will have to do the merge bit by bit as it is so large. so we will split it into years and then merge each year, check the data, then move on to the next year.
    left_only = pd.DataFrame()
    right_only = pd.DataFrame()
    both_different_rows = pd.DataFrame()
    all = pd.DataFrame()
    epsilon = 1e-6
    SAVE = False
    for years in np.array_split(new_merged_df['year'].unique(), 10):
        breakpoint()#is year int vs str
        all_ = pd.merge(new_merged_df[new_merged_df['year'].isin(years)], previous_merged_df[previous_merged_df['year'].isin(years)], on=shared_categories+['year'], how='outer', indicator=True, suffixes=('_new', '_previous'))
        #check for rows that arent in both dataframes
        left_only_ = all_[all_['_merge'] == 'left_only']
        right_only_ = all_[all_['_merge'] == 'right_only']
        #where both is true, see what the difference in value sum is between the two dataframes
        all_['value_sum_difference'] = all_['value_new'] - all_['value_previous']
        #if the difference is greater than epsilon, then there is a difference between the two dataframes and separate the different values into a new dataframe
        all_['is_different'] = all_['is_different'].astype(bool)
        all_.loc[(abs(all_['value_sum_difference']) > epsilon) & (all_['_merge'] == 'both'), 'is_different'] = True
        all_['is_different'] = all_['is_different'].fillna(False)
        
        left_only = pd.concat([left_only, left_only_], ignore_index=True)
        right_only = pd.concat([right_only, right_only_], ignore_index=True)
        
        all = pd.concat([all, all_], ignore_index=True)
    
    #rename left_only and right_only to new and previous_only
    merge_dict = {'left_only': 'new_only', 'right_only': 'previous_only'}
    all['_merge'] = all['_merge'].map(merge_dict).fillna(all['_merge'])
    
    if all['is_different'].any():
        SAVE=True
        breakpoint()
        print("{} differences in values, to an absolute sum of {} between previous and new merged df saved to data/temp/error_checking/compare_to_previous_merge.csv".format(all['is_different'].sum(), all['value_sum_difference'].sum()))
    
    #check out left_only and right_only
    if left_only.shape[0] > 0:
        SAVE=True
        print("{} rows that are only in the new merged df saved in data/temp/error_checking/compare_to_previous_merge.csv where _merge is left_only".format(left_only.shape[0]))
        breakpoint()
    
    if right_only.shape[0] > 0:
        SAVE=True
        print("{} rows that are only in the previous merged df saved in data/temp/error_checking/compare_to_previous_merge.csv where _merge is right_only".format(right_only.shape[0]))
        breakpoint()
    
    if SAVE:
        all.to_csv('data/temp/error_checking/compare_to_previous_merge.csv', index=False)
    else:
        print("No differences found between previous and new merged df.")
        
    return




def check_for_issues_by_comparing_to_layout_df(layout_df, results_layout_df, shared_categories):
    """Use this to check that the layout df and the newly processed layout df match for the years in the layout file. This should not happen, and if there are isues its likely some process in the merging_results script is wrong.."""
    #Check that the layout df matches aggregates_df where year is in  [year for year in range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1) if year in layout_df.columns]
    results_layout_df_layout_years = results_layout_df[shared_categories +  [col for col in results_layout_df.columns if col in range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1)]].copy()
    # layout_df_test = layout_df[shared_categories +  [col for col in results_layout_df.columns if col in range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1)]].copy()
    #test they match by doing an outer merge and checking where indicator != both
    shared_categories_old = shared_categories.copy()
    shared_categories_old.remove('subtotal_layout')
    shared_categories_old.remove('subtotal_results')
    merged_df_on_values = pd.merge(results_layout_df_layout_years, layout_df, on=shared_categories_old+  [col for col in results_layout_df_layout_years.columns if col in range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1)], how="outer", indicator=True)
    merged_df_on_categories_only = pd.merge(results_layout_df_layout_years, layout_df, on=shared_categories_old, how="outer", indicator=True, suffixes=('_new_layout_df', '_original_layout'))
    
    if merged_df_on_values[merged_df_on_values['_merge'] != 'both'].shape[0] > 0:
        #wrongly calculated rows occur where we have duplicates when you ignore the years. these dont show up in merged_df_on_categories_only.
        merged_df_bad_values = merged_df_on_values[merged_df_on_values['_merge'] != 'both'].copy()
        merged_df_bad_values = merged_df_bad_values[merged_df_bad_values.duplicated(subset=shared_categories_old, keep=False)]
        
        #idenitfy where the diffrence between values is > than epsilon. to do this we should first melt both dfs so we have a year and value col. then join on shared_categories_old and year. then we can compare the values and see where they are different.
        merged_df_bad_values = merged_df_bad_values.drop(columns=['subtotal_layout', 'subtotal_results']).copy()
        new_layout_df = merged_df_bad_values[merged_df_bad_values['_merge'] == 'new_layout_df'].copy()
        original_layout = merged_df_bad_values[merged_df_bad_values['_merge'] == 'original_layout'].copy()
        #melt both then join
        new_layout_df = new_layout_df.drop(columns=['_merge']).melt(id_vars=shared_categories_old, var_name='year', value_name='value')
        original_layout = original_layout.drop(columns=['_merge']).melt(id_vars=shared_categories_old, var_name='year', value_name='value')
        merged_df_bad_values = pd.merge(new_layout_df, original_layout, on=shared_categories_old+['year'], how='inner', suffixes=('_new_layout_df', '_original_layout'))
        epsilon = 1e-6
        merged_df_bad_values['difference'] = merged_df_bad_values['value_new_layout_df'] - merged_df_bad_values['value_original_layout']
        merged_df_bad_values = merged_df_bad_values[abs(merged_df_bad_values['difference']) > epsilon].copy()
        
        #where we have unexpected rows, this is where either _merge is new_layout_df or original_layout in merged_df_on_categories_only. new_layout_df means the layout file has a row that isnt in the results file. original_layout means the results file has a row that isnt in the layout file.
        missing_rows = merged_df_on_categories_only[merged_df_on_categories_only['_merge'] != 'both']
        
        #that covers all possible issues. we will now save them and ask the user to fix them.
        if merged_df_bad_values.shape[0] > 0:
            #put them in order to help see the issue
            merged_df_bad_values.sort_values(by=shared_categories_old, inplace=True)
            merged_df_bad_values.to_csv('data/temp/error_checking/merged_df_bad_values.csv', index=False)
            print("There are {} rows where the values in the results file do not match the values in the layout file. These rows have been saved to data/temp/error_checking/merged_df_bad_values.csv".format(merged_df_bad_values.shape[0]))
            breakpoint()
        if missing_rows.shape[0] > 0:
            #put them in order to help see the issue
            missing_rows.sort_values(by=shared_categories_old, inplace=True)
            #put the _merge col at front
            missing_rows = missing_rows[['_merge'] + missing_rows.columns[:-1].tolist()]
            missing_rows.to_csv('data/temp/error_checking/missing_rows.csv', index=False)
            print("There are {} rows where the results file is missing rows from the layout file. These rows have been saved to data/temp/error_checking/missing_rows.csv".format(missing_rows.shape[0]))
            breakpoint()
        if merged_df_bad_values.shape[0] > 0 or missing_rows.shape[0] > 0:
            raise Exception("The layout df and the newly processes layout df do not match for the years in the layout file. This should not happen.")
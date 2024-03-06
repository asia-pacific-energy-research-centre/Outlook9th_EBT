import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from utility_functions import *
import warnings
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

def label_subtotals(results_layout_df, shared_categories):  
    def label_subtotals_for_sub_col(df, sub_col):
        """
        This fucntion is run for each sub_col. It identifies and labels subtotal rows within the dataframe, based on the current subcol 
        
        If this row is not a subtotal for this sub_col then it wont change anythign, but if it is a subtotal for this sub_col then it will label it as a subtotal.
        
        (so, if the row is identified as a subtotal, then it will be labelled as a subtotal, but technically this means that the sub_col will be 'x' (see x_mask varaible), since it is a subtotal of all values in that sub_col, but for the category one level higher than that sub_col).

        This function goes through the dataframe to find subtotal rows based on the presence of 'x' in specific columns.
        
        It distinguishes between definite non-subtotals (where tehre is no data to aggregate) and potential subtotals (where there's a mix of detailed and aggregated data in that group). It then labels the potential subtotals accordingly.
        
        A similar thing to the x's has been added for the presence of 0's and nans. Note that a value is  definitely not a subtotal if EITHER of the conditions are met.
        
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
        grouped = df.loc[x_mask, [col for col in df.columns if col not in ['value', sub_col, 'is_subtotal']]].groupby([col for col in df.columns if col not in ['value', sub_col, 'is_subtotal']]).size().reset_index(name='count')

        # Merge the original df with the grouped data to get the count of 'x' for each group
        merged = df.merge(grouped, on=[col for col in df.columns if col not in ['value', sub_col, 'is_subtotal']], how='left')

        # Create a mask where the count is equal to the size of the group, indicating all values in sub_col were 'x' for that group.
        non_subtotal_mask = merged['count'] == df.groupby([col for col in df.columns if col not in ['value', sub_col, 'is_subtotal']]).transform('size').reset_index(drop=True)
        
        ############################# 
        #if more than one value are not zero/nan for this group, then it could be a subtotal, if not, its a definite non-subtotal since its the most specific data we have for this group.
        value_mask = (abs(df['value'])> 0)
        
        # Group by all columns except 'value' and sub_col and check how many values are >0 or <0 for that group
        grouped = df.loc[value_mask, [col for col in df.columns if col not in ['value', sub_col, 'is_subtotal']]].groupby([col for col in df.columns if col not in ['value', sub_col, 'is_subtotal']]).size().reset_index(name='count')

        # Merge the original df with the grouped data to get the count of 'x' for each group
        merged = df.merge(grouped, on=[col for col in df.columns if col not in ['value', sub_col, 'is_subtotal']], how='left')

        #fill nas in merged with True, as this is where no values are >0 or <0 for this group
        merged['count'] = merged['count'].fillna(True)
        
        # Create a mask where the count of values in the group that are >0 or <0 is <= 1, indicating that its not a subtotal 
        non_subtotal_mask2 = merged['count'] <= 1
        #############################
        
        # df.reset_index(drop=True, inplace=True)
        df['non_subtotal_mask'] = non_subtotal_mask
        df['non_subtotal_mask2'] = non_subtotal_mask2
        #separate where all the values in the group are x. these are not subtotals since there are no more specific values than them. dont label them at all.
        
        df_definitely_not_subtotals = df[non_subtotal_mask2 | non_subtotal_mask].copy()
        df_maybe_subtotals = df[~(non_subtotal_mask2 | non_subtotal_mask)].copy()
        df_maybe_subtotals[df_maybe_subtotals['sub1sectors'] == '15_01_domestic_air_transport']
        conditions = {
            'sub_col_is_x': df_maybe_subtotals[sub_col] == 'x'
        }
        # Combine conditions for subtotals
        conditions['is_subtotal'] =  conditions['sub_col_is_x'] 
        
        # Apply the conditions to label subtotal rows. but only label if it is a subtotal. dont label with false if it is not a subtotal. this is because it could overwrite where we previously labelled it as a subtotal.
        df_maybe_subtotals['is_subtotal'] = np.where(conditions['is_subtotal'], True, df_maybe_subtotals['is_subtotal'])
        # Concatenate the two DataFrames back together
        df = pd.concat([df_definitely_not_subtotals, df_maybe_subtotals], ignore_index=True).reset_index(drop=True)
        
        #drop non_subtotal_mask and non_subtotal_mask2
        df.drop(columns=['non_subtotal_mask', 'non_subtotal_mask2'], inplace=True)
        
        return df
    #####################################################################
    #SUBFUNCTION
    ######################################################################
        
    # Melt the DataFrame
    df_melted = results_layout_df.melt(id_vars=shared_categories, var_name='year', value_name='value')
    
    #Double check for duplicates:
    dupes = df_melted[df_melted.duplicated(subset=shared_categories+['year'], keep=False)]
    if dupes.shape[0] > 0:
        print('Found duplicates in subtotal input')
        breakpoint()
    
    #make sure year is int
    df_melted['year'] = df_melted['year'].astype(int)
    
    #remove year col and sum value col. this will allow us to more easily identify if a value is standalone or a sum of others. 
    df_melted_sum = df_melted.groupby([col for col in df_melted.columns if col not in ['year', 'value']]).sum(numeric_only=True).reset_index()
    df_melted_sum.drop(columns=['year'], inplace=True)

    #set is_subtotal to False. It'll be set to True, eventually, if it is a subtotal
    df_melted_sum['is_subtotal'] = False
    for sub_col in ['subfuels', 'sub4sectors', 'sub3sectors', 'sub2sectors', 'sub1sectors']:
        df_melted_sum = label_subtotals_for_sub_col(df_melted_sum, sub_col)
    
    #join df_melted_sum, wthout value col to df_melted on [col for col in df_melted.columns if col not in ['value'] to get the is_subtotal col in the original df_melted
    df_melted = pd.merge(df_melted, df_melted_sum[[col for col in df_melted_sum.columns if col not in ['value']]], on=[col for col in df_melted.columns if col not in ['year', 'value']], how='left')
    
    # # Drop rows where 'value' column has NaN
    # df_melted = df_melted.dropna(subset=['value']).copy()
    
    # Group by all columns except 'value' and sum the 'value' column
    df_melted = df_melted.groupby([col for col in df_melted.columns if col != 'value']).sum().reset_index().copy()
    
    duplicates = df_melted[df_melted.duplicated(subset=shared_categories + ['year'], keep=False)]
    if not duplicates.empty:
        print("Duplicates found:", duplicates)
        duplicates.to_csv('data/temp/error_checking/duplicates_in_tfc.csv', index=False)
    
    df_melted = df_melted.pivot(index=[col for col in df_melted.columns if col not in ['year', 'value']], columns='year', values='value').reset_index()
    
    return df_melted

def calculate_subtotals(df, shared_categories, DATAFRAME_ORIGIN):
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
    df_no_subtotals = df[df['is_subtotal'] == False].copy()
    df_no_subtotals = df_no_subtotals.drop(columns=['is_subtotal']).copy()
    ############################
    def calculate_subtotal_for_columns(melted_df, cols_to_sum):
        #gruop by the shared categories except the ones we are summing (in cols_to_sum) and sum the values. By doing this on each combination of the shared cateorgires, starting from the most specific ones, we can create subtotals for each combination of the shared categories.        
        group_cols = [col for col in melted_df.columns if col not in ['value'] and col not in cols_to_sum]
        group_cols_no_year = [col for col in group_cols if col != 'year']
        agg_df = melted_df.copy()
        
        #ignore where all of the cols in cols_to_sum are 'x'. This is where  the data is at its most detailed level already. We will ignore these rows and tehrefor not create a subtotal for them.
        agg_df = agg_df[~(agg_df[cols_to_sum] == 'x').all(axis=1)].copy()#but what this will do is create duplcates?i think this is the falt of mixing subfuels and subsectors as they are independt. need to trawt them separately
        
        #and ignore where only one, or less, values are non zero/nan for this group (group_cols), as this is also the most detailed level of data (or there is at least no data to sum up to a subtotal)and so shoudlnt have its values replaced with x'sand be summed up.
        agg_df_more_than_one_non_zero = melted_df.loc[(melted_df['value'] != 0) & (melted_df['value'].notnull())].copy()
        agg_df_more_than_one_non_zero = agg_df_more_than_one_non_zero.groupby(group_cols_no_year, as_index=False)['value'].count().reset_index().copy()
        agg_df_more_than_one_non_zero = agg_df_more_than_one_non_zero[agg_df_more_than_one_non_zero['value'] > 1].copy()
        agg_df = agg_df.merge(agg_df_more_than_one_non_zero[group_cols_no_year], on=group_cols_no_year, how='left', indicator=True)
        agg_df = agg_df[agg_df['_merge'] == 'both'].copy()
        agg_df.drop(columns=['_merge'], inplace=True)       
        
        # agg_df = agg_df[~(agg_df[cols_to_sum] == 'x').any(axis=1)].copy()#i think this is causeing us to drop allpossible rows to gruop by. resultuiing in no new subtotals being created. TODO: investigate this.
        agg_df = agg_df.groupby(group_cols, as_index=False)['value'].sum().copy()
        for omit_col in cols_to_sum:
            agg_df[omit_col] = 'x'
            
        #dont label as subttoal yet

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
    melted_df = df_no_subtotals.melt(id_vars=shared_categories,
                            value_vars=[col for col in df_no_subtotals.columns if str(col).isnumeric()],
                            var_name='year',
                            value_name='value')
    melted_df_with_subtotals = df.melt(id_vars=shared_categories+['is_subtotal'],
                            value_vars=[col for col in df.columns if str(col).isnumeric()],
                            var_name='year',
                            value_name='value')
    
    # #sum up vlaues so we can check for where there are more than one na/0 in the value col for the group_cols. 
    # melted_df = melted_df.groupby(shared_categories, as_index=False)['value'].sum().reset_index().copy()  
    #check for duplicates:
    duplicates = melted_df[melted_df.duplicated(subset=shared_categories+['year'], keep=False)]
    if duplicates.shape[0] > 0:
        duplicates.to_csv('data/temp/error_checking/duplicates_in_subtotaled_df.csv', index=False)
        print("WARNING: There are duplicates in the subtotaled DataFrame.")
        breakpoint()  
    
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
            duplicates.to_csv('data/temp/error_checking/duplicates_in_subtotals.csv', index=False)
            raise Exception("WARNING: There are nas in the subtotaled DataFrame.")#uplifted to exception as this is unexpected and should be investigated.
            # subtotalled_results[col] = subtotalled_results[col].fillna('x').copy()

    ############################
    
    #check for duplicates in these new subtotals based on all the cols except value.. if there are duplicates then perhaphs something went wrong with the subtotaling. But if the values are all 0 then just drop them.
    #Note: We are taking a risk of not catching potential errors if we assume we can drop subtotals that are duplciates and = 0...but if we dont do this it is quite complicated to work find the error anyway. It should only happen if there is not specific enough data in the layout file, so 0's aregetting subtotaled, and then being foudn asduplicates beside what is actually the most specific data for that group. So we will drop these by ideniftying if we are working on the layout file, otherwise throw an error.
    duplicates = subtotalled_results[subtotalled_results.duplicated(subset=[col for col in subtotalled_results.columns if col not in ['value']], keep=False)]
    if duplicates.shape[0] > 0:
        duplicates_non_zero = duplicates[duplicates['value'] != 0].copy()
        duplicates_non_zero = duplicates_non_zero[duplicates_non_zero.duplicated(subset=[col for col in duplicates_non_zero.columns if col not in ['value']], keep=False)]
        if duplicates_non_zero.shape[0] > 0 or DATAFRAME_ORIGIN != 'layout':
            #sort them
            duplicates.sort_values(by=[col for col in duplicates.columns if col not in ['value']], inplace=True)
            if duplicates.shape[0] > 0:
                print(duplicates)
                breakpoint()
                duplicates.to_csv('data/temp/error_checking/duplicates_in_subtotaled_dataframe.csv', index=False)
                raise Exception("There are duplicates in the subtotaled DataFrame.")
        else:
            #sort by value and keep the last duplicate always since it will not be 0.
            subtotalled_results = subtotalled_results.sort_values(by=['value'], ascending=True).drop_duplicates(subset=[col for col in subtotalled_results.columns if col not in ['value']], keep='last').copy()
    
    #now we've dropped all duplicates except those that might be caused by having different origins, we will sum up all values by all cols except origin. this will remove any duplicates that are caused by having different origins.
    if 'origin' in subtotalled_results.columns:
        subtotalled_results = subtotalled_results.groupby([col for col in subtotalled_results.columns if col not in ['value', 'origin']], as_index=False)['value'].sum().copy()
        shared_categories.remove('origin')
    ###################
    
    #now merge with the original df with subtotals. Fristly, we will idenitfy where subtotals dont calacualte to the same vlaue. If the input is a results df then assume its an error. If the input is a layout df then assume that what was originally labelled as a subtotal is in fact a value that wasnt able to be disaggregated. For these values, keep the original, dont calcualte the subtotal and relabel it as not a subtotal.
    EPSILON_PERCENT = 0.01
    merged_data = melted_df_with_subtotals.merge(subtotalled_results, on=shared_categories+['year'], how='outer', suffixes=('', '_new_subtotal'), indicator=True)
    #check where the values are different:
    merged_data['value_diff_pct'] = abs((merged_data['value'] - merged_data['value_new_subtotal']) / merged_data['value'])
    subtotals_with_different_values = merged_data[(merged_data['_merge'] == 'both') & (merged_data['value_diff_pct'] > EPSILON_PERCENT) & (merged_data['is_subtotal'] == True)].copy()
    
    ###add some exceptions weve already dealt with:
    #drop any 14_industry_sector in sectors if the DATAFRAME_ORIGIN is results:
    if DATAFRAME_ORIGIN == 'results':
        subtotals_with_different_values = subtotals_with_different_values[subtotals_with_different_values['sectors'] != '14_industry_sector'].copy() 
    #drop buildings electricity in fuels if the DATAFRAME_ORIGIN is results:
    if DATAFRAME_ORIGIN == 'results':
        subtotals_with_different_values = subtotals_with_different_values[(subtotals_with_different_values['sub1sectors'] != '16_01_buildings') & (subtotals_with_different_values['fuels'] != '17_electricity')].copy()
    ###
    if subtotals_with_different_values.shape[0] > 0 and DATAFRAME_ORIGIN == 'results':
        breakpoint()
        #save the data
        subtotals_with_different_values.to_csv(f'data/temp/error_checking/subtotals_with_different_values.csv', index=False)
        raise Exception('{} subtotals have different values in the original and subtotalled data. This is likely a mistake on the side of the modeller'.format(subtotals_with_different_values.shape[0]))
    elif subtotals_with_different_values.shape[0] > 0 and DATAFRAME_ORIGIN == 'layout':                
        #relable the subtotals as not subtotals
        merged_data.loc[(merged_data['_merge'] == 'both') & (merged_data['value_diff_pct'] > EPSILON_PERCENT) & (merged_data['is_subtotal'] == True), 'is_subtotal'] = False
        #but note that this could create rows where there are two rows with the same shared_categories but not year, but one is a subtotal and one is not. so we will need to drop the isntances where is_subtotal = True so taht we dont get duplicates. We will do this later in this function when we pivot the data wide.
    
    # # Then drop where there are any other subtotals that match rows in the origianl data, but they arent subttotals in the origianl data. Since we removed all labelled subtotals at the start of the funciton we must assume that these matching rows are actual data points for their cateogires, and thereofre shouldnt be replaced with a subtotal! So we will keep the original data and remove the subtotalled data.
    # merged_data = melted_df.merge(subtotalled_results, on=shared_categories+['year'], how='outer', suffixes=('', '_new_subtotal'), indicator=True)
    
    values_to_keep_in_original = merged_data[(merged_data['_merge'] == 'both') & (merged_data['is_subtotal'] == False)].copy()
    values_only_in_original = merged_data[(merged_data['_merge'] == 'left_only')].copy()
    new_subtotalled_values = merged_data[(merged_data['_merge'] == 'right_only')].copy()
    subtotal_values_in_both= merged_data[(merged_data['_merge'] == 'both') & (merged_data['is_subtotal'] == True)].copy()
    
    values_to_keep_in_original['value'] = values_to_keep_in_original['value']
    values_only_in_original['value'] = values_only_in_original['value']
    new_subtotalled_values['value'] = new_subtotalled_values['value_new_subtotal']
    subtotal_values_in_both['value'] = subtotal_values_in_both['value']
    
    new_subtotalled_values['is_subtotal'] = True
    
    #concat all together
    final_df = pd.concat([values_to_keep_in_original, values_only_in_original, new_subtotalled_values, subtotal_values_in_both], ignore_index=True)
    #drop merge and value_original and value_subtotalled
    final_df.drop(columns=['_merge', 'value_new_subtotal', 'value_diff_pct'], inplace=True)
    
    #TESTING FIND THESE sectors	sub1sectors	sub2sectors	sub3sectors	sub4sectors	fuels	subfuels
    # 14_industry_sector	x	x	x	x	01_coal	x
    # 14_industry_sector	x	x	x	x	01_coal	x
    test = final_df[(final_df['sectors'] == '14_industry_sector') & (final_df['sub1sectors'] == 'x') & (final_df['sub2sectors'] == 'x') & (final_df['sub3sectors'] == 'x') & (final_df['sub4sectors'] == 'x') & (final_df['fuels'] == '08_gas') & (final_df['subfuels'] == 'x') & (final_df['year'].isin([EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR]))].copy()
    # if test.shape[0] > 0:
        
    #     breakpoint()
    ###################
    #make final_df wide
    final_df_wide = final_df.pivot(index=shared_categories+['is_subtotal'], columns='year', values='value').reset_index()
    ###################
    try:
        #where two rows exist for the same shared_categories but one is a subtotal and one is not, drop the one that is a subtotal.
        same_rows = final_df_wide[final_df_wide.duplicated(subset=shared_categories, keep=False)]
        #find where there are two rows and one is a subtotasl, the other is not:
        to_drop = final_df_wide.loc[same_rows.index.tolist()]
        to_drop = to_drop[to_drop['is_subtotal'] == True].copy()
        to_keep = final_df_wide.loc[same_rows.index.tolist()]
        to_keep = to_keep[to_keep['is_subtotal'] == False].copy()
        #join them on shared_categories and keep only the ones where is_subtotal = False and True. Then drop these rows from final_df_wide
        to_drop = to_drop.merge(to_keep, on=shared_categories, how='left', suffixes=('', '_to_keep'), indicator=True)
        to_drop = to_drop[to_drop['_merge'] == 'both'].copy()
        to_drop = to_drop[shared_categories + ['is_subtotal']].copy()
        final_df_wide = final_df_wide.merge(to_drop, on=shared_categories+['is_subtotal'], how='left', indicator=True)
        final_df_wide = final_df_wide[final_df_wide['_merge'] == 'left_only'].copy()
        final_df_wide.drop(columns=['_merge'], inplace=True)
    except:
        breakpoint()
        print('WARNNG: There was an error when trying to drop subtotals that were duplicates of non-subtotals. This is unexpected and should be investigated. See line 299 in merging_functions.py')
    ###################
    
    # Check again for any duplicates
    duplicates = final_df_wide[final_df_wide.duplicated(subset=[col for col in final_df_wide.columns if col not in ['is_subtotal'] +[year for year in range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1)]], keep=False)]
    if duplicates.shape[0] > 0:
        # print("WARNING: There final_df_wideare duplicates in the subtotaled DataFrame.")
        # print(duplicates)
        breakpoint()
        duplicates.to_csv('data/temp/error_checking/duplicates_in_subtotaled_DF.csv', index=False)
        raise Exception("There are duplicates in the subtotaled DataFrame.") 
    return final_df_wide

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
        
def filter_out_solar_with_zeros_in_buildings_file(results_df):
    """This is a temporary fix to remove solar data that contains only 0s from the buildings file, as it creates duplicates which arent actually duplicates in calculate_subtotals.
    
    Specificlaly the issue is 0's where fuels=12_solar, sub1sectors =16_01_buildings , sub2sectors=x, subfuels=x """
    years = [str(col) for col in results_df.columns if any(str(year) in col for year in range(OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR+1))]
    #filter out solar data from buildings file:
    results_df = results_df[~((results_df['fuels']=='12_solar') & (results_df['sub1sectors']=='16_01_buildings')&(results_df['sub2sectors']=='x')&(results_df['subfuels']=='x')&(results_df[years]==0).all(axis=1))].copy()
    #OK THERE IS ALSO A PROBLEM WHERE SUBFUELS = 12_x_other_solar... SO DO THE SAME FOR THAT TOO...
    results_df = results_df[~((results_df['subfuels']=='12_x_other_solar') & (results_df['sub1sectors']=='16_01_buildings')&(results_df['sub2sectors']=='x')&(results_df['fuels']=='12_solar')&(results_df[years]==0).all(axis=1))].copy()
    #ugh ok and also, if they are nas, then we should also drop them...
    results_df = results_df[~((results_df['subfuels']=='12_x_other_solar') & (results_df['sub1sectors']=='16_01_buildings')&(results_df['sub2sectors']=='x')&(results_df['fuels']=='12_solar')&(results_df[years].isna().all(axis=1)))].copy()
    results_df = results_df[~((results_df['fuels']=='12_solar') & (results_df['sub1sectors']=='16_01_buildings')&(results_df['sub2sectors']=='x')&(results_df['subfuels']=='x')&(results_df[years].isna().all(axis=1)))].copy()
    return results_df   
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
    """Trim the layout DataFrame before merging with the results DataFrame to cut down on time. This especially involves removing sectors that are not in the results DataFrame. the missing sectors are returned in a DataFrame so the user can see what is missing, and they will be concatted on after the merge.

    Args:
        layout_df (_type_): _description_
        concatted_results_df (_type_): _description_

    Returns:
        _type_: _description_
    """
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

def format_merged_layout_results_df(merged_df, shared_categories, trimmed_layout_df, trimmed_concatted_results_df, missing_sectors_df):
    #we may have found that during the merge, some rows  subtotals and some arent
    merged_df['subtotal_layout'] = merged_df['subtotal_layout'].fillna(False)
    merged_df['subtotal_results'] = merged_df['subtotal_results'].fillna(False)
    missing_sectors_df['subtotal_results'] = False
    
    #Check for duplicate rows in concatted_results_df. if there are something is wrong as
    duplicates = merged_df[merged_df.duplicated(subset=shared_categories, keep=False)]
    if duplicates.shape[0] > 0:
        breakpoint()
        raise Exception("Duplicate rows found in concatted_results_df. Check the results files.")

    # Check if there were any unexpected or extra rows in concatted_results_df
    unexpected_rows = merged_df[merged_df['_merge'] != 'both']
    if unexpected_rows.shape[0] > 0:
        missing_from_results_df = unexpected_rows[unexpected_rows['_merge'] == 'left_only']
        #find total value of data in OUTLOOK_BASE_YEAR. this shows how much worth of data might be missing from the results file.
        missing_from_results_df_value = missing_from_results_df[OUTLOOK_BASE_YEAR].sum()
        extra_in_results_df = unexpected_rows[unexpected_rows['_merge'] == 'right_only']
        #find total value of data in OUTLOOK_BASE_YEAR+1
        extra_in_results_df_value = extra_in_results_df[OUTLOOK_BASE_YEAR+1].sum()
        
        print(f"Unexpected rows found in concatted_results_df. Check the results files.\nMissing from results_df: {missing_from_results_df.shape[0]}, with value {missing_from_results_df_value}\nExtra in results_df: {extra_in_results_df.shape[0]}, with value {extra_in_results_df_value}")#TODO IS THIS GOING TO BE TOO STRICT? WILL IT INCLUDE FUEL TYPES THAT SHOULD BE MISSING EG. CRUDE IN TRANSPORT, or even where there is no subttotals to create in the layout file since it is not specific enough (eg breaking domestic air into freight/passenger in transport sector)
        # breakpoint()
        # raise Exception("Unexpected rows found in concatted_results_df. Check the results files.")
    
    # Print the number of rows in both dataframes
    print("Number of rows in new_layout_df:", trimmed_layout_df.shape[0])
    print("Number of rows in merged_results_df:", trimmed_concatted_results_df.shape[0])
    print("Number of rows in merged_df:", merged_df.shape[0])

    #drop the _merge column
    merged_df.drop(columns=['_merge'], inplace=True)
    
    # Combine the remainign rows form the original layout_df with the merged_df
    results_layout_df = pd.concat([missing_sectors_df, merged_df])
    return results_layout_df

def calculate_sector_aggregates(df, sectors, aggregate_sector, shared_categories, shared_categories_w_subtotals):
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
    # Create lists of year columns for each condition using floats
    layout_years = [float(year) for year in range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR + 1)]
    results_years = [float(year) for year in range(OUTLOOK_BASE_YEAR + 1, OUTLOOK_LAST_YEAR + 1)]

    # Split the DataFrame into two based on 'subtotal_layout' and 'subtotal_results'
    df_filtered_layout = df[(df['subtotal_layout'] == False) & (df['sectors'].isin(sectors))].copy()
    df_filtered_results = df[(df['subtotal_results'] == False) & (df['sectors'].isin(sectors))].copy()

    # Drop the years not relevant for each DataFrame
    df_filtered_layout = df_filtered_layout[shared_categories_w_subtotals + layout_years]
    df_filtered_results = df_filtered_results[shared_categories_w_subtotals + results_years]

    # Define key columns for grouping
    key_cols = ['scenarios', 'economy', 'fuels', 'subfuels']

    # Initialize an empty DataFrame to store aggregated results
    aggregated_dfs = []

    # Iterate over the two filtered DataFrames
    for df_filtered in [df_filtered_layout, df_filtered_results]:
        # Assigning names to the DataFrames for identification
        df_filtered.name = 'layout' if df_filtered is df_filtered_layout else 'results'

        # If calculating total primary energy supply, make all TFC components negative first, then add them to the other components.
        if aggregate_sector == '07_total_primary_energy_supply':
            #TPES needs to be calauclate as ??? (TODO write the process for calcualting tpes so the context for below is explained)
            
            # Calculate TPES bottom up
            # Filter df to only include the demand sectors
            demand_sectors_df = df_filtered[df_filtered['sectors'].isin(['14_industry_sector', '15_transport_sector', '16_other_sector', '17_nonenergy_use'])].copy()
            tfc_df = demand_sectors_df.groupby(key_cols).sum(numeric_only=True).reset_index()
            # Multiplies all numeric columns by -1 to turn the values into negative
            numeric_cols = tfc_df.select_dtypes(include=[np.number]).columns
            tfc_df[numeric_cols] = tfc_df[numeric_cols] * -1
            # Filter df to only include the transformation sector
            transformation_sector_df = df_filtered[df_filtered['sectors'].isin(['09_total_transformation_sector', '10_losses_and_own_use', '11_statistical_discrepancy'])].copy()
            # Concatenating the two DataFrames
            tpes_df = pd.concat([transformation_sector_df, tfc_df], ignore_index=True)
            tpes_bottom_up_df = tpes_df.groupby(key_cols).sum(numeric_only=True).reset_index()
            # Multiplying all numeric columns by -1 to flip the values
            numeric_cols = tpes_bottom_up_df.select_dtypes(include=[np.number]).columns
            tpes_bottom_up_df[numeric_cols] = tpes_bottom_up_df[numeric_cols] * -1
            
            # tpes_bottom_up_df.to_csv('data/temp/error_checking/tpes_bottom_up_' + df_filtered.name + '.csv', index=False)
            
            # Calculate TPES top down
            # Filter df to only include the supply sectors
            supply_sectors_df = df_filtered[df_filtered['sectors'].isin(['01_production','02_imports', '03_exports', '06_stock_changes', '04_international_marine_bunkers','05_international_aviation_bunkers'])].copy()
            # Define TFC sectors and negate their values
            negative_sectors = ['03_exports', '04_international_marine_bunkers','05_international_aviation_bunkers' ]
            # Make the values in all numeric columns where the sector is a negative sector, negative
            negative_df = supply_sectors_df[supply_sectors_df['sectors'].isin(negative_sectors)].copy()
            numeric_cols = negative_df.select_dtypes(include=[np.number]).columns
            negative_df[numeric_cols] *= -1
            supply_df = pd.concat([supply_sectors_df[~supply_sectors_df['sectors'].isin(negative_sectors)], negative_df], ignore_index=True).copy()
            
            tpes_top_down_df = supply_df.groupby(key_cols).sum(numeric_only=True).reset_index()
            
            # Identify numeric columns for comparison
            numeric_cols = tpes_top_down_df.select_dtypes(include=[np.number]).columns

            # Initialize a dataframe to store differences
            differences = pd.DataFrame()

            # Iterate over each row in the first dataframe
            for idx, row in tpes_top_down_df.iterrows():
                # Find the corresponding row in the second dataframe
                corresponding_row = tpes_bottom_up_df[(tpes_bottom_up_df[key_cols] == row[key_cols]).all(axis=1)]

                if not corresponding_row.empty:
                    # Compare numeric columns within tolerance
                    for col in numeric_cols:
                        value1 = row[col]
                        value2 = corresponding_row[col].values[0]
                        
                        # Check if both values are not NaN or zero, and if difference exceeds tolerance
                        if not (np.isnan(value1) or np.isnan(value2) or value1 == 0 or value2 == 0):
                            if np.abs(value1 - value2) > 100000:
                                # Save the differing rows
                                differences = differences.append(row)
                                differences = differences.append(corresponding_row)

            # Remove duplicates if any
            differences = differences.drop_duplicates()

            # Check and save the differences
            if not differences.empty:
                differences.to_csv('data/temp/error_checking/tpes_differences_' + df_filtered.name + '.csv', index=False)
                raise Exception(f"Differences found in TPES calculation with {df_filtered.name} DataFrame and saved to 'tpes_differences_{df_filtered.name}.csv'.")
            else:
                print(f"No significant differences found in TPES calculation with {df_filtered.name} DataFrame.")
                aggregated_df = tpes_bottom_up_df.copy()
            
        elif aggregate_sector in ['13_total_final_energy_consumption', '12_total_final_consumption']:#these also need to ahve values calcualted for fuel subtotals, like TPES
            # If not calculating total primary energy supply, just perform the grouping and sum
            aggregated_df = df_filtered.groupby(key_cols).sum(numeric_only=True).reset_index().copy()
            
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
            
        #check fr duplicates
        dupes = aggregated_df[aggregated_df.duplicated(subset=shared_categories, keep=False)]
        if dupes.shape[0] > 0:
            print(dupes)
            breakpoint()
            raise Exception("Duplicates found in aggregated_df. Check the results files.")

        #where subtotal_layout or subtotal_results is 1 or 0 replace with True or False #TODO CLEAN THIS UP. SHOULDNT HAPPEN IN FIRST PLACE
        aggregated_df['subtotal_layout'] = aggregated_df['subtotal_layout'].replace({0:False, 1:True})
        aggregated_df['subtotal_results'] = aggregated_df['subtotal_results'].replace({0:False, 1:True})
        
        aggregated_dfs.append(aggregated_df)
    
    # Concatenate the processed DataFrames
    final_aggregated_df = pd.merge(aggregated_dfs[0], aggregated_dfs[1], on=shared_categories_w_subtotals, how='outer')
    
    return final_aggregated_df

def calculating_subtotals_in_sector_aggregates(df, shared_categories_w_subtotals):
    excluded_cols = ['subfuels']
    
    # Generate year columns as floats
    year_columns = [float(year) for year in range(EBT_EARLIEST_YEAR, OUTLOOK_LAST_YEAR + 1)]
    
    # Remove 'subfuels' from the shared categories list
    group_columns = [cat for cat in shared_categories_w_subtotals if cat not in excluded_cols]
    
    # Reset index if the DataFrame has an unnamed index column
    if df.index.name is None and not isinstance(df.index, pd.RangeIndex):
        df = df.reset_index(drop=True)
    
    # Check for missing columns
    missing_columns = [col for col in group_columns + year_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Not all specified columns are in the DataFrame. Missing columns: {missing_columns}")

    # Aggregate the data
    aggregated_df = df.groupby(group_columns).sum(numeric_only=True).reset_index().copy()

    # Add missing columns with 'x'
    for col in shared_categories_w_subtotals:
        if col not in aggregated_df.columns.to_list() + ['subtotal_layout', 'subtotal_results']:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="DataFrame is highly fragmented")
                aggregated_df[col] = 'x'

    # Reorder columns to match shared_categories order
    ordered_columns = shared_categories_w_subtotals + [col for col in aggregated_df.columns if col not in shared_categories_w_subtotals]
    aggregated_df = aggregated_df[ordered_columns]

    # Drop rows in df where 'subfuels' is exactly 'x'
    df = df[df['subfuels'] != 'x']

    # Concatenate df and aggregated_df
    final_df = pd.concat([df, aggregated_df])
    
    # Drop the 'subtotal_layout' and 'subtotal_results' columns to relabel them
    final_df = final_df.drop(['subtotal_layout', 'subtotal_results'], axis=1).copy()

    return final_df

def create_final_energy_df(sector_aggregates_df, fuel_aggregates_df,results_layout_df, shared_categories):
    final_df = pd.concat([sector_aggregates_df, fuel_aggregates_df, results_layout_df], ignore_index=True)

    #replace any nas in years cols with 0
    final_df.loc[:,[year for year in range(EBT_EARLIEST_YEAR+1, OUTLOOK_LAST_YEAR+1)]] = final_df.loc[:,[year for year in range(EBT_EARLIEST_YEAR+1, OUTLOOK_LAST_YEAR+1)]].fillna(0)
    #ceck for duplicates
    final_df_dupes = final_df[final_df.duplicated(subset=shared_categories, keep=False)]
    if final_df_dupes.shape[0] > 0:
        print(final_df_dupes)
        breakpoint()
        raise Exception("Duplicates found in final_df. Check the results files.")
    
    return final_df
    
    
def calculate_fuel_aggregates(new_aggregates_df, results_layout_df, shared_categories):
    """19_total is used to sum up all energy for all fuels in any sector. THis is a useful aggregate to basically check the sum of all eneryg in each sector/subsector
    
    21_modern_renewables is used to sum up all modern renewables in any sector. This is a useful aggregate to check the sum of all modern renewables in each sector/subsector. Modern renewables are any renewables that arent biomass consumed in the buildings and ag sectors.
    
    20_total_renewables is used to sum up all renewables in any sector. This is a useful aggregate to check the sum of all renewables in each sector/subsector

    Args:
        df (_type_): _description_
        shared_categories (_type_): _description_

    Raises:
        Exception: _description_

    Returns:
        _type_: _description_
    """
    #TODO ADD IN TOTAL RENEWABLES AND MODERN RENEWABLES. THEY FOLLOW SAME PATTERN (I.E CALCUALTED FOR EVERY GROUP EXCEPT FUELS, AND THEN ADDED TO THE DF)

    # # Concatenate new_aggregates_df and the original df (with no aggregates)
    df = pd.concat([new_aggregates_df, results_layout_df], ignore_index=True)
    
    # Melt the DataFrame
    df_melted = df.melt(id_vars=shared_categories + ['subtotal_layout', 'subtotal_results'], value_vars=[col for col in df.columns if col in [year for year in range(EBT_EARLIEST_YEAR, OUTLOOK_LAST_YEAR+1)]],var_name='year', value_name='value')

    # Convert year column to integer
    df_melted['year'] = df_melted['year'].astype(int)

    # drop 08_02_interproduct_transfers, 09_12_nonspecified_transformation as we dont want to include it in the aggregates
    df_melted = df_melted[~df_melted['sub1sectors'].isin(['09_12_nonspecified_transformation', '08_02_interproduct_transfers'])].copy()

    # Split the melted DataFrame into two based on the year ranges
    df_layout = df_melted[(df_melted['year'].between(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR)) & (df_melted['subtotal_layout'] == False)].copy()
    df_results = df_melted[(df_melted['year'].between(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR)) & (df_melted['subtotal_results'] == False)].copy()

    def process_df_19_total(split_df):
        fuel_aggregates_list = []
        for cols_to_exclude in exclusion_sets:
            excluded_cols = ['fuels', 'subfuels'] + cols_to_exclude
            group_columns = [cat for cat in shared_categories if cat not in excluded_cols] + ['year']

            # Aggregate '19_total'
            total_19 = split_df.groupby(group_columns)['value'].sum().reset_index()
            total_19['fuels'] = '19_total'
            for col in excluded_cols[1:]:
                total_19[col] = 'x'

            fuel_aggregates_list.append(total_19)

        fuel_aggregates = pd.concat(fuel_aggregates_list, ignore_index=True)
        
        # Remove exact duplicates, keeping the first occurrence
        fuel_aggregates = fuel_aggregates.drop_duplicates()

        # Remove rows where 'value' is 0
        fuel_aggregates = fuel_aggregates[fuel_aggregates['value'] != 0]
        
        return fuel_aggregates
    
    def process_df_20_total_renewables(split_df):
        total_renewables_df = split_df[split_df['fuels'].isin(['10_hydro', '11_geothermal', '12_solar', '13_tide_wave_ocean', '14_wind', '15_solid_biomass', '16_others'])].copy()
        total_renewables_df = total_renewables_df[~total_renewables_df['subfuels'].isin(['16_02_industrial_waste', '16_04_municipal_solid_waste_nonrenewable', '16_09_other_sources', '16_x_hydrogen', '16_x_ammonia'])].copy()
        
        fuel_aggregates_list = []
        for cols_to_exclude in exclusion_sets:
            excluded_cols = ['fuels', 'subfuels'] + cols_to_exclude
            group_columns = [cat for cat in shared_categories if cat not in excluded_cols] + ['year']

            # Aggregate '20_total_renewables'
            renewables_20 = total_renewables_df.groupby(group_columns)['value'].sum().reset_index()
            renewables_20['fuels'] = '20_total_renewables'
            for col in excluded_cols[1:]:
                renewables_20[col] = 'x'

            fuel_aggregates_list.append(renewables_20)

        fuel_aggregates = pd.concat(fuel_aggregates_list, ignore_index=True)
        
        # Remove exact duplicates, keeping the first occurrence
        fuel_aggregates = fuel_aggregates.drop_duplicates()

        # Remove rows where 'value' is 0
        fuel_aggregates = fuel_aggregates[fuel_aggregates['value'] != 0]
        
        return fuel_aggregates
    
    def process_df_21_modern_renewables(split_df, df_19_total, df_20_total_renewables):
        # Filter out '18_electricity_output_in_gwh' and '19_heat_output_in_pj' from df_19_total and df_20_total_renewables for renewable shares calculation
        df_19_total_filtered = df_19_total[(df_19_total['sectors'] == '18_electricity_output_in_gwh') & (df_19_total['sub1sectors'] == 'x') | (df_19_total['sectors'] == '19_heat_output_in_pj') & (df_19_total['sub1sectors'] == 'x')].copy()
        df_20_total_renewables_filtered = df_20_total_renewables[(df_20_total_renewables['sectors'] == '18_electricity_output_in_gwh') & (df_20_total_renewables['sub1sectors'] == 'x') | (df_20_total_renewables['sectors'] == '19_heat_output_in_pj') & (df_20_total_renewables['sub1sectors'] == 'x')].copy()

        # Filter out split_df to include all possible renewables
        modern_renewables_df = split_df[split_df['fuels'].isin(['10_hydro', '11_geothermal', '12_solar', '13_tide_wave_ocean', '14_wind', '15_solid_biomass', '16_others', '17_electricity', '18_heat'])].copy()
        modern_renewables_df = modern_renewables_df[~modern_renewables_df['subfuels'].isin(['16_02_industrial_waste', '16_04_municipal_solid_waste_nonrenewable', '16_09_other_sources', '16_x_hydrogen', '16_x_ammonia'])].copy()
        # Filter out biomass consumed in buildings and agriculture
        modern_renewables_df = modern_renewables_df[~((modern_renewables_df['fuels'] == '15_solid_biomass') & (modern_renewables_df['sectors'] == '16_other_sector'))].copy()
        
        # Filter out some subsectors
        modern_renewables_df = modern_renewables_df[~((modern_renewables_df['sub1sectors'] == '09_04_electric_boilers') | (modern_renewables_df['sub2sectors'] == '10_01_02_gas_works_plants'))].copy()
        
        # Filter out '12_total_final_consumption' and '13_total_final_energy_consumption' to exclude biomass consumed in buildings and agriculture
        # modern_renewables_df = modern_renewables_df[~((modern_renewables_df['sectors'] == '12_total_final_consumption') | (modern_renewables_df['sectors'] == '13_total_final_energy_consumption'))].copy()

        # Calculate the shares
        shares_df = df_20_total_renewables_filtered[['year', 'scenarios', 'sectors', 'value']].merge(df_19_total_filtered[['year', 'scenarios', 'sectors', 'value']], on=['year', 'scenarios', 'sectors'], suffixes=('_20', '_19'))
        shares_df['share'] = shares_df['value_20'] / shares_df['value_19']
        # Add a 'fuels' column based on 'sectors' to tell if the share is for electricity or heat and be able to adjust electricity and heat values easily
        shares_df['fuels'] = shares_df['sectors'].apply(lambda x: '17_electricity' if x == '18_electricity_output_in_gwh' else ('18_heat' if x == '19_heat_output_in_pj' else None))
        shares_df.drop(columns=['sectors'], inplace=True)
        
        # Merge the share into modern_renewables_df for adjustment
        modern_renewables_df = modern_renewables_df.merge(shares_df[['year', 'scenarios', 'fuels', 'share']], on=['year', 'scenarios', 'fuels'], how='left')
        
        # Adjust values for '17_electricity' and '18_heat'
        modern_renewables_df.loc[modern_renewables_df['fuels'].isin(['17_electricity', '18_heat']), 'value'] *= modern_renewables_df['share']
        
        # Drop the 'share' column before continuing to the aggregation part
        modern_renewables_df.drop(columns=['share'], inplace=True)

        fuel_aggregates_list = []
        for cols_to_exclude in exclusion_sets:
            excluded_cols = ['fuels', 'subfuels'] + cols_to_exclude
            group_columns = [cat for cat in shared_categories if cat not in excluded_cols] + ['year']

            # Aggregate '21_modern_renewables'
            renewables_21 = modern_renewables_df.groupby(group_columns)['value'].sum().reset_index()
            renewables_21['fuels'] = '21_modern_renewables'
            for col in excluded_cols[1:]:
                renewables_21[col] = 'x'

            fuel_aggregates_list.append(renewables_21)

        fuel_aggregates = pd.concat(fuel_aggregates_list, ignore_index=True)
        
        # Remove exact duplicates, keeping the first occurrence
        fuel_aggregates = fuel_aggregates.drop_duplicates()

        # Remove rows where 'value' is 0
        fuel_aggregates = fuel_aggregates[fuel_aggregates['value'] != 0]
        
        return fuel_aggregates
    
    # Define sets of columns to exclude in each iteration
    exclusion_sets = [
        ['sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors'],
        ['sub2sectors', 'sub3sectors', 'sub4sectors'],
        ['sub3sectors', 'sub4sectors'],
        ['sub4sectors'],
        []
    ]
    
    def replace_values_of_tfc_and_tfec(input_df):
        # This function replaces the tfc and tfec values with the sum of the modern renewables of the sectors to exclude biomass in buildings and agriculture in modern renewables

        # Identify the rows for summing up the 'value'
        sum_df = input_df[input_df['sectors'].isin(['14_industry_sector', '15_transport_sector', '16_other_sector'])]
        sum_df = sum_df.groupby(['year', 'scenarios', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors'], as_index=False)['value'].sum().copy()

        # Define a function to apply the sum to the 'value' column where sectors are '12_total_final_consumption' or '13_total_final_energy_consumption'
        def apply_sum(row, sum_df):
            if row['sectors'] in ['12_total_final_consumption', '13_total_final_energy_consumption']:
                sum_row = sum_df[(sum_df['year'] == row['year']) & (sum_df['scenarios'] == row['scenarios']) & (sum_df['sub1sectors'] == row['sub1sectors']) & (sum_df['sub2sectors'] == row['sub2sectors']) & (sum_df['sub3sectors'] == row['sub3sectors']) & (sum_df['sub4sectors'] == row['sub4sectors'])]
                if not sum_row.empty:
                    return sum_row['value'].values[0]
            return row['value']
        
        # Apply the function to the 'value' column
        input_df['value'] = input_df.apply(apply_sum, axis=1, sum_df=sum_df)

        return input_df
    
    # Process each split DataFrame and concatenate
    fuel_aggregates_layout_19 = process_df_19_total(df_layout)
    fuel_aggregates_results_19 = process_df_19_total(df_results)
    fuel_aggregates_layout_20 = process_df_20_total_renewables(df_layout)
    fuel_aggregates_results_20 = process_df_20_total_renewables(df_results)
    
    fuel_aggregates_layout_21 = process_df_21_modern_renewables(df_layout, fuel_aggregates_layout_19, fuel_aggregates_layout_20)
    fuel_aggregates_layout_21_modified = replace_values_of_tfc_and_tfec(fuel_aggregates_layout_21)
    fuel_aggregates_results_21 = process_df_21_modern_renewables(df_results, fuel_aggregates_results_19, fuel_aggregates_results_20)
    fuel_aggregates_results_21_modified = replace_values_of_tfc_and_tfec(fuel_aggregates_results_21)
    
    fuel_aggregates_df = pd.concat([fuel_aggregates_layout_19, fuel_aggregates_results_19,
                                    fuel_aggregates_layout_20, fuel_aggregates_results_20,
                                    fuel_aggregates_layout_21_modified, fuel_aggregates_results_21_modified], ignore_index=True)

    # Pivot the aggregated DataFrame
    fuel_aggregates_pivoted = fuel_aggregates_df.pivot_table(index=shared_categories, columns='year', values='value').reset_index()

    # # Change columns to str and reorder
    # pivoted_df.columns = pivoted_df.columns.astype(str)
    # pivoted_columns_order = shared_categories + [str(year) for year in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1)]
    # fuel_aggregates_pivoted = pivoted_df[pivoted_columns_order]

    # # Merge pivoted data back into the original layout
    # fuel_aggregates_pivoted = results_layout_df.merge(pivoted_df, on=shared_categories, how='left')
    # layout_columns_order = shared_categories + [str(year) for year in range(EBT_EARLIEST_YEAR, OUTLOOK_LAST_YEAR+1)]
    # fuel_aggregates_pivoted = fuel_aggregates_pivoted[layout_columns_order]

        
    ####
    #20_total_renewables
    # fuels:
    # '01_coal', '02_coal_products', '03_peat', '04_peat_products',
    #    '05_oil_shale_and_oil_sands', '06_crude_oil_and_ngl',
    #    '07_petroleum_products', '08_gas', '09_nuclear', '10_hydro',
    #    '11_geothermal', '12_solar', '13_tide_wave_ocean', '14_wind',
    #    '15_solid_biomass', '16_others', '17_electricity', '18_heat'
    
    #subfuels:
    #array(['01_01_coking_coal', '01_05_lignite', '01_x_thermal_coal', 'x',
    #    '06_01_crude_oil', '06_02_natural_gas_liquids',
    #    '06_x_other_hydrocarbons', '07_01_motor_gasoline',
    #    '07_02_aviation_gasoline', '07_03_naphtha', '07_06_kerosene',
    #    '07_07_gas_diesel_oil', '07_08_fuel_oil', '07_09_lpg',
    #    '07_10_refinery_gas_not_liquefied', '07_11_ethane',
    #    '07_x_jet_fuel', '07_x_other_petroleum_products',
    #    '08_01_natural_gas', '08_02_lng', '08_03_gas_works_gas',
    #    '12_01_of_which_photovoltaics', '12_x_other_solar',
    #    '15_01_fuelwood_and_woodwaste', '15_02_bagasse', '15_03_charcoal',
    #    '15_04_black_liquor', '15_05_other_biomass', '16_01_biogas',
    #    '16_02_industrial_waste', '16_03_municipal_solid_waste_renewable',
    #    '16_04_municipal_solid_waste_nonrenewable', '16_05_biogasoline',
    #    '16_06_biodiesel', '16_07_bio_jet_kerosene',
    #    '16_08_other_liquid_biofuels', '16_09_other_sources',
    #    '16_x_ammonia', '16_x_efuel', '16_x_hydrogen'], dtype=object)
    
    # #the problem with renwables calcualtion is that we have to consider the renewables used in ouput of electricity and heat. this has to be done using the share of renewables in total genreation. this is so that renewables also has a share in losses and own use.  (edito said that). I dont know how to do this yet!
    # #TODO JNSUT CALCUALTE USING SAME METHOD AS 19 TOTAL, FOR NOW
    # total_renewables_20 = df_melted.groupby(group_columns)['value'].sum().reset_index()
    # total_renewables_20['fuels'] = '20_total_renewables'
    # for col in excluded_cols[1:]:  # We start from 1 as 'fuels' is already addressed above
    #     total_renewables_20[col] = 'x'
    # ####
    # #21_modern_renewables
    # #here, we do know that it is renewables that arent biomass consumed in the buildings and ag sectors. but we still dont know how to claulte the share of renewables in total generation. so for now we will just do the same as 19 total.
    # modern_renewables_21 = df_melted.groupby(group_columns)['value'].sum().reset_index()
    # modern_renewables_21['fuels'] = '21_modern_renewables'
    # for col in excluded_cols[1:]:  # We start from 1 as 'fuels' is already addressed above
    #     modern_renewables_21[col] = 'x'
        
    # ####    
    # fuel_aggregates = pd.concat([total_19, total_renewables_20, modern_renewables_21], ignore_index=True)
    ##############   
    
    # with warnings.catch_warnings():
    #     warnings.filterwarnings("ignore", message="DataFrame is highly fragmented")
    #     Pivot the dataframe back to its original format
    #     fuel_aggregates_pivoted = fuel_aggregates.pivot(index=[col for col in fuel_aggregates.columns if col not in ['year', 'value']], columns='year', values='value').reset_index()
        
    # dupes = fuel_aggregates_pivoted[fuel_aggregates_pivoted.duplicated(subset=shared_categories, keep=False)]
    # if dupes.shape[0] > 0:
    #     print(dupes)
    #     breakpoint()
    #     raise Exception("Duplicates found in fuel_aggregates_pivoted. Check the results files.")
    
    # set subtotals to true
    fuel_aggregates_pivoted['subtotal_layout'] = False
    fuel_aggregates_pivoted['subtotal_results'] = False
    
    return fuel_aggregates_pivoted

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

def check_for_issues_by_comparing_to_layout_df(results_layout_df, shared_categories, new_aggregate_sectors, layout_df_subtotals_labelled, REMOVE_LABELLED_SUBTOTALS=False):
    """Use this to check that the layout df and the newly processed layout df match for the years in the layout file. This should not happen, and if there are isues its likely some process in the merging_results script is wrong.
    The process will do two comparisons, one on the values and the next on whether any rows are missing. The one that checks values will
    
    REMOVE_LABELLED_SUBTOTALS:
    Because there are so many issues with both the input layout and input results dfs we are never going to be able to do this perfectly, but if we assume that we have labelled subttoals correctly then if we remove the subttoals from the layout df, we will have far fewer missing rows. if you are trying to track down specific bugs though, you might want to set REMOVE_LABELLED_SUBTOTALS to False"""
    #drop where there are aggregate sectors where subfuel != x as we have removed those.
    layout_df = layout_df_subtotals_labelled.copy()
    layout_df = layout_df[~((layout_df.sectors.isin(new_aggregate_sectors) & (layout_df.subfuels != 'x')))].copy()
    if REMOVE_LABELLED_SUBTOTALS:
        layout_df=layout_df[~layout_df.is_subtotal].copy()
        results_layout_df = results_layout_df[~results_layout_df.subtotal_layout].copy()
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
        merged_df_bad_values = merged_df_bad_values[merged_df_bad_values.duplicated(subset=shared_categories_old, keep=False)].copy()
        
        #replace left_only with new_layout_df and right_only with original_layout
        merge_dict = {'left_only': 'new_layout_df', 'right_only': 'original_layout'}
        merged_df_bad_values['_merge'] = merged_df_bad_values['_merge'].map(merge_dict).fillna(merged_df_bad_values['_merge'])
        
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
        
        ###############
        bad_values_rows_exceptions_dict = {}#IGNORE ANY AGGREGATE FUELS/SECTORS UNTIL THEYVE BEEN CALCAULTED PROPERLY
        bad_values_rows_exceptions_dict['19_total'] = {'fuels':'19_total'}
        bad_values_rows_exceptions_dict['20_total_renewables'] = {'fuels':'20_total_renewables'}
        bad_values_rows_exceptions_dict['21_modern_renewables'] = {'fuels':'21_modern_renewables'}
        bad_values_rows_exceptions_dict['13_total_final_energy_consumption'] = {'sectors':'13_total_final_energy_consumption'}
        bad_values_rows_exceptions_dict['12_total_final_consumption'] = {'sectors':'12_total_final_consumption'}
        bad_values_rows_exceptions_dict['07_total_primary_energy_supply'] = {'sectors':'07_total_primary_energy_supply'}

        # USA file has some issues with the following rows
        bad_values_rows_exceptions_dict['USA_14_industry_sector'] = {'economy':'20_USA', 'sectors':'14_industry_sector', 'sub1sectors':'x', 'fuels':'07_petroleum_products', 'subfuels':'x'}
        bad_values_rows_exceptions_dict['USA_10_losses_and_own_use'] = {'economy':'20_USA', 'sectors':'10_losses_and_own_use', 'fuels':'01_coal', 'subfuels':'x'}
        bad_values_rows_exceptions_dict['USA_11_statistical_discrepancy'] = {'economy':'20_USA', 'sectors':'11_statistical_discrepancy', 'sub1sectors':'x', 'fuels':'16_others', 'subfuels':'x'}
        
        # JPN file has some issues with the following rows
        bad_values_rows_exceptions_dict['JPN_09_total_transformation_sector'] = {'economy':'08_JPN', 'sectors':'09_total_transformation_sector', 'sub1sectors':'x', 'fuels':'12_solar', 'subfuels':'x'}
        bad_values_rows_exceptions_dict['JPN_11_statistical_discrepancy'] = {'economy':'08_JPN', 'sectors':'11_statistical_discrepancy', 'sub1sectors':'x', 'fuels':'16_others', 'subfuels':'x'}
        
        # CDA file has some issues with the following rows
        bad_values_rows_exceptions_dict['CDA_11_statistical_discrepancy'] = {'economy':'03_CDA', 'sectors':'11_statistical_discrepancy', 'sub1sectors':'x', 'fuels':'16_others', 'subfuels':'x'}
        
        # ROK file has some issues with the following rows
        bad_values_rows_exceptions_dict['ROK_11_statistical_discrepancy'] = {'economy':'09_ROK', 'sectors':'11_statistical_discrepancy', 'sub1sectors':'x', 'fuels':'16_others', 'subfuels':'x'}
        bad_values_rows_exceptions_dict['ROK_19_heat_output_in_pj'] = {'economy':'09_ROK', 'sectors':'19_heat_output_in_pj', 'sub1sectors':'x'}

        #CREATE ROWS TO IGNORE. THESE ARE ONES THAT WE KNOW CAUSE ISSUES BUT ARENT NECESSARY TO FIX, AT LEAST RIGHT NOW
        #use the keys as column names to remove the rows in the dict:
        for ignored_issue in bad_values_rows_exceptions_dict.keys():
            #iterate through the dict to thin down to the rows we want to remove and then remove them by index
            rows_to_remove = merged_df_bad_values.copy()
            for col in bad_values_rows_exceptions_dict[ignored_issue].keys():
                rows_to_remove = rows_to_remove[rows_to_remove[col] == bad_values_rows_exceptions_dict[ignored_issue][col]].copy()
            merged_df_bad_values = merged_df_bad_values.drop(rows_to_remove.index).copy()
        
        ###############
        #where we have unexpected rows, this is where either _merge is new_layout_df or original_layout in merged_df_on_categories_only. new_layout_df means the layout file has a row that isnt in the results file. original_layout means the results file has a row that isnt in the layout file.
        missing_rows = merged_df_on_categories_only[merged_df_on_categories_only['_merge'] != 'both'].copy()
        missing_rows['_merge'] = missing_rows['_merge'].map(merge_dict).fillna(missing_rows['_merge'])
        #that covers all possible issues. we will now save them and ask the user to fix them.
        if merged_df_bad_values.shape[0] > 0:
            #put them in order to help see the issue
            merged_df_bad_values.sort_values(by=shared_categories_old, inplace=True)
            merged_df_bad_values.to_csv('data/temp/error_checking/merged_df_bad_values.csv', index=False)
            print("There are {} rows where the values in the results file do not match the values in the layout file. These rows have been saved to data/temp/error_checking/merged_df_bad_values.csv".format(merged_df_bad_values.shape[0]))
            breakpoint()
        if missing_rows.shape[0] > 0:
            ###############
            missing_rows_exceptions_dict = {}
            #CREATE MISSING ROWS TO IGNORE. THESE ARE ONES THAT WE KNOW CAUSE ISSUES BUT ARENT NECESSARY TO FIX, AT LEAST RIGHT NOW
            missing_rows_exceptions_dict['nonspecified_transformation'] = {'_merge':'original_layout', 'sub1sectors':'09_12_nonspecified_transformation'}
            
            missing_rows_exceptions_dict['12_total_final_consumption'] = {'_merge':'new_layout_df', 'sectors':'12_total_final_consumption'}
            missing_rows_exceptions_dict['13_total_final_energy_consumption'] = {'_merge':'new_layout_df', 'sectors':'13_total_final_energy_consumption'}
            missing_rows_exceptions_dict['07_total_primary_energy_supply'] = {'sectors':'07_total_primary_energy_supply'}
            
            missing_rows_exceptions_dict['19_total'] = {'_merge':'original_layout', 'fuels':'19_total'}
            missing_rows_exceptions_dict['20_total_renewables'] = {'_merge':'original_layout', 'fuels':'20_total_renewables'}
            missing_rows_exceptions_dict['21_modern_renewables'] = {'_merge':'original_layout', 'fuels':'21_modern_renewables'}

            # USA file has some issues with the following rows (couldn't work out the cause)
            missing_rows_exceptions_dict['nonspecified_transformation2'] = {'_merge':'new_layout_df', 'economy':'20_USA', 'sub1sectors':'09_06_gas_processing_plants', 'fuels':'08_gas'}
            missing_rows_exceptions_dict['nonspecified_transformation3'] = {'_merge':'new_layout_df', 'economy':'20_USA', 'sub1sectors':'09_06_gas_processing_plants', 'fuels':'20_total_renewables'}
            missing_rows_exceptions_dict['nonspecified_transformation4'] = {'_merge':'new_layout_df', 'economy':'20_USA', 'sub1sectors':'09_06_gas_processing_plants', 'fuels':'21_modern_renewables'}
            missing_rows_exceptions_dict['10_losses_and_own_use'] = {'_merge':'new_layout_df', 'economy':'20_USA', 'sub1sectors':'10_01_own_use'}
            missing_rows_exceptions_dict['08_transfers'] = {'_merge':'new_layout_df', 'economy':'20_USA', 'sectors':'08_transfers'}
            # missing_rows_exceptions_dict['19_01_05_others'] = {'_merge':'new_layout_df', 'economy':'20_USA', 'sectors':'19_heat_output_in_pj', 'sub1sectors':'19_01_chp_plants', 'sub2sectors':'19_01_05_others'}

            # JPN file has some issues with the following rows
            missing_rows_exceptions_dict['19_heat_output_in_pj'] = {'_merge':'new_layout_df', 'economy':'08_JPN', 'sub1sectors':'19_02_heat_plants'}
            # missing_rows_exceptions_dict['10_losses_and_own_use'] = {'_merge':'new_layout_df', 'economy':'08_JPN', 'sectors':'10_losses_and_own_use', 'sub1sectors':'10_02_transmision_and_distribution_losses', 'sub2sectors':'x'}

            #use the keys as column names to remove the rows in the dict:
            # for ignored_issue in missing_rows_exceptions_dict.keys():
            #     #iterate through the dict to thin down to the rows we want to remove and then remove them by index
            #     rows_to_remove = missing_rows.copy()
            #     for col in missing_rows_exceptions_dict[ignored_issue].keys():
            #         rows_to_remove = rows_to_remove[rows_to_remove[col] == missing_rows_exceptions_dict[ignored_issue][col]].copy()
            #     missing_rows = missing_rows.drop(rows_to_remove.index).copy()
            
            for ignored_issue, criteria in missing_rows_exceptions_dict.items():
                query = ' & '.join([f"{col} == '{val}'" for col, val in criteria.items()])
                rows_to_remove = missing_rows.query(query).index
                missing_rows.drop(rows_to_remove, inplace=True)

            ###############
            if missing_rows.shape[0] > 0:
                #put them in order to help see the issue
                missing_rows.sort_values(by=shared_categories_old, inplace=True)
                #put the _merge col at front
                missing_rows = missing_rows[['_merge'] + missing_rows.columns[:-1].tolist()]
                missing_rows.to_csv('data/temp/error_checking/missing_rows.csv', index=False)
                print("There are {} rows where the results file is missing rows from the layout file. These rows have been saved to data/temp/error_checking/missing_rows.csv".format(missing_rows.shape[0]))
                breakpoint()
        if merged_df_bad_values.shape[0] > 0 or missing_rows.shape[0] > 0:
            #save the results_layout_df for user to check
            breakpoint()
            results_layout_df.to_csv('data/temp/error_checking/results_layout_df.csv', index=False)
            raise Exception("The layout df and the newly processes layout df do not match for the years in the layout file. This should not happen.")
        

def power_move_x_in_chp_and_hp_to_biomass(results_df):
    # Anything that has sub1sectors in 18_02_chp_plants, 09_02_chp_plants, 09_x_heat_plants and the sub2sectors col is 'x' should be moved to another sector in same level. we will state that in a dict below:
    corresp_sectors_dict = {}
    corresp_sectors_dict['18_02_chp_plants'] = '18_02_04_biomass'
    corresp_sectors_dict['09_02_chp_plants'] = '09_02_04_biomass'
    corresp_sectors_dict['09_x_heat_plants'] = '09_x_04_biomass'
    corresp_sectors_dict['19_01_chp_plants'] = '19_01_04_biomass'
    
    # List of sub2sectors values to check
    values_to_check = ['x', '19_01_05_others']
    
    for key, value in corresp_sectors_dict.items():
        # Get the rows where sub1sectors is the key and sub2sectors is one of the specified values
        rows_to_change = results_df.loc[(results_df['sub1sectors'] == key) & (results_df['sub2sectors'].isin(values_to_check))].copy()
        # Remove these rows from the original dataframe
        results_df = results_df.loc[~((results_df['sub1sectors'] == key) & (results_df['sub2sectors'].isin(values_to_check)))].copy()
        # Change the sub1sectors to the corresponding value
        rows_to_change['sub2sectors'] = value
        # Append the modified rows back to the results_df
        results_df = pd.concat([results_df, rows_to_change])
    return results_df

def process_sheet(sheet_name, excel_file, economy, OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR, mapping_dict):
    wb = load_workbook(filename=excel_file)
    sheet = wb[sheet_name]

    sheet_data = pd.DataFrame()

    for scenario in ['REF', 'TGT']:
        # Initialize variables for the scenario range
        scenario_start_row = None
        scenario_end_row = None

        # Iterate through merged cell ranges to find the scenario range
        for merged_range in sheet.merged_cells.ranges:
            for row in sheet.iter_rows(min_row=merged_range.min_row, max_row=merged_range.max_row, min_col=merged_range.min_col, max_col=merged_range.max_col):
                if row[0].value == scenario:
                    scenario_start_row = merged_range.min_row
                    scenario_end_row = merged_range.max_row
                    break
            if scenario_start_row:
                break

        if not scenario_start_row:
            raise Exception(f"'{scenario}' merged cell range not found in {sheet_name}.")

        # Locate 'Energy Demand (PJ)' cell within the scenario range
        energy_demand_cell = None
        for row in range(scenario_start_row, scenario_end_row + 1):
            for cell in sheet[row]:
                if cell.value == 'Energy Demand (PJ)':
                    energy_demand_cell = cell
                    break
            if energy_demand_cell:
                break

        if not energy_demand_cell:
            raise Exception(f"'Energy Demand (PJ)' cell not found for {scenario} in {sheet_name}.")

        # Calculate the range to read from the Excel file
        start_col = energy_demand_cell.column_letter
        start_row = energy_demand_cell.row

        # Calculate the ending column letter based on the number of years
        end_col_num = energy_demand_cell.column + 81  # 81 additional columns after the start column (1990-2070)
        end_col_letter = get_column_letter(end_col_num)

        # Read the data from the Excel file
        data_df = pd.read_excel(excel_file, sheet_name=sheet_name, header=start_row - 1, usecols=f"{start_col}:{end_col_letter}")

        # Drop rows after 'Total'
        energy_demand_header = data_df.columns[0]
        total_index = data_df.index[data_df[energy_demand_header] == 'Total'].tolist()
        if total_index:
            data_df = data_df.iloc[:total_index[0]].reset_index(drop=True)

        # Process the data
        transformed_data = pd.DataFrame()
        for index, row in data_df.iterrows():
            if row[energy_demand_header] == 'Total':
                continue

            mapped_values = mapping_dict.get(row[energy_demand_header], {'fuels': 'Unknown', 'subfuels': 'Unknown'})
            new_row = {
                'scenarios': 'reference' if scenario == 'REF' else 'target',
                'economy': economy[0],
                'sectors': '16_other_sector',
                'sub1sectors': '16_02_agriculture_and_fishing',
                'sub2sectors': 'x',
                'sub3sectors': 'x',
                'sub4sectors': 'x',
                'fuels': mapped_values['fuels'],
                'subfuels': mapped_values['subfuels'],
                **{str(year): row[year] for year in range(OUTLOOK_BASE_YEAR + 1, OUTLOOK_LAST_YEAR + 1)}
            }
            transformed_data = transformed_data.append(new_row, ignore_index=True)

        sheet_data = pd.concat([sheet_data, transformed_data])

    return sheet_data

def process_agriculture(excel_file, shared_categories, economy, OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR):
    wb = load_workbook(filename=excel_file)

    # Load the mapping document
    mapping_df = pd.read_excel('./config/agriculture_mapping.xlsx')
    mapping_dict = mapping_df.set_index('Energy Demand (PJ)').to_dict('index')

    all_transformed_data = pd.DataFrame()

    # Determine which sheets to process
    if 'Output' in wb.sheetnames:
        all_transformed_data = pd.concat([all_transformed_data, process_sheet('Output', excel_file, economy, OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR, mapping_dict)])

    if 'Agriculture Output' in wb.sheetnames and 'Fishing Output' in wb.sheetnames:
        agri_data = process_sheet('Agriculture Output', excel_file, economy, OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR, mapping_dict)
        fish_data = process_sheet('Fishing Output', excel_file, economy, OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR, mapping_dict)

        # Concatenate the two DataFrames
        combined_data = pd.concat([agri_data, fish_data])

        # Group by shared categories and sum the agri and fish values
        combined_data = combined_data.groupby(shared_categories, as_index=False)[[str(year) for year in range(OUTLOOK_BASE_YEAR + 1, OUTLOOK_LAST_YEAR + 1)]].sum()

        all_transformed_data = pd.concat([all_transformed_data, combined_data])

    # Reorder the columns to match shared_categories
    final_columns = shared_categories + [str(year) for year in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1)]
    all_transformed_data = all_transformed_data[final_columns]

    # Save the combined transformed data
    # all_transformed_data.to_csv('data/temp/error_checking/agriculture_transformed.csv', index=False)

    return all_transformed_data

def split_subfuels(csv_file, layout_df, shared_categories, OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR):
    df = pd.read_csv(csv_file)

    # Determine the fuels to check based on file name
    if 'pipeline' in csv_file.lower():
        fuels_to_check = ['08_gas', '07_petroleum_products']
    # elif 'buildings' in csv_file.lower():
    #     fuels_to_check = ['12_solar']

    # Exclude 'subfuels' from shared_categories
    categories_for_matching = [cat for cat in shared_categories if cat != 'subfuels']

    # Define year columns for analysis (past 5 years)
    year_columns_for_analysis = list(range(OUTLOOK_BASE_YEAR-4, OUTLOOK_BASE_YEAR+1))

    for fuel in fuels_to_check:
        # Check if the fuel is in the 'fuels' column
        if fuel in df['fuels'].values:
            # Find rows in df with the specific fuel
            fuel_rows = df[df['fuels'] == fuel]

            for _, fuel_row in fuel_rows.iterrows():
                # Extract shared categories values from this row, excluding 'subfuels'
                category_values = {cat: fuel_row[cat] for cat in categories_for_matching}

                # Find rows in layout_df that match these category values exactly
                matching_condition = (layout_df[categories_for_matching] == pd.Series(category_values)).all(axis=1)
                matching_rows = layout_df[matching_condition]

                # Check if the year columns exist in matching_rows
                if not all(col in matching_rows.columns for col in year_columns_for_analysis):
                    raise Exception("Missing year columns in matching_rows:", [col for col in year_columns_for_analysis if col not in matching_rows.columns])

                # Melt matching_rows and keep only the past 5 years
                melted = matching_rows.melt(id_vars=shared_categories, value_vars=year_columns_for_analysis)

                # Sum up the years using groupby
                summed = melted.groupby(shared_categories).sum().reset_index()

                # Calculate the proportions
                total_values = summed[summed['subfuels'] == 'x']['value']
                proportion_dict = {}
                for _, row in summed.iterrows():
                    if row['subfuels'] != 'x':
                        proportion = row['value'] / total_values.iloc[0]
                        proportion_dict[row['subfuels']] = proportion

                # Create new rows in df using the proportions
                total_row_df = fuel_rows[fuel_rows['subfuels'] == 'x']
                if not total_row_df.empty:
                    total_row = total_row_df.iloc[0]
                    for subfuel, proportion in proportion_dict.items():
                        new_row = total_row.copy()
                        new_row['subfuels'] = subfuel
                        for year in range(OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR+1):
                            new_row[str(year)] = new_row[str(year)] * proportion
                        df = df.append(new_row, ignore_index=True)
        # Drop the total rows (with 'x' in 'subfuels') for the current fuel type
        df = df.drop(df[(df['fuels'] == fuel) & (df['subfuels'] == 'x')].index)

    return df


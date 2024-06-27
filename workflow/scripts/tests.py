import A_initial_read_and_save as A
import B_Create_energy_df as B
import C_subset_data as C
import D_merging_results as D
import E_calculate_emissions as E
import F_incorporate_capacity as F
import utility_functions as utils
import merging_functions
import supply_component_repo_functions
from datetime import datetime
import pandas as pd

def test_update_function(layout_df, new_layout_df,tfc_grouped_df, tfec_grouped_df,shared_categories, SINGLE_ECONOMY_ID, SAVE_TEST_RESULTS_TO_CSV=True):
    
    #TESTING:
    #COMPARE USING UDPATE to REMOVEING THEN CONCATING THE VLAUES, like in new_layout_df
    
    #DO UPDATE PROCESS:
    # Update the layout_df with the values from grouped_df
    layout_df.update(tfc_grouped_df)
    layout_df.update(tfec_grouped_df)

    #melt both dfs:
    layout_df = layout_df.reset_index().melt(id_vars=shared_categories, var_name='year', value_name='value')
    new_layout_df = new_layout_df.reset_index().melt(id_vars=shared_categories, var_name='year', value_name='value')
    
    #CHECK IF THE TWO DATAFRAMES ARE THE SAME:
    # Sort the dataframes by their index
    new_layout_df = new_layout_df.sort_index()
    layout_df = layout_df.sort_index()

    # Compare the two dataframes
    equal_df = new_layout_df.eq(layout_df)

    # Find the rows where the dataframes are not equal
    different_rows = equal_df[~equal_df.all(axis=1)]

    if different_rows.empty:
        print("The two dataframes are equal when comparing using update to removeing then concating the values.")
        #pivot the dfs back to the original format:
        layout_df = layout_df.pivot(index=shared_categories, columns='year', values='value')
        return layout_df
    else:
        # Print the indices of the different rows
        print(different_rows.index)

        # Print the different rows from new_layout_df
        print(new_layout_df.loc[different_rows.index])

        # Print the different rows from layout_df
        print(layout_df.loc[different_rows.index])
        
        if SAVE_TEST_RESULTS_TO_CSV:
            new_layout_df.loc[different_rows.index].to_csv(f'data/tests/merged_file_1_{SINGLE_ECONOMY_ID}.csv', index=False)
            layout_df.loc[different_rows.index].to_csv(f'data/tests/merged_file_2_{SINGLE_ECONOMY_ID}.csv', index=False)
        raise AssertionError("The two dataframes are not equal.")
    

def test_supply_components(SINGLE_ECONOMY_ID):
    #TESTING FUNCTION FOR GETTING SUPLY COMPONENTS TO WORK. needs a bit of study to get this function working properly. will help to keep you from having to move files to and fro.
    import os
    import re
    import shutil
    for file in os.listdir(f'./data/modelled_data/{SINGLE_ECONOMY_ID}/'):
        if re.search(SINGLE_ECONOMY_ID + '_pipeline_transport_', file):
            # os.remove(f'./data/modelled_data/{economy}/' + file)
            shutil.move(f'./data/modelled_data/{SINGLE_ECONOMY_ID}/' + file, f'./data/modelled_data/{SINGLE_ECONOMY_ID}/archive/' + file)
        if re.search(SINGLE_ECONOMY_ID + '_other_transformation_', file):
            # os.remove(f'./data/modelled_data/{economy}/' + file)
            shutil.move(f'./data/modelled_data/{SINGLE_ECONOMY_ID}/' + file, f'./data/modelled_data/{SINGLE_ECONOMY_ID}/archive/' + file)
        if re.search(SINGLE_ECONOMY_ID + '_other_own_use_', file):
            # os.remove(f'./data/modelled_data/{economy}/' + file)
            shutil.move(f'./data/modelled_data/{SINGLE_ECONOMY_ID}/' + file, f'./data/modelled_data/{SINGLE_ECONOMY_ID}/archive/' + file)
        if re.search(SINGLE_ECONOMY_ID + '_non_specified_', file):
            # os.remove(f'./data/modelled_data/{economy}/' + file)
            shutil.move(f'./data/modelled_data/{SINGLE_ECONOMY_ID}/' + file, f'./data/modelled_data/{SINGLE_ECONOMY_ID}/archive/' + file)
        if re.search(SINGLE_ECONOMY_ID + '_biomass_others_supply_', file):
            # os.remove(f'./data/modelled_data/{economy}/' + file)
            shutil.move(f'./data/modelled_data/{SINGLE_ECONOMY_ID}/' + file, f'./data/modelled_data/{SINGLE_ECONOMY_ID}/archive/' + file)
            
    # Set the working directory
    utils.set_working_directory()
    # #######
    # #DEFAULT FILES
    # final_energy_df = pd.read_pickle(f'final_energy_df_{SINGLE_ECONOMY_ID}.pkl')
    # model_df_clean_wide = pd.read_pickle(f'model_df_clean_wide_{SINGLE_ECONOMY_ID}.pkl')
    # #######
    # #these files are for testing the supply_component_repo_functions.pipeline_transport(SINGLE_ECONOMY_ID, final_energy_df) vs running it independently and trans_own_use_addon vs running it independently
    # model_df_clean_wide = pd.read_csv(f'merged_file_energy_20_USA_20231219.csv')
    # final_energy_df = pd.read_csv(f'final_energy_df_20_USA_20231219.csv')
    
    # # # make the column name of any columns that are 4 digvit numbers (years) into ints
    # for col in model_df_clean_wide.columns:
    #     if re.match(r'\d{4}', col):
    #         model_df_clean_wide.rename(columns={col: int(col)}, inplace=True)
            
    # for col in final_energy_df.columns:
    #     if re.match(r'\d{4}', col):
    #         final_energy_df.rename(columns={col: int(col)}, inplace=True)
    #######
    #these files are for testing supply_component_repo_functions.minor_supply_components(SINGLE_ECONOMY_ID, final_energy_df) vs running it independently
    # model_df_clean_wide = pd.read_csv(f'merged_file_energy_20_USA_20240313.csv')
    # final_energy_df = pd.read_csv(f'final_energy_df_20_USA_20240313.csv')
    
    #make the column name of any columns that are 4 digvit numbers (years) into ints
    # for col in model_df_clean_wide.columns:
    #     if re.match(r'\d{4}', col):
    #         model_df_clean_wide.rename(columns={col: int(col)}, inplace=True)
            
    # for col in final_energy_df.columns:
    #     if re.match(r'\d{4}', col):
    #         final_energy_df.rename(columns={col: int(col)}, inplace=True)
    #######
    
    
    # final_energy_df = pd.read_csv(f'final_energy_df_{SINGLE_ECONOMY_ID}.csv')
    print('\n ################################################# \nRunning supply component repo functions and merging_results right afterwards: \n')
    breakpoint()
    try:
        supply_component_repo_functions.pipeline_transport(SINGLE_ECONOMY_ID, final_energy_df)
        supply_component_repo_functions.trans_own_use_addon(SINGLE_ECONOMY_ID, final_energy_df)
        supply_component_repo_functions.minor_supply_components(SINGLE_ECONOMY_ID, final_energy_df)
        a = 1#set this to make error handling easier
    except Exception as e:
        print('################################################\n')
        print(e)
        print('################################################\n')
        breakpoint()

    old_final_energy_df = final_energy_df.copy()
    try:
        if a == 1:
            final_energy_df = D.merging_results(model_df_clean_wide, SINGLE_ECONOMY_ID)
    except Exception as e:
        #remove the files that were created and replace the old files
        breakpoint()
        print('################################################\n')
        print(e)
        print('################################################\n') 
        
        # #and save them to modelled_data folder too. but only after removing the latest version of the file
        for file in os.listdir(f'./data/modelled_data/{SINGLE_ECONOMY_ID}/'):
            if (re.search(SINGLE_ECONOMY_ID + '_pipeline_transport_', file) or
                re.search(SINGLE_ECONOMY_ID + '_other_transformation_', file) or
                re.search(SINGLE_ECONOMY_ID + '_other_own_use_', file) or
                re.search(SINGLE_ECONOMY_ID + '_non_specified_', file) or
                re.search(SINGLE_ECONOMY_ID + '_biomass_others_supply_', file)):
        
                    shutil.move(os.path.join(f'./data/modelled_data/{SINGLE_ECONOMY_ID}/', file), f'./data/modelled_data/{SINGLE_ECONOMY_ID}/archive2/' + file)
            
        for file in os.listdir(f'./data/modelled_data/{SINGLE_ECONOMY_ID}/archive/'):
            if re.search(SINGLE_ECONOMY_ID + '_pipeline_transport_', file):
                shutil.move(f'./data/modelled_data/{SINGLE_ECONOMY_ID}/archive/' + file, f'./data/modelled_data/{SINGLE_ECONOMY_ID}/' + file)
            if re.search(SINGLE_ECONOMY_ID + '_other_transformation_', file):
                shutil.move(f'./data/modelled_data/{SINGLE_ECONOMY_ID}/archive/' + file, f'./data/modelled_data/{SINGLE_ECONOMY_ID}/' + file)
            if re.search(SINGLE_ECONOMY_ID + '_other_own_use_', file):
                shutil.move(f'./data/modelled_data/{SINGLE_ECONOMY_ID}/archive/' + file, f'./data/modelled_data/{SINGLE_ECONOMY_ID}/' + file)
            if re.search(SINGLE_ECONOMY_ID + '_non_specified_', file):
                shutil.move(f'./data/modelled_data/{SINGLE_ECONOMY_ID}/archive/' + file, f'./data/modelled_data/{SINGLE_ECONOMY_ID}/' + file)
            if re.search(SINGLE_ECONOMY_ID + '_biomass_others_supply_', file):
                shutil.move(f'./data/modelled_data/{SINGLE_ECONOMY_ID}/archive/' + file, f'./data/modelled_data/{SINGLE_ECONOMY_ID}/' + file)
        
    utils.compare_values_in_final_energy_dfs(old_final_energy_df, final_energy_df)
    print('Done running supply component repo functions and merging_results ')
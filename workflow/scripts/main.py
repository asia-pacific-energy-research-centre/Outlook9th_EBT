#%%
# Import necessary modules
import A_initial_read_and_save as A
import B_Create_energy_df as B
import C_subset_data as C
import D_merging_results as D
import E_calculate_emissions as E
import F_incorporate_capacity as F
import G_aggregate_economies as G
import utility_functions as utils
import merging_functions
# import supply_component_repo_functions
import workflow.scripts.minor_fuel_supply_modelling as minor_fuel_supply_modelling

from estimate_missing_sectors import estimate_missing_sectors_using_activity_estimates
from final_data_adjustment_functions import adjust_projected_supply_to_balance_demand, make_manual_changes_to_rows
from data_checking_functions import double_check_difference_one_year_after_base_year, check_for_negatives_or_postives_in_wrong_sectors

from datetime import datetime
import pandas as pd

def main(SINGLE_ECONOMY_ID, ONLY_RUN_UP_TO_MERGING=False):
    """
    SINGLE_ECONOMY_ID: when we have new data from ESTO you will want to set this to False or None and run the data through the whole pipeline to get results/model_df_wide_' + date_today +'.csv' for modellers to use as an input
    
    Steps of this function are:
        A.initial_read_and_save(): Read in the data and save it as a pickle file
        
        B.create_energy_df(): Create the energy DataFrame. Includes some cleaning and formatting. Not very flexible so in the future we may want to make it more modular.
        
        C.subset_data(): Seems similar to B, but it's not. TODO whats it do?
        
        D.merging_results(): If you are using a single economy, it will take in data from modellers and merge it with the data from ESTO that was formatted in the previous steps. this will give us a final energy df that we can use to calculate emissions, visualize, etc.

        E.calculate_emissions(): Calculate emissions from the final energy df. simple
        
        F.incorporate_capacity_data(): Incorporate capacity data from the modellers into the final energy df. Requires the data form the modellers to be in a very specific format otherwise it will break. This might be the best way to do it, otherwise we would have to make a lot of assumptions about what the modellers are doing.
        
    Returns:
        _type_: _description_
    """
    # Set the working directory
    utils.set_working_directory()
    utils.check_russia_base_year(SINGLE_ECONOMY_ID)
    
    # Check if SINGLE_ECONOMY_ID is in utils.AGGREGATE_ECONOMIES
    if SINGLE_ECONOMY_ID in utils.AGGREGATE_ECONOMIES:
        # Run the aggregation function
        G.aggregate_economies(SINGLE_ECONOMY_ID)
        return None, None, None, None
    else:
        
        utils.identify_if_major_supply_data_is_available(SINGLE_ECONOMY_ID)
        
        # Perform initial read and save
        df_no_year_econ_index = A.initial_read_and_save(SINGLE_ECONOMY_ID) 
        
        # Create energy DataFrame
        layout_df = B.create_energy_df(df_no_year_econ_index, SINGLE_ECONOMY_ID)
        # breakpoint()
        # Subset the data
        layout_df = C.subset_data(layout_df, SINGLE_ECONOMY_ID)
        # breakpoint()
        if (isinstance(SINGLE_ECONOMY_ID, str)) and not (ONLY_RUN_UP_TO_MERGING):#if we arent using a single economy we dont need to merge
            # Merge the results
            first_merge_df = D.merging_results(layout_df, SINGLE_ECONOMY_ID)
            print('\n ### First merge complete ### \n')
            
            #########
            #NOTE TESTING PUTTING THIS HERE TO TRY ESTIMATE MISTING SECTORS WHERE WE CAN AS SOON AS THE DATA IS AVAILABLE. THIS WAY THE TRANSFOMATION SECTORS CAN GET THE DATA THEY NEED EARLIER IN THE PIPELINE.
            estimate_missing_sectors_using_activity_estimates(first_merge_df,SINGLE_ECONOMY_ID, MERGE_ONTO_INPUT_DATA=False,SAVE_OUTPUT_TO_MODELLED_DATA_FOLDER=True)
        
            #run merging again now that we've created new modelled files in the modelled data folder
            second_merge_df = D.merging_results(layout_df, SINGLE_ECONOMY_ID)
            # if economy == '16_RUS':#TESTING
            #     for scenario in second_merge_df.scenarios.unique():
            #         second_merge_df = make_manual_changes_to_rows(second_merge_df, SINGLE_ECONOMY_ID, scenario)
            #########
            if utils.MAJOR_SUPPLY_DATA_AVAILABLE:
                
                # #create newly modelled data using that first merge:
                # estimate_missing_sectors_using_activity_estimates(first_merge_df,SINGLE_ECONOMY_ID, MERGE_ONTO_INPUT_DATA=False,SAVE_OUTPUT_TO_MODELLED_DATA_FOLDER=True)

                # #run merging again now that we've created new modelled files in the modelled data folder
                # second_merge_df = D.merging_results(layout_df, SINGLE_ECONOMY_ID)
                
                #important to run this after merging new sector estiamtes in case they include new demands or transformation outputs.
                minor_fuel_supply_modelling.minor_fuels_supply_and_transformation_handler(SINGLE_ECONOMY_ID, second_merge_df, PLOT = True, CREATE_MARS_EXAMPLE=False)
                
                #run merging again now that we've created new modelled files in the modelled data folder
                third_merge_df = D.merging_results(layout_df, SINGLE_ECONOMY_ID)
                
                #this function will not save anything to the modelled data folder so it needs to be run at the end of the pipeline. it is important especially for balancing the extra demand created in estimate_missing_sectors_using_activity_estimates()
                try:
                    final_results_df = adjust_projected_supply_to_balance_demand(third_merge_df,SINGLE_ECONOMY_ID, utils.ERRORS_DAIJOUBU)
                except Exception as e:
                    breakpoint()
                    final_results_df = adjust_projected_supply_to_balance_demand(third_merge_df,SINGLE_ECONOMY_ID, utils.ERRORS_DAIJOUBU)
                #run merging to merge layout (i.e. layout_df) and final_results_df
                final_energy_df = D.merging_results(layout_df, SINGLE_ECONOMY_ID, final_results_df)           
                
                # utils.compare_values_in_final_energy_dfs(old_final_energy_df, final_energy_df)
                print('Done running supply component repo functions and merging_results \n################################################\n')
                
                for scenario in final_energy_df.scenarios.unique():
                    final_energy_df = make_manual_changes_to_rows(final_energy_df, SINGLE_ECONOMY_ID, scenario)
                
                if utils.CHECK_DATA:
                    print('Running data checks \n################################################\n')
                    ERROR1, error_text1 = double_check_difference_one_year_after_base_year(final_energy_df,SINGLE_ECONOMY_ID)
                        
                    ERROR2, error_text2, minor_errors_df, final_energy_df = check_for_negatives_or_postives_in_wrong_sectors(final_energy_df,SINGLE_ECONOMY_ID, file='final_energy_df')
    
                #calc emissions:
                emissions_df = E.calculate_emissions(final_energy_df,SINGLE_ECONOMY_ID)
                #calc capacity
                capacity_df = F.incorporate_capacity_data(final_energy_df,SINGLE_ECONOMY_ID)
                
                if utils.CHECK_DATA:
                    if (ERROR1 or ERROR2) and not utils.ERRORS_DAIJOUBU:
                        breakpoint()
                        raise ValueError('Errors found in data checks: {} \n {}'.format(error_text1, error_text2))
                    elif (ERROR1 or ERROR2) and utils.ERRORS_DAIJOUBU:
                        print('Errors found in data checks but ERRORS_DAIJOUBU: {} \n {}'.format(error_text1, error_text2))
                    print('Done running data checks \n################################################\n')
            else:
                #calc emissions:
                emissions_df = E.calculate_emissions(first_merge_df,SINGLE_ECONOMY_ID)
                #calc capacity
                capacity_df = F.incorporate_capacity_data(first_merge_df,SINGLE_ECONOMY_ID)
                
                return first_merge_df, None, None, layout_df
        else:
            return None, None, None, layout_df
    # Return the final DataFrame
    return final_energy_df, emissions_df, capacity_df, layout_df
#%%
# Run the main function and store the result
RUSSIA = ['16_RUS']
COMPLETED = ['15_PHL','01_AUS','18_CT','12_NZ', '11_MEX','06_HKC', '03_CDA','02_BD', '21_VN', '13_PNG', '17_SGP']#'10_MAS', '20_USA','05_PRC',
TRANSFORMATION = ['09_ROK','07_INA']
DEMAND = []
SUPPLY = [ '19_THA','08_JPN', '14_PE','04_CHL']#'05_PRC']#, '08_JPN','04_CHL']#'20_USA', '19_THA', '07_INA',]['05_PRC', '11_MEX']#,#'16_RUS']# '14_PE',
FOUND=False#TODO, USA'S TRANSFERS FOR CRUDE AND OPETROLUM PRODUCTS SHOULDBE SONCISDRED.
if __name__ == "__main__":#"03_CDA","05_PRC","07_INA","11_MEX","18_CT","19_THA", 
    AGGREGATE_ECONOMY_MAPPING = {
        '00_APEC': ['01_AUS', '02_BD', '03_CDA', '04_CHL', '05_PRC', '06_HKC', '07_INA', '08_JPN', '09_ROK', '10_MAS', '11_MEX', '12_NZ', '13_PNG', '14_PE', '15_PHL', '16_RUS', '17_SGP', '18_CT', '19_THA', '20_USA', '21_VN'],
        '22_SEA': ['02_BD', '07_INA', '10_MAS', '15_PHL', '17_SGP', '19_THA', '21_VN'],
        '23_NEA': ['06_HKC', '08_JPN', '09_ROK', '18_CT'],
        '23b_ONEA': ['06_HKC', '09_ROK', '18_CT'],
        '24_OAM': ['03_CDA', '04_CHL', '11_MEX', '14_PE'],
        '24b_OOAM': ['04_CHL', '11_MEX', '14_PE'],
        '25_OCE': ['01_AUS', '12_NZ', '13_PNG'],
        '26_NA': ['03_CDA', '20_USA'],
    }
    for economy in ['12_NZ']:#['01_AUS']:#AGGREGATE_ECONOMY_MAPPING.keys():#['16_RUS']:#['18_CT','01_AUS']+SUPPLY+'19_THA']:#['05_PRC']:#[ '19_THA']:#['07_INA','09_ROK', '20_USA','02_BD', '15_PHL',  '21_VN', '13_PNG']:#['07_INA']:# ['18_CT']:#['15_PHL', '07_INA']:#,'15_PHL','02_BD', '21_VN','01_AUS']:#[  '14_PE']:#[ '14_PE','04_CHL','08_JPN', '17_SGP','19_THA', '20_USA','21_VN', '18_CT','03_CDA''17_SGP',]+SUPPLY:#['00_APEC', '22_SEA', '23_NEA', '23b_ONEA', '24_OAM', '25_OCE', '24b_OOAM', '26_NA'] + SUPPLY + TRANSFORMATION:# ['20_USA', '19_THA', '07_INA','03_CDA']:#['26_NA' ]:# "01_AUS"]:#, 01_AUS', "02_BD", "03_CDA", "04_CHL", "05_PRC", "06_HKC", "07_INA", "08_JPN", "09_ROK", "10_MAS", "11_MEX", "12_NZ", "13_PNG", "14_PE", "15_PHL", "16_RUS", "17_SGP", "18_CT", "19_THA", "20_USA", "21_VN", '00_APEC '09_ROK', '02_BD',  '15_PHL' "10_MAS", '08_JPN', '17_SGP','19_THA', '20_USA', '20_USA',['15_PHL', '10_MAS', '21_VN','01_AUS']+'03_CDA', '08_JPN','20_USA', '15_PHL',
        final_energy_df, emissions_df, capacity_df, model_df_clean_wide = main(SINGLE_ECONOMY_ID=economy)
#%%
#"06_HKC",'04_CHL' - seemed it could be because we need to just base iput data for moelling offnew data. 
#%%
#todo:
#mexico, indonesia, '02_BD'

# %%
# utils.move_files_to_onedrive('08_Final', origin_date_id='20250602', econ_list=['06_HKC', '03_CDA','09_ROK', '20_USA','02_BD', '21_VN', '13_PNG','05_PRC', '17_SGP','07_INA','15_PHL', '19_THA']) #todo: '17_SGP','05_PRC', '07_INA'
# %%

# #analyse the file merged_file_energy_19_THA_20250603.csv. we want to drop rows where all year vlaues are 0
# import re
# a = pd.read_csv('merged_file_energy_19_THA_20250603.csv')
# year_cols = [col for col in a.columns if re.match(r'^\d{4}$', col)]
# a =a.loc[~(a[year_cols] == 0).all(axis=1)] #drop rows where all year values are 

# # then melt and split into twp dfs, one for where year < 2023 and one for where year >= 2023. then keep subtotal_layout where year < 2023 and keep subtotal_results where year >= 2023. then rename them to subtotal. then drop where it is true. Then concat them for
# cols = ['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels', 'subtotal_layout', 'subtotal_results']
# a1 = a.melt(id_vars=cols, value_vars=year_cols, var_name='year', value_name='value')
# a1['year'] = a1['year'].astype(int)
# a2 = a1.loc[a1['year'] < 2023]
# a2 = a2.loc[~(a2['subtotal_layout'] == True)]
# a3 = a1.loc[a1['year'] >= 2023]
# a3 = a3.loc[~(a3['subtotal_results'] == True)]
# a3 = a3.rename(columns={'subtotal_results': 'subtotal'})
# a2 = a2.rename(columns={'subtotal_layout': 'subtotal'})
# #drop 0s too
# a2 = a2.loc[~(a2['value'] == 0)]
# a3 = a3.loc[~(a3['value'] == 0)]
# #concat them
# a4 = pd.concat([a2, a3], ignore_index=True)
# #sum all by  ['scenarios', 'economy', 'sectors','fuels', 'subfuels']
# a4 = a4.groupby(['scenarios', 'economy', 'sectors','fuels', 'subfuels', 'year'])['value'].sum().reset_index()
# #pvito wid

# cols = ['scenarios', 'economy', 'sectors','fuels', 'subfuels']
# a4 = a4.pivot_table(index=cols, columns='year', values='value', fill_value=0).reset_index()
# a4.to_clipboard()

# %%

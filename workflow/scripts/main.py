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
from final_data_adjustment_functions import adjust_projected_supply_to_balance_demand
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
    utils.identify_if_major_supply_data_is_available(SINGLE_ECONOMY_ID)
    utils.check_russia_base_year(SINGLE_ECONOMY_ID)
    
    # Check if SINGLE_ECONOMY_ID is in utils.AGGREGATE_ECONOMIES
    if SINGLE_ECONOMY_ID in utils.AGGREGATE_ECONOMIES:
        # Run the aggregation function
        G.aggregate_economies(SINGLE_ECONOMY_ID)
        return None, None, None, None
    else:
        # Perform initial read and save
        df_no_year_econ_index = A.initial_read_and_save(SINGLE_ECONOMY_ID) 
        
        # Create energy DataFrame
        layout_df = B.create_energy_df(df_no_year_econ_index, SINGLE_ECONOMY_ID)
        # Subset the data
        
        layout_df = C.subset_data(layout_df, SINGLE_ECONOMY_ID)
        
        if (isinstance(SINGLE_ECONOMY_ID, str)) and not (ONLY_RUN_UP_TO_MERGING):#if we arent using a single economy we dont need to merge
            # Merge the results
            # layout_df.to_csv('temp.csv', index=False)
            # breakpoint()
            # layout_df = pd.read_csv('temp.csv')
            first_merge_df = D.merging_results(layout_df, SINGLE_ECONOMY_ID)
            print('\n ### First merge complete ### \n')
            if utils.MAJOR_SUPPLY_DATA_AVAILABLE:
                #create newly modelled data using that first merge:
                estimate_missing_sectors_using_activity_estimates(first_merge_df,SINGLE_ECONOMY_ID, MERGE_ONTO_INPUT_DATA=False,SAVE_OUTPUT_TO_MODELLED_DATA_FOLDER=True)

                #run merging again now that we've created new modelled files in the modelled data folder
                second_merge_df = D.merging_results(layout_df, SINGLE_ECONOMY_ID)
                
                #important to run this after merging new sector estiamtes in case they include new demands or transformation outputs.
                minor_fuel_supply_modelling.minor_fuels_supply_and_transformation_handler(SINGLE_ECONOMY_ID, second_merge_df, PLOT = True, CREATE_MARS_EXAMPLE=False)
                
                #run merging again now that we've created new modelled files in the modelled data folder
                third_merge_df = D.merging_results(layout_df, SINGLE_ECONOMY_ID)
                
                #this function will not save anything to the modelled data folder so it needs to be run at the end of the pipeline. it is important especially for balancing the extra demand created in estimate_missing_sectors_using_activity_estimates()
                final_results_df = adjust_projected_supply_to_balance_demand(third_merge_df,SINGLE_ECONOMY_ID, utils.ERRORS_DAIJOUBU)
                
                #run merging to merge layout (i.e. layout_df) and final_results_df
                final_energy_df = D.merging_results(layout_df, SINGLE_ECONOMY_ID, final_results_df)           
                
                # utils.compare_values_in_final_energy_dfs(old_final_energy_df, final_energy_df)
                print('Done running supply component repo functions and merging_results \n################################################\n')
                
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
            return None, None, None, layout_df
    # Return the final DataFrame
    return final_energy_df, emissions_df, capacity_df, layout_df
#%%
# Run the main function and store the result
FOUND=False
if __name__ == "__main__":
    # for economy in [ "18_CT", "19_THA", "20_USA", "21_VN"]:#'01_AUS', "02_BD", "03_CDA", "04_CHL", "05_PRC", "06_HKC", "07_INA", "08_JPN", "09_ROK", "10_MAS", "11_MEX", "12_NZ", "13_PNG", "14_PE", "15_PHL", "16_RUS", "17_SGP", "18_CT", "19_THA", "20_USA", "21_VN"]:#['01_AUS', "02_BD", "15_PHL", "18_CT","21_VN"]:# utils.AGGREGATE_ECONOMIES:#['26_NA' ]:# "01_AUS"]:#, 01_AUS', "02_BD", "03_CDA", "04_CHL", "05_PRC", "06_HKC", "07_INA", "08_JPN", "09_ROK", "10_MAS", "11_MEX", "12_NZ", "13_PNG", "14_PE", "15_PHL", "16_RUS", "17_SGP", "18_CT", "19_THA", "20_USA", "21_VN", '00_APEC '09_ROK', '02_BD',  '15_PHL'
        #     if economy == '05_PRC':
        #         FOUND = True
        #     elif not FOUND:
        #         continue
        # '01_AUS', "02_BD", "03_CDA", "04_CHL", "05_PRC", "06_HKC", "07_INA", "08_JPN", "09_ROK", "10_MAS", "11_MEX", "12_NZ", "13_PNG", "14_PE", "15_PHL", "16_RUS", "17_SGP", "18_CT", "19_THA", "20_USA", "21_VN", '00_APEC' 
    # try:
    final_energy_df, emissions_df, capacity_df, model_df_clean_wide = main(SINGLE_ECONOMY_ID='03_CDA')#'01_AUS')#'16_RUS')#economy)#'00_APEC')#economy)
    
#%%
#"06_HKC",'04_CHL' - seemed it could be because we need to just base iput data for moelling offnew data. 
# %%
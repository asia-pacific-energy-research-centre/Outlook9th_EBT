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
import supply_component_repo_functions
import biofuels_functions as biofuels_functions

from datetime import datetime
import pandas as pd

def main(ONLY_RUN_UP_TO_MERGING=False, SINGLE_ECONOMY_ID = utils.SINGLE_ECONOMY_ID_VAR):
    """
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
    if SINGLE_ECONOMY_ID == '16_RUS':
        #check that the OUTLOOK_BASE_YEAR in utils is == to OUTLOOK_BASE_YEAR_russia in utils, else raise an error
        #and if SINGLE_ECONOMY_ID != '16_RUS' then check that OUTLOOK_BASE_YEAR in utils != OUTLOOK_BASE_YEAR_russia in utils, else raise an error
        if utils.OUTLOOK_BASE_YEAR != utils.OUTLOOK_BASE_YEAR_RUSSIA:
            raise ValueError('OUTLOOK_BASE_YEAR in utils is not equal to OUTLOOK_BASE_YEAR_RUSSIA in utils')
    elif SINGLE_ECONOMY_ID != '16_RUS':
        if utils.OUTLOOK_BASE_YEAR == utils.OUTLOOK_BASE_YEAR_RUSSIA:
            raise ValueError('OUTLOOK_BASE_YEAR in utils is equal to OUTLOOK_BASE_YEAR_RUSSIA in utils')
        
    # Check if SINGLE_ECONOMY_ID is in utils.AGGREGATE_ECONOMIES
    if SINGLE_ECONOMY_ID in utils.AGGREGATE_ECONOMIES:
        # Run the aggregation function
        G.aggregate_economies(SINGLE_ECONOMY_ID)
        return None, None, None, None
    else:
        # Perform initial read and save
        try:
            df_no_year_econ_index = A.initial_read_and_save(SINGLE_ECONOMY_ID)
        except Exception as e:
            print(f'Error in initial_read_and_save for {SINGLE_ECONOMY_ID}')
            print(e)
            breakpoint()
            #save error to a txt file
            with open(f'error_{SINGLE_ECONOMY_ID}.txt', 'w') as f:
                f.write(str(e))
            return None, None, None, None    
        
        # Create energy DataFrame
        model_df_clean_wide = B.create_energy_df(df_no_year_econ_index, SINGLE_ECONOMY_ID)
        # Subset the data
        model_df_clean_wide = C.subset_data(model_df_clean_wide, SINGLE_ECONOMY_ID)
        if (isinstance(SINGLE_ECONOMY_ID, str)) and not (ONLY_RUN_UP_TO_MERGING):#if we arent using a single economy we dont need to merge
            # Merge the results
            final_energy_df = D.merging_results(model_df_clean_wide, SINGLE_ECONOMY_ID)
            print('\n ################################################# \nRunning supply component repo functions and merging_results right afterwards: \n')
            supply_component_repo_functions.pipeline_transport(SINGLE_ECONOMY_ID, final_energy_df)
            supply_component_repo_functions.trans_own_use_addon(SINGLE_ECONOMY_ID, final_energy_df)
            supply_component_repo_functions.minor_supply_components(SINGLE_ECONOMY_ID, final_energy_df)
            biofuels_functions.biofuels_supply_and_transformation_handler(SINGLE_ECONOMY_ID, final_energy_df, PLOT = True, CREATE_MARS_EXAMPLE=False)
            old_final_energy_df = final_energy_df.copy()
            final_energy_df = D.merging_results(model_df_clean_wide, SINGLE_ECONOMY_ID)
            # utils.compare_values_in_final_energy_dfs(old_final_energy_df, final_energy_df)
            print('Done running supply component repo functions and merging_results \n################################################\n')
            
            #calc emissions:
            # breakpoint()
            # final_energy_df.to_csv('final_energy_df.csv', index=False)#TEST
            try:
                emissions_df = E.calculate_emissions(final_energy_df,SINGLE_ECONOMY_ID)
            except Exception as e:
                print(f'Error in calculate_emissions for {SINGLE_ECONOMY_ID}')
                print(e)
                breakpoint()
                #save error to a txt file
                with open(f'emissions_error_{SINGLE_ECONOMY_ID}.txt', 'w') as f:
                    f.write(str(e))
                return None, None, None, model_df_clean_wide
            
            #calc capacity
            capacity_df = F.incorporate_capacity_data(final_energy_df,SINGLE_ECONOMY_ID)
        else:
            return None, None, None, model_df_clean_wide
    # Return the final DataFrame
    return final_energy_df, emissions_df, capacity_df, model_df_clean_wide
#%%
# Run the main function and store the result
FOUND=False
if __name__ == "__main__":
    # for economy in utils.AGGREGATE_ECONOMIES:#['26_NA' ]:# "01_AUS"]:#, 01_AUS', "02_BD", "03_CDA", "04_CHL", "05_PRC", "06_HKC", "07_INA", "08_JPN", "09_ROK", "10_MAS", "11_MEX", "12_NZ", "13_PNG", "14_PE", "15_PHL", "16_RUS", "17_SGP", "18_CT", "19_THA", "20_USA", "21_VN", '00_APEC
        #     if economy == '05_PRC':
        #         FOUND = True
        #     elif not FOUND:
        #         continue
        # '01_AUS', "02_BD", "03_CDA", "04_CHL", "05_PRC", "06_HKC", "07_INA", "08_JPN", "09_ROK", "10_MAS", "11_MEX", "12_NZ", "13_PNG", "14_PE", "15_PHL", "16_RUS", "17_SGP", "18_CT", "19_THA", "20_USA", "21_VN", '00_APEC' 
    # try:
    final_energy_df, emissions_df, capacity_df, model_df_clean_wide = main(SINGLE_ECONOMY_ID='16_RUS')#'16_RUS')#economy)#'00_APEC')#economy)
    
    
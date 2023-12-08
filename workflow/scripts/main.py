#%%
# Import necessary modules
import A_initial_read_and_save as A
import B_Create_energy_df as B
import C_subset_data as C
import D_merging_results as D
import E_calculate_emissions as E
import F_incorporate_capacity as F
import utility_functions as utils
import merging_functions
from datetime import datetime

def main(ONLY_RUN_UP_TO_MERGING=False, SINGLE_ECONOMY_ID = utils.SINGLE_ECONOMY_ID_VAR):
    """
    Steps of this function are:
        A.initial_read_and_save(): Read in the data and save it as a pickle file
        
        B.create_energy_df(): Create the energy DataFrame. Includes some cleaning and formatting. Not very flexible so in the future we may want to make it more modular.
        
        C.subset_data(): Seems similar to B, but it's not. TODO whats it do?
        
        D.merging_results(): If you are using a single economy, it will take in data from modellers and merge it with the data from ESTO that was formatted in the previous steps. this will give us a final energy df that we can use to calculate emissions, visualise, etc.

        E.calculate_emissions(): Calculate emissions from the final energy df. simple
        
        F.incorporate_capacity_data(): Incorporate capacity data from the modellers into the final energy df. Requires the data form the modellers to be in a very specific format otherwise it will break. This might be the best way to do it, otehrwise we would have to make a lot of assumptions about what the modellers are doing.
        
    Returns:
        _type_: _description_
    """
    # Set the working directory
    utils.set_working_directory()
    
    # Perform initial read and save
    df_no_year_econ_index = A.initial_read_and_save(SINGLE_ECONOMY_ID)
    
    # Create energy DataFrame
    model_df_clean_wide = B.create_energy_df(df_no_year_econ_index, SINGLE_ECONOMY_ID)
    
    # Subset the data
    model_df_clean_wide = C.subset_data(model_df_clean_wide, SINGLE_ECONOMY_ID)
    
    if (isinstance(SINGLE_ECONOMY_ID, str)) and not (ONLY_RUN_UP_TO_MERGING):#if we arent using a single economy we dont need to merge
        # Merge the results
        final_energy_df = D.merging_results(model_df_clean_wide, SINGLE_ECONOMY_ID)
        
        #calc emissions:
        emissions_df = E.calculate_emissions(final_energy_df,SINGLE_ECONOMY_ID)
        
        #calc capacity
        capacity_df = F.incorporate_capacity_data(final_energy_df,SINGLE_ECONOMY_ID)
    else:
        return None, None, None, model_df_clean_wide
    # Return the final DataFrame
    return final_energy_df, emissions_df, capacity_df, model_df_clean_wide

def run_main_up_to_merging_for_every_economy(LOCAL_FILE_PATH, MOVE_OLD_FILES_TO_ARCHIVE=False):
    """
    This is really just meant for moving every economy's model_df_clean_wide df into {LOCAL_FILE_PATH}\Modelling\Integration\{ECONOMY_ID}\00_LayoutTemplate so the modellers can use it as a starting point for their modelling.
    
    it willremove the original files from the folder and move them to an archive folder in the same directory using the function utils.move_files_to_archive_for_economy(LOCAL_FILE_PATH, economy) if MOVE_OLD_FILES_TO_ARCHIVE is True
    """
    file_date_id = datetime.now().strftime('%Y%m%d')
    for economy in utils.ALL_ECONOMY_IDS:
        
        if MOVE_OLD_FILES_TO_ARCHIVE:
            utils.move_files_to_archive_for_economy(LOCAL_FILE_PATH, economy)
        final_energy_df, emissions_df, capacity_df, model_df_clean_wide = main(ONLY_RUN_UP_TO_MERGING = True, SINGLE_ECONOMY_ID=economy)
        model_df_clean_wide.to_csv(f'{LOCAL_FILE_PATH}/Modelling/Integration/{economy}/00_LayoutTemplate/model_df_wide_{economy}_{file_date_id}.csv', index=False)
        
        reference_df = model_df_clean_wide[model_df_clean_wide['scenarios'] == 'reference'].copy().reset_index(drop = True)
        target_df = model_df_clean_wide[model_df_clean_wide['scenarios'] == 'target'].copy().reset_index(drop = True)
        
        reference_df.to_csv(f'{LOCAL_FILE_PATH}/Modelling/Integration/{economy}/00_LayoutTemplate/model_df_wide_tgt_{economy}_{file_date_id}.csv', index=False)
        target_df.to_csv(f'{LOCAL_FILE_PATH}/Modelling/Integration/{economy}/00_LayoutTemplate/model_df_wide_ref_{economy}_{file_date_id}.csv', index=False)
        print('Done run_main_up_to_merging_for_every_economy for ' + economy) 
        
    
#%%
# Run the main function and store the result
final_energy_df, emissions_df, capacity_df, model_df_clean_wide = main()
#C:/Users/finbar.maunsell/OneDrive - APERC/outlook 9th
# run_main_up_to_merging_for_every_economy(LOCAL_FILE_PATH= r'C:/Users/finbar.maunsell/OneDrive - APERC/outlook 9th', MOVE_OLD_FILES_TO_ARCHIVE=True)

#%%
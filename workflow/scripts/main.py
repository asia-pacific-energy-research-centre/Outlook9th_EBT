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

def main():
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
    df_no_year_econ_index = A.initial_read_and_save()
    
    # Create energy DataFrame
    merged_df_clean_wide = B.create_energy_df(df_no_year_econ_index)
    
    # Subset the data
    merged_df_clean_wide = C.subset_data(merged_df_clean_wide)
    
    if utils.USE_SINGLE_ECONOMY:#if we arent using a single economy we dont need to merge
        # Merge the results
        final_energy_df = D.merging_results(merged_df_clean_wide)
        
        #calc emissions:
        emissions_df = E.calculate_emissions(final_energy_df)
        
        #calc capacity
        capacity_df = F.incorporate_capacity_data(final_energy_df)
    else:
        return None, None, None, merged_df_clean_wide
    # Return the final DataFrame
    return final_energy_df, emissions_df, capacity_df, merged_df_clean_wide
#%%
# Run the main function and store the result
final_energy_df, emissions_df, capacity_df, merged_df_clean_wide = main()
#%%
#%%
# Import necessary modules
import A_initial_read_and_save as A
import B_Create_energy_df as B
import C_subset_data as C
import D_merging_results as D
import E_calculate_emissions as E
import F_incorporate_capacity as F
import utility_functions as utils

def main():
    # Set the working directory
    utils.set_working_directory()
    
    # Perform initial read and save
    df_no_year_econ_index = A.initial_read_and_save()
    
    # Create energy DataFrame
    merged_df_clean_wide = B.create_energy_df(df_no_year_econ_index)
    
    # Subset the data
    merged_df_clean_wide = C.subset_data(merged_df_clean_wide)
    
    # Merge the results
    final_energy_df = D.merging_results(merged_df_clean_wide)
    
    #calc emissions:
    emissions_df = E.calculate_emissions(final_energy_df)
    
    #calc capacity
    capacity_df = F.incorporate_capacity_data(final_energy_df)
    # Return the final DataFrame
    return final_energy_df, emissions_df, capacity_df
#%%
# Run the main function and store the result
final_energy_df, emissions_df, capacity_df = main()
#%%
#%%
# Import necessary modules
import A_initial_read_and_save as A
import B_Create_energy_df as B
import C_subset_data as C
import D_merging_results as D
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
    layout_df = D.merging_results(merged_df_clean_wide)

    # Return the final DataFrame
    return layout_df

# Run the main function and store the result
result = main()
#%%

import pandas as pd

def test_update_function(layout_df, new_layout_df,tfc_grouped_df, tfec_grouped_df,shared_categories, SINGLE_ECONOMY, SAVE_TEST_RESULTS_TO_CSV=True):
    
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
            new_layout_df.loc[different_rows.index].to_csv(f'data/tests/merged_file_1_{SINGLE_ECONOMY}.csv', index=False)
            layout_df.loc[different_rows.index].to_csv(f'data/tests/merged_file_2_{SINGLE_ECONOMY}.csv', index=False)
        raise AssertionError("The two dataframes are not equal.")
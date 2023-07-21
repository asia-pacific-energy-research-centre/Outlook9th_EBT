
import pandas as pd

def test_update_function(layout_df, tfc_grouped_df, tfec_grouped_df,shared_categories, SINGLE_ECONOMY, SAVE_TEST_RESULTS_TO_CSV=True):
    
    #TESTING:
    #COMPARE USING UDPATE AND INSTERAD REMOVEING THEN CONCATING THE VLAUES:
    #SO DROP WHERE SECTION IS 12 OR 13 THEN CONCAT THE TWO DATAFRAMES:
    LAYOUT_DF_TEST = layout_df.copy()
    LAYOUT_DF_TEST.reset_index(inplace=True)
    LAYOUT_DF_TEST = LAYOUT_DF_TEST[~LAYOUT_DF_TEST['sectors'].isin(['12_total_final_consumption', '13_total_final_energy_consumption'])]
    LAYOUT_DF_TEST.set_index(shared_categories, inplace=True)
    #CONCAT
    LAYOUT_DF_TEST = pd.concat([LAYOUT_DF_TEST, tfc_grouped_df, tfec_grouped_df])
    
    # Update the layout_df with the values from grouped_df
    layout_df.update(tfc_grouped_df)
    layout_df.update(tfec_grouped_df)

    #melt both dfs:
    layout_df = layout_df.reset_index().melt(id_vars=shared_categories, var_name='year', value_name='value')
    LAYOUT_DF_TEST = LAYOUT_DF_TEST.reset_index().melt(id_vars=shared_categories, var_name='year', value_name='value')
    
    #CHECK IF THE TWO DATAFRAMES ARE THE SAME:
    # Sort the dataframes by their index
    LAYOUT_DF_TEST = LAYOUT_DF_TEST.sort_index()
    layout_df = layout_df.sort_index()

    # Compare the two dataframes
    equal_df = LAYOUT_DF_TEST.eq(layout_df)

    # Find the rows where the dataframes are not equal
    different_rows = equal_df[~equal_df.all(axis=1)]

    if different_rows.empty:
        return layout_df
    else:
        # Print the indices of the different rows
        print(different_rows.index)

        # Print the different rows from LAYOUT_DF_TEST
        print(LAYOUT_DF_TEST.loc[different_rows.index])

        # Print the different rows from layout_df
        print(layout_df.loc[different_rows.index])
        
        if SAVE_TEST_RESULTS_TO_CSV:
            LAYOUT_DF_TEST.loc[different_rows.index].to_csv(f'data/tests/merged_file_1_{SINGLE_ECONOMY}.csv', index=False)
            layout_df.loc[different_rows.index].to_csv(f'data/tests/merged_file_2_{SINGLE_ECONOMY}.csv', index=False)
        raise AssertionError("The two dataframes are not equal.")
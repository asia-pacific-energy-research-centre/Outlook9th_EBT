"""A script to merge the layout file and the demand output results files."""
#%%
import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from utility_functions import *
import merging_functions
import warnings
import results_adjustment_functions
from data_checking_functions import check_for_negatives_or_postives_in_wrong_sectors
# from adjust_data_with_post_hoc_changes import revert_changrevert_changes_to_merged_file_where_KEEP_CHANGES_IN_FINAL_OUTPUT_is_Falsees_to_merged_file_where_KEEP_CHANGES_IN_FINAL_OUTPUT_is_False

def merging_results(original_layout_df, SINGLE_ECONOMY_ID, final_results_df=None, previous_merged_df_filename=None, PRINT=False):
    """Takes the layout file and the results files, and merges them together.  The results files are generated from the model runs in the demand model, and the layout file is generated from the historical data file and the layout file template. 
    The process is messy and in some places doesnt seem to take the simplest way forwards. Most of the time this is because of compromises that are made because of the different level of detail between inputs, and the layout file. So the code here is complex, but relatively flexible.
    Some issues with the structure of the data that we need to deal with:
    - subtotals are not consistently calcaulted in any of the layout or results data so we label them, remove them and then recalcautle them always. 
    - layout file contains less specific data than the categories are for, so for example, even if there is a possible category for passenger air transprot, the layout file will only have data for air transport. So this is partly why calculate subtotals in results and layout data separately, and also why the merge is an outer join.

    #note that revert_changes_to_merged_file_where_KEEP_CHANGES_IN_FINAL_OUTPUT_is_False is used at the end o this file
    """
    # layout_df = pd.read_csv(layout_file)
    layout_df = original_layout_df.copy()

    # #make sure all year columns in layout_df are ints
    # layout_year_cols = [col for col in layout_df.columns if str(col).isdigit()]
    # layout_df.rename(columns={col: int(col) for col in layout_year_cols}, inplace=True)
    # layout_df = revert_changes_to_merged_file_where_KEEP_CHANGES_IN_FINAL_OUTPUT_is_False(SINGLE_ECONOMY_ID,layout_df)#i dont think this will work becasue it will create differences between results and layout for hist years but maybe its ok?
    #extract unique economies:
    economies = layout_df['economy'].unique()

    years_to_keep_in_results = [str(year) for year in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1)]

    # Create an empty concatted_results_df with the shared_categories
    concatted_results_df = pd.DataFrame(columns=shared_categories)

    if (isinstance(SINGLE_ECONOMY_ID, str)):
        # Define the path pattern for the results data files
        results_data_path = 'data/modelled_data/'+SINGLE_ECONOMY_ID+'/*'
        print(results_data_path)
    else:
        print("Not implemented yet.")

    # Define the path pattern for the results data files
    #results_data_path = 'data/modelled_data/*'
    # Get a list of all matching results data file paths
    results_data_files = [f for f in glob.glob(results_data_path) if os.path.isfile(f)]
    # Check if results_data_files is empty
    if not results_data_files:
        print("No files found in the specified path.")
        # Exit the function
        return None
    USING_FINAL_RESULTS_DF = False
    if final_results_df is not None:
        USING_FINAL_RESULTS_DF = True
        #we wont bother going through the results files and sintead just use the final_results_df that has been passed in. This is useful for when we have the output fromthis funciton that had a few adjusmtnes made and we jsut want to merge it again with the layout file.
        # quickly make surethe data matches requirements: 
        final_results_df = final_results_df[(final_results_df['economy'].isin(economies))]
        if 'subtotal_results' in final_results_df.columns:
            final_results_df = final_results_df.loc[final_results_df['subtotal_results']==False]
            final_results_df.drop(columns='subtotal_results', inplace=True)
        
        #now if we ignore te year columns, check the columns mathc otherwsie
        non_year_cols = [col for col in final_results_df.columns if not col.isdigit()]
        if set(non_year_cols) != set(concatted_results_df.columns):
            breakpoint()
            raise ValueError("final_results_df columns do not match the concatted_results_df columns.")
        
        #and set the only year cols we have to be the ones in 
        final_results_df = final_results_df[shared_categories + years_to_keep_in_results]
        # breakpoint()#seems theres still subttals?
        concatted_results_df = final_results_df.copy()
        results_data_files = []
        concatted_results_df['origin'] = 'final_results_df'
        #and label subtotals with false so we can use them later:
        concatted_results_df['is_subtotal'] = False
        # concatted_results_df = merging_functions.label_subtotals(concatted_results_df, shared_categories + ['origin'])
        
    #FIRST, LOAD IN ALL THE RESULTS FILES AND CONCAT THEM TOGETHER. THEN WE CAN MERGE THEM WITH THE LAYOUT FILE AND IDENTIFY ANY STRUCTURAL ISSUES
    # Iterate over the results files
    for file in results_data_files:
            
        if PRINT:
            print(f"Start processing {file}...")
        # Check if the file is an Excel file
        if file.endswith('.xlsx'):
            xls = pd.ExcelFile(file)

            # Check if 'agriculture' is in the file name (case-insensitive)
            if 'agriculture' in file.lower():
                # Check if the last sheet is either 'output' or 'fishing output' or 'agriculture output'
                last_sheet_name = xls.sheet_names[-1].lower()
                if last_sheet_name == 'output' or last_sheet_name == 'fishing output' or last_sheet_name == 'agriculture output':
                    results_df = merging_functions.process_agriculture(file, shared_categories, economies, OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR)
                else:
                    results_df = pd.read_excel(file, sheet_name=-1)  # Processing for already transformed agriculture files
            else:
                results_df = pd.read_excel(file, sheet_name=-1)  # Processing for non-agriculture files
            if PRINT:
                print(f"Processing Excel file {file}...")

        # Process CSV files
        elif file.endswith('.csv'):
            # Check if 'pipeline' or 'buildings' is in the file name (case-insensitive)
            if 'pipeline' in file.lower(): # or 'buildings' in file.lower():
                results_df = merging_functions.split_fuels_into_subfuels_based_on_historical_splits(file, layout_df, shared_categories, OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR) # Split subfuels for pipeline and buildings files
                if PRINT:
                    print(f"Processing CSV file {file} with split subfuels...")
            else:
                results_df = pd.read_csv(file)
                if PRINT:
                    print(f"Processing CSV file {file}...")

        # Handle unsupported file formats
        else:
            print(f"Unsupported file format: {file}")
            continue
        try:
            # Reorder the shared categories in the results DataFrame
            results_df = results_df[shared_categories + list(results_df.columns.difference(shared_categories))]
        except Exception as e:
            breakpoint()
            raise e

        # Convert columns to string type
        results_df.columns = results_df.columns.astype(str)
        
        ###TEST### DATA CENTRES
        # layout_df = merging_functions.insert_data_centres_into_layout_df(layout_df, results_df,shared_categories)
        ###TEST### DATA CENTRES      
        
        #Keep columns from outlook_base_year to outlook_last_year only
        results_df.drop(columns=[str(col) for col in results_df.columns if any(str(year) in col for year in range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1))], inplace=True)
        
        #filter for only economies in the layout file:
        results_df = results_df[results_df['economy'].isin(economies)]
        #drop where all vlaues in a row are 0. these arent needed as the rows will come from the layout file
        results_df = merging_functions.remove_all_zeros(results_df, years_to_keep_in_results)
        
        #convert all columns in sectors	sub1sectors	sub2sectors	sub3sectors	sub4sectors	fuels	subfuels to lowercase. there should be no uppercase in these columns )sometimes a modeller will put in a sector name in uppercase, but we will convert it to lowercase to match the layout file)
        for column in ['sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels']:
            results_df[column] = results_df[column].str.lower()
                
        #TEMP# 
        #buildings file currently includes all sectors historical data, just like the layout file, not just buildings. This is a temporary fix to remove the non-buildings sectors from the results file.
        results_df = results_adjustment_functions.filter_for_only_buildings_data_in_buildings_file(results_df)
        results_df = results_adjustment_functions.filter_out_solar_with_zeros_in_buildings_file(results_df)
        results_df = results_adjustment_functions.power_move_x_in_chp_and_hp_to_biomass(results_df)
        results_df, layout_df = results_adjustment_functions.allocate_problematic_x_rows_to_unallocated(results_df, layout_df,years_to_keep_in_results)
        results_df = results_adjustment_functions.edit_hydrogen_transfomation_rows(results_df)
        results_df = results_adjustment_functions.set_subfuel_for_supply_data(results_df)
        results_df = results_adjustment_functions.nullify_supply_stock_changes(results_df, PLOTTING=True)
        # results_df = results_adjustment_functions.create_transformation_losses_pipeline_rows_for_gas_based_on_supply(results_df, layout_df, SINGLE_ECONOMY_ID, shared_categories, years_to_keep_in_results)
        # breakpoint()#check the gas resutls
        try:
            results_df = results_adjustment_functions.consolidate_imports_exports_from_supply_sector(results_df,economy=SINGLE_ECONOMY_ID)
        except:
            breakpoint()
            results_df = results_adjustment_functions.consolidate_imports_exports_from_supply_sector(results_df,economy=SINGLE_ECONOMY_ID)
        ########
        #TEMP#
        
        # find sectors where there are null values for all the years base_year->end_year. This will help to identify where perhaps the results file is missing data or has been incorrectly formatted.
        null_sectors = results_df.loc[results_df.isnull().all(axis=1), 'sectors'].unique().tolist()#[years_to_keep]
        if null_sectors:
            print(f"Full rows of null values found in {file} for the following sectors:")
            print(null_sectors)
        filtered_results_df = results_df[~results_df['sectors'].isin(null_sectors)].copy()
        
        if results_df.empty or filtered_results_df.empty:
            continue
        #########RUN COMMON CHECKS ON THE RESULTS FILE.#########
        files_with_subtotals_that_we_can_just_drop = ['CHL_Agriculture and Fishing_V1_tgt_2024_07_30.csv', 'CHL_Agriculture and Fishing_V1_ref_2024_07_30.csv', 'MEX_Agriculture_V1_ref_2024_07_04','MEX_Agriculture_V1_tgt_2024_07_04', 'PE_Agriculture and Fishing_V1_ref_2024_07_08.csv' , 'PE_Agriculture and Fishing_V1_tgt_2024_07_08.csv', 'PNG_Agriculture_V1_tgt_2024_06_21.csv', 'PNG_Agriculture_V1_ref_2024_06_21.csv', 'HKC_Agriculture_V1_ref_2024_04_11.csv', 'HKC_Agriculture_V1_tgt_2024_04_11.csv', 'VN_Agriculture and Fishing_V1_ref_2024_04_11.csv', 'VN_Agriculture and Fishing_V1_tgt_2024_04_11.csv', 'CT_Agriculture and Fishing_V3_ref_2024_11_27.csv', 'CT_Agriculture and Fishing_V3_tgt_2024_11_27.csv', 'ROK_Agriculture and Fishing_V2_ref_2024_11_27.csv', 'ROK_Agriculture and Fishing_V2_tgt_2024_11_27.csv', 'PHL_Agriculture and Fishing_V5_ref_2024_12_16.csv', 'PHL_Agriculture and Fishing_V5_tgt_2024_12_16.csv', 'VN_Agriculture and Fishing_V2_ref_2024_12_05.csv', 'VN_Agriculture and Fishing_V2_tgt_2024_12_05.csv', 'AUS_Agriculture_V3_ref_2024_12_02.csv', 'AUS_Agriculture_V4_tgt_2024_01_21.csv', 'THA_Agriculture_V3_ref_2024_11_28.csv', 'THA_Agriculture_V3_tgt_2024_11_28.csv', 'MAS_Agriculture and Fishing_V2_ref_2025_01_27.csv', 'MAS_Agriculture and Fishing_V2_tgt_2025_01_27.csv', 'RUS_Agriculture and Fishing_V2_ref_2025_01_29.csv', 'RUS_Agriculture and Fishing_V2_tgt_2025_01_29.csv', 'INA_Agriculture_V3_ref_2025_01_27.csv', 'INA_Agriculture_V3_tgt_2025_01_27.csv', 'USA_Agriculture_V3_ref_2024_12_04.csv', 'USA_Agriculture_V3_tgt_2024_12_04.csv', 'PRC_Agriculture_V4_ref_2025_01_27.csv', 'PRC_Agriculture_V4_tgt_2025_01_27.csv', 'MEX_Agriculture_V2_ref_2025_01_27.csv', 'MEX_Agriculture_V2_tgt_2025_01_27.csv', 'CDA_Agriculture_V2_ref_2024_12_03.csv', 'CDA_Agriculture_V2_tgt_2024_12_03.csv','JPN_Agriculture and Fishing_V3_ref_2025_02_18.csv','JPN_Agriculture and Fishing_V3_tgt_2025_02_18.csv','PE_Agriculture and Fishing_V2_ref_2025_02_19.csv','PE_Agriculture and Fishing_V2_tgt_2025_02_19.csv','NZ_Agriculture and Fishing_V5_ref_2025_04_17.csv','NZ_Agriculture and Fishing_V5_tgt_2025_04_17.csv', 'PNG_Agriculture_V2_ref_2025_02_20.csv', 'PNG_Agriculture_V2_tgt_2025_02_20.csv', 'CHL_Agriculture and Fishing_V2_ref_2025_02_20.csv', 'CHL_Agriculture and Fishing_V2_tgt_2025_02_20.csv']#WARNING BE CARFEUL DOING THIS BECAUSE IF WE HAVE MULTIPLE SUBTOTAL COLS AND ONE IS TRUE AND THE OTHER IS FALSE, WE WOULD DROP THE ROW AND POTEINTIALLY LOSE DATA. NORMALLY JUST SAFER TO REMOVE THE SUBTOTALS MANUAULLY OR EVEN LEAVE THEM IN (WE ARENT TOTALLY SURE THIS IS SAFE THO).
        DROP_SUBTOTALS = False
        for file_ in files_with_subtotals_that_we_can_just_drop:
            if file_ in file:
                DROP_SUBTOTALS = True
                
        #if there are any subtotal columns, check none of them are true, cause not sure if we should keep them or not. and then remove the cols
        try:
            subtotal_cols = [col for col in filtered_results_df.columns if 'subtotal' in col]
        except:
            breakpoint()
            subtotal_cols = [col for col in filtered_results_df.columns if 'subtotal' in col]
        if len(subtotal_cols) > 0:
            if filtered_results_df[subtotal_cols].sum().sum() > 0:
                if DROP_SUBTOTALS:
                    for subtotal_col in subtotal_cols:
                        if subtotal_col in filtered_results_df.columns:
                            filtered_results_df = filtered_results_df.loc[filtered_results_df[subtotal_col]!=True].copy()
                else:
                    breakpoint()
                    raise ValueError(f"Subtotal columns found in {file}, not sure if we should keep them or not.")
            filtered_results_df.drop(columns=subtotal_cols, inplace=True)
            
        #check that the economy in economy col is the single_economy_id:
        if SINGLE_ECONOMY_ID in ALL_ECONOMY_IDS:
            #check that the economy in economy col is the single_economy_id:
            if results_df['economy'].nunique() > 1:
                breakpoint()
                raise ValueError(f"Not one economy in the results file {file}.")
            elif results_df['economy'].unique()[0] != SINGLE_ECONOMY_ID:
                breakpoint()
                raise ValueError(f"Economy in results file {file} is not the same as the single economy id {SINGLE_ECONOMY_ID}.")
            
        check_for_negatives_or_postives_in_wrong_sectors(filtered_results_df, SINGLE_ECONOMY_ID, file,INGORE_NANS=True)
           
        # try:
        merging_functions.check_for_differeces_between_layout_and_results_df(layout_df, filtered_results_df, shared_categories, file)
        # except Exception as e:
        #     breakpoint()
        #     merging_functions.check_for_differeces_between_layout_and_results_df(layout_df, filtered_results_df, shared_categories, file)
            
        #########RUN COMMON CHECKS ON THE RESULTS FILE OVER.#########
        basename = os.path.basename(file)
        filtered_results_df['origin'] = basename.split('.')[0]
        #the origin col is used because some data will come from two different results files, yet have the same sector and fuels columns but different levels of detail. This means that after we remove subtotals and then try to recreate them in calculate_subtotals, we might end up with duplicate rows. So we need to be able to identify that these rows came from different origin files so the duplicates can be removed by being summed together.
        filtered_results_df_subtotals_labelled = merging_functions.label_subtotals(filtered_results_df, shared_categories + ['origin'])
        # Combine the results_df with all the other results_dfs we have read so far
        
        if len([col for col in layout_df.columns if '2022' in col]) > 1:
            breakpoint()
        concatted_results_df = pd.concat([concatted_results_df, filtered_results_df_subtotals_labelled])
    # breakpoint()#want to adjust gas based on supply but first incroparte the direct use of lng ... haha
    #drop all subtotals from concatted_results_df
    
    if not USING_FINAL_RESULTS_DF:
        # breakpoint()#seems we lose the origin col comsethimes?
        concatted_results_df = concatted_results_df.loc[concatted_results_df['is_subtotal'] == False].copy()
        concatted_results_df = results_adjustment_functions.create_transformation_losses_pipeline_rows_for_gas_based_on_supply(concatted_results_df, layout_df, SINGLE_ECONOMY_ID, shared_categories, years_to_keep_in_results)
        # breakpoint()#origin col comsethimes?#are we missing the exports for china 
    # if SINGLE_ECONOMY_ID == '05_PRC':
    #     breakpoint()#check the gas works total from china demand vs supply and whether we need a discrepancy or not
    #make all cols strs
    concatted_results_df.columns = concatted_results_df.columns.astype(str)
    # Define the range of years to keep
    years_to_keep = set(range(EBT_EARLIEST_YEAR, OUTLOOK_LAST_YEAR + 1))
    year_cols = [col for col in concatted_results_df.columns if str(col).isdigit()]
    year_cols_to_drop = [col for col in year_cols if int(col) not in years_to_keep]
    concatted_results_df.drop(columns=year_cols_to_drop, inplace=True)
    
    #ONLY CALCUALTE SUBTOTALS ONCE WE HAVE CONCATTED ALL RESULTS TOGETHER, SO WE CAN GENERATE SUBTOTALS ACROSS RESUTLS. I.E. 09_total_transformation_sector
    
    #if there are two 2022s in the cols then breakpint
    if len([col for col in concatted_results_df.columns if '2022' in col]) > 1:
        breakpoint()
    if len([col for col in layout_df.columns if '2022' in col]) > 1:
        breakpoint()
    # breakpoint()#wats goign on with scenarios	economy	sectors	sub1sectors	sub2sectors	sub3sectors	sub4sectors	fuels	subfuels	subtotal_layout	subtotal_results	2021	2022

    # #target	06_HKC	14_industry_sector	x	x	x	x	07_petroleum_products	x	FALSE	TRUE	0	3.232906698
    # #this should be labeled as a subtotal in subtotal layout but it is not. check it
    # subtotal = concatted_results_df.loc[(concatted_results_df['sectors'] == '14_industry_sector') & (concatted_results_df['sub1sectors'] == 'x') & (concatted_results_df['fuels'] == '07_petroleum_products') & (concatted_results_df['subfuels'] == 'x')]
    #chck if the subtotal is true or false:
    # breakpoint()
    
    concatted_results_df = merging_functions.calculate_subtotals(concatted_results_df, shared_categories + ['origin'], DATAFRAME_ORIGIN='results')
    
    # breakpoint()#wats goign on with scenarios	economy	sectors	sub1sectors	sub2sectors	sub3sectors	sub4sectors	fuels	subfuels	subtotal_layout	subtotal_results	2021	2022

    # #target	06_HKC	14_industry_sector	x	x	x	x	07_petroleum_products	x	FALSE	TRUE	0	3.232906698
    # #this should be labeled as a subtotal in subtotal layout but it is not. check it
    # subtotal = concatted_results_df.loc[(concatted_results_df['sectors'] == '14_industry_sector') & (concatted_results_df['sub1sectors'] == 'x') & (concatted_results_df['fuels'] == '07_petroleum_products') & (concatted_results_df['subfuels'] == 'x')]
    #chck if the subtotal is true or false:
    
    concatted_results_df, layout_df = merging_functions.set_subfuel_x_rows_to_unallocated(concatted_results_df, layout_df)
    
    ##############################

    ###NOW WE HAVE THE concatted RESULTS DF, WITH SUBTOTALS CALCAULTED. WE NEED TO MERGE IT WITH THE LAYOUT FILE TO IDENTIFY ANY STRUCTURAL ISSUES####
    layout_df = layout_df[layout_df['economy'].isin(economies)].copy()
    #drop years in range(OUTLOOK_BASE_YEAR, OUTLOOK_BASE_YEAR+1) as we dont need it. This will help to speed up the process. 
    
    layout_df.drop(columns=[col for col in layout_df.columns if any(str(year) in str(col) for year in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1))], inplace=True)
    # breakpoint()#this is where issues pop up layout_df.loc[(layout_df['sectors'] == '14_industry_sector') & (layout_df['sub1sectors'] == 'x') & (layout_df['fuels'] == '07_petroleum_products') & (layout_df['subfuels'] == 'x')]
    # layout_df_subtotals_labelled = merging_functions.label_subtotals(layout_df, shared_categories) #now has been moved to C_subset_data.py
    layout_df_subtotals_recalculated = merging_functions.calculate_subtotals(layout_df, shared_categories, DATAFRAME_ORIGIN='layout')
    
    #chck if the subtotal is true or false:
    ############################## 
    
    trimmed_layout_df, missing_sectors_df = merging_functions.trim_layout_before_merging_with_results(layout_df_subtotals_recalculated,concatted_results_df)
    trimmed_concatted_results_df = merging_functions.trim_results_before_merging_with_layout(concatted_results_df, shared_categories)
    #rename subtotal columns before merging:
    trimmed_concatted_results_df.rename(columns={'is_subtotal': 'subtotal_results'}, inplace=True)
    trimmed_layout_df.rename(columns={'is_subtotal': 'subtotal_layout'}, inplace=True)
    missing_sectors_df.rename(columns={'is_subtotal': 'subtotal_layout'}, inplace=True)
    # Merge the new_layout_df with the concatted_results_df based on shared_categories using outer merge (so we can see which rows are missing/extra from the results_df)
    merged_df = pd.merge(trimmed_layout_df, trimmed_concatted_results_df, on=shared_categories, how="outer", indicator=True)

    results_layout_df = merging_functions.format_merged_layout_results_df(merged_df, shared_categories, trimmed_layout_df, trimmed_concatted_results_df,missing_sectors_df)
    
    #add subtotals to shared_categories now its in all the dfs
    shared_categories_w_subtotals = shared_categories + ['subtotal_layout', 'subtotal_results']
    #########################
    #NOW CALCAULTE THE AGGREGATES WHICH ARE COMBINATIONS OF SECTORS FROM DIFFERENT MODELLERS RESULTS. EVEN THOUGH THE LAYOUT DATA ALREADY CONTAINS THESE AGGREGATES, WE WILL RECALCULATE THEM AS A WAY OF TESTING THAT THE MERGES DONE UNTIL NOW ARE CORRECT. 
    #now, in case they are there, drop the aggregate sectors (except total transformation) from the merged data so we can recalculate them
    new_aggregate_sectors = ['12_total_final_consumption', '13_total_final_energy_consumption', '07_total_primary_energy_supply']
    results_layout_df = results_layout_df.loc[~results_layout_df['sectors'].isin(new_aggregate_sectors)].copy()
    #and drop aggregate fuels since we will recalculate them
    results_layout_df = results_layout_df.loc[~results_layout_df['fuels'].isin(['19_total', '21_modern_renewables', '20_total_renewables'])].copy()
    
    # results_layout_df.to_csv('results_layout_df_after_drop.csv')

    # Define a dictionary that maps each sector group to its corresponding total column
    sector_mappings = [
        (['14_industry_sector', '15_transport_sector', '16_other_sector', '17_nonenergy_use'], '12_total_final_consumption'),
        (['14_industry_sector', '15_transport_sector', '16_other_sector'], '13_total_final_energy_consumption'),
        # (['01_production', '09_total_transformation_sector'], '01_production')
        # (['01_production','02_imports', '03_exports', '06_stock_changes', '04_international_marine_bunkers','05_international_aviation_bunkers', '08_transfers',  '09_total_transformation_sector', '10_losses_and_own_use', '11_statistical_discrepancy', '14_industry_sector', '15_transport_sector', '16_other_sector', '17_nonenergy_use'], '07_total_primary_energy_supply') #'09_total_transformation_sector', #TODO IM NOT SURE IF IVE CALCUALTED TPES RIGHT
        #'10_losses_and_own_use', '11_statistical_discrepancy',#,
        # (['09_total_transformation_sector'], '09_total_transformation_sector')
    ]

    # Initialize an empty dictionary to store the resulting DataFrames
    sector_aggregates_df = pd.DataFrame()
    # Loop over the sector mappings and process the data for each sector group
    for (sectors, aggregate_sector) in sector_mappings:
        sector_df = merging_functions.calculate_sector_aggregates(results_layout_df, sectors, aggregate_sector, shared_categories, shared_categories_w_subtotals)
        sector_aggregates_df = pd.concat([sector_aggregates_df, sector_df])
    
    # Drop '01_production' from results_layout_df as it is included in sector_aggregates_df
    # results_layout_df = results_layout_df[results_layout_df['sectors'] != '01_production'].copy()
    
    # Add the new '01_production' back to results_layout_df
    # new_01_production = sector_aggregates_df[sector_aggregates_df['sectors'] == '01_production'].copy()
    # new_01_production.to_csv('data/temp/error_checking/new_01_production.csv')
    # results_layout_df.to_csv('data/temp/error_checking/results_layout_df.csv')
    # new_01_production = merging_functions.label_subtotals(new_01_production, shared_categories)
    # new_01_production.rename(columns={'is_subtotal': 'subtotal_layout'}, inplace=True)
    # new_01_production['subtotal_results'] = new_01_production['subtotal_layout']
    # results_layout_df = pd.concat([results_layout_df, new_01_production])
    
    # Calculate '07_total_primary_energy_supply' using updated results_layout_df
    new_sectors_for_tpes = ['01_production', '02_imports', '03_exports', '06_stock_changes', '04_international_marine_bunkers', '05_international_aviation_bunkers', '08_transfers', '09_total_transformation_sector', '10_losses_and_own_use', '11_statistical_discrepancy', '14_industry_sector', '15_transport_sector', '16_other_sector', '17_nonenergy_use']
    sector_df = merging_functions.calculate_sector_aggregates(results_layout_df, new_sectors_for_tpes, '07_total_primary_energy_supply', shared_categories, shared_categories_w_subtotals)
    sector_aggregates_df = pd.concat([sector_aggregates_df, sector_df])
    # Drop '01_production' again from results_layout_df to avoid duplication
    # results_layout_df = results_layout_df[results_layout_df['sectors'] != '01_production'].copy()
    
    # Calculate the subtotals in the sector aggregates
    sector_aggregates_df = merging_functions.calculating_subtotals_in_sector_aggregates(sector_aggregates_df, shared_categories_w_subtotals)
    
    # Label the subtotals in the sector aggregates
    sector_aggregates_df = merging_functions.label_subtotals(sector_aggregates_df, shared_categories)
    
    sector_aggregates_df.rename(columns={'is_subtotal': 'subtotal_layout'}, inplace=True)
    sector_aggregates_df['subtotal_results'] = sector_aggregates_df['subtotal_layout']
    
    # Ensure the index is consistent after concatenation if needed
    sector_aggregates_df.reset_index(drop=True, inplace=True)
    fuel_aggregates_df = merging_functions.calculate_fuel_aggregates(sector_aggregates_df, results_layout_df, shared_categories)
    # breakpoint()
    final_df = merging_functions.create_final_energy_df(sector_aggregates_df, fuel_aggregates_df,results_layout_df, shared_categories)
    
    #now check for issues with the new aggregates and subtotals by using the layout file as the reference    
    merging_functions.check_for_issues_by_comparing_to_layout_df(final_df, shared_categories_w_subtotals, new_aggregate_sectors, layout_df, REMOVE_LABELLED_SUBTOTALS=False)
    #######################################
    #FINALISE THE DATA
    
    #set up the order of columns to be shared_cateogires, subtotal_results, subtotal_layout, then the years in order
    final_df = final_df[shared_categories_w_subtotals +  [col for col in final_df.columns if col not in shared_categories_w_subtotals]]
    
    # final_df with rows ordered in the same sequence as layout_df based on the columns in shared_categories
    final_df = layout_df[shared_categories].merge(final_df, on=shared_categories, sort=False)
    
    #######################################
    merging_functions.save_merged_file(final_df, SINGLE_ECONOMY_ID, previous_merged_df_filename, shared_categories_w_subtotals, 
    folder_path=f'results/{SINGLE_ECONOMY_ID}/merged', old_folder_path=f'results/{SINGLE_ECONOMY_ID}/merged/old', COMPARE_TO_PREVIOUS_MERGE = True)
    
    return final_df


# #%%
# a = merging_results()
# a.to_csv('new.csv')
#%%




# %%

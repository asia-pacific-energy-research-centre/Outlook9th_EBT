#%%
import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from utility_functions import *
import warnings
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter  
import pickle
import plotly.express as px
                                    
def filter_for_only_buildings_data_in_buildings_file(results_df):
    #this is only because the buildings file is being given to us with all the other data in it. so we need to filter it to only have the buildings data in it so that nothing unexpected happens.
    #check for data in the end year where sub1sectors is 16_01_buildings
    
    
    if results_df.loc[results_df['sub1sectors'] == '16_01_buildings', str(OUTLOOK_BASE_YEAR+1):str(OUTLOOK_LAST_YEAR)].notnull().any().any():
        cols = [str(i) for i in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1)]
        #set 0's to null for now, as we dont mind if there are 0's in the buildings file. we just dont want any other data.
        bad_rows = results_df.copy()
        bad_rows[cols] = bad_rows[cols].replace(0, np.nan)
        #search for any rows where there is non na or 0 data in the years we are interested in, and where sub1sectors is not 16_01_buildings
        bad_rows = bad_rows.loc[(bad_rows[cols].notnull().any(axis=1)) & (bad_rows['sub1sectors'] != '16_01_buildings')].copy()
        if bad_rows.shape[0] > 0:
            print(bad_rows)            
            breakpoint()
            raise Exception("There is data in the buildings file that is not in the buildings sector. This is unexpected. Please check the buildings file.")
            
        results_df = results_df[results_df['sub1sectors'] == '16_01_buildings'].copy()
    return results_df


def filter_out_solar_with_zeros_in_buildings_file(results_df):
    """This is a temporary fix to remove solar data that contains only 0s from the buildings file, as it creates duplicates which arent actually duplicates in calculate_subtotals.
    
    Specificlaly the issue is 0's where fuels=12_solar, sub1sectors =16_01_buildings , sub2sectors=x, subfuels=x """
    years = [str(col) for col in results_df.columns if any(str(year) in col for year in range(OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR+1))]
    #filter out solar data from buildings file:
    results_df = results_df[~((results_df['fuels']=='12_solar') & (results_df['sub1sectors']=='16_01_buildings')&(results_df['sub2sectors']=='x')&(results_df['subfuels']=='x')&(results_df[years]==0).all(axis=1))].copy()
    #OK THERE IS ALSO A PROBLEM WHERE SUBFUELS = 12_x_other_solar... SO DO THE SAME FOR THAT TOO...
    results_df = results_df[~((results_df['subfuels']=='12_x_other_solar') & (results_df['sub1sectors']=='16_01_buildings')&(results_df['sub2sectors']=='x')&(results_df['fuels']=='12_solar')&(results_df[years]==0).all(axis=1))].copy()
    #ugh ok and also, if they are nas, then we should also drop them...
    results_df = results_df[~((results_df['subfuels']=='12_x_other_solar') & (results_df['sub1sectors']=='16_01_buildings')&(results_df['sub2sectors']=='x')&(results_df['fuels']=='12_solar')&(results_df[years].isna().all(axis=1)))].copy()
    results_df = results_df[~((results_df['fuels']=='12_solar') & (results_df['sub1sectors']=='16_01_buildings')&(results_df['sub2sectors']=='x')&(results_df['subfuels']=='x')&(results_df[years].isna().all(axis=1)))].copy()
    return results_df   

def set_subfuel_for_supply_data(results_df):
    #set stock changes in supply of oil to be for 06_01_crude_oil. That is, where fuel is 06_crude_oil_and_ngl and subfuel is x, set the subfuel to 06_01_crude_oil. But only do this where sectors is 06_stock_changes
    #this is because otherwise we end up with stock changes in crude po; unaclloatied which may have no other data in it. this would then cause a supply inbalance in the data. better ti just set it to crude oil and ngl and assume that the stock changes are for crude oil and ngl.
    if results_df.loc[(results_df['fuels'] == '06_crude_oil_and_ngl') & (results_df['subfuels'] == 'x') & (results_df['sectors'] == '06_stock_changes')].shape[0] > 0:
        results_df.loc[(results_df['fuels'] == '06_crude_oil_and_ngl') & (results_df['subfuels'] == 'x') & (results_df['sectors'] == '06_stock_changes'), 'subfuels'] = '06_01_crude_oil'
    #and set subfuel for 08_Gas production and stock changes to be for 08_01_natural_gas
    if results_df.loc[(results_df['fuels'] == '08_gas') & (results_df['subfuels'] == 'x') & (results_df['sectors'] == '06_stock_changes')].shape[0] > 0:
        results_df.loc[(results_df['fuels'] == '08_gas') & (results_df['subfuels'] == 'x') & (results_df['sectors'] == '06_stock_changes'), 'subfuels'] = '08_01_natural_gas'
    #and set subfuel for 08_Gas production and stock changes to be for 08_01_natural_gas
    if results_df.loc[(results_df['fuels'] == '08_gas') & (results_df['subfuels'] == 'x') & (results_df['sectors'] == '01_production')].shape[0] > 0:
        results_df.loc[(results_df['fuels'] == '08_gas') & (results_df['subfuels'] == 'x') & (results_df['sectors'] == '01_production'), 'subfuels'] = '08_01_natural_gas'
    
    return results_df

def consolidate_imports_exports_from_supply_sector(results_df, economy, PLOTTING=True):
    """
    Replace situations where both imports and exports are nonzero with a consolidated (net imports) row.
    
    Dont do this for some economies where they export and import the same fuel a lot, like in usa. this is useful data comapred to where the majority of values are exported or imported and the counterpart is really small (and therefore distracting)
    For each group identified by the key columns (economy, scenarios, fuels, subfuels):
      - If both an import row ('02_imports') and an export row ('03_exports') are present,
        compute, for each year, the net value as:
            net = imports (positive) + exports (assumed negative)
      - Then, if the net value is nonnegative, set the import row to the net value and the export row to 0;
        if net is negative, assign the net value to the export row (so that the value remains negative) and set imports to 0.
      - Finally, drop one of the two rows so that only one consolidated row remains.
    
    Parameters:
        df: DataFrame containing gas supply data.
        year_cols: List of columns (as strings) that correspond to projection years.
    
    Returns:
        A DataFrame where for each key group (economy, scenarios, fuels, subfuels)
        only one row is nonzero—representing net imports.
    """
    df_copy = results_df.copy()
    keys = ['economy', 'scenarios', 'fuels', 'subfuels']
    year_cols = [col for col in results_df.columns if re.match(r'^\d{4}$', str(col))]
    future_years = [str(col) for col in results_df.columns if any(str(year) in col for year in range(OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR+1))]
    
    #################
    supply_fuels = ['01_coal', '06_crude_oil_and_ngl', '08_gas']
    supply_sectors = ['01_production', '02_imports', '03_exports']
    ECONOMIES_TO_ADJUST = []#['05_PRC', '20_USA', '01_AUS', '07_INA', '14_PE', '11_MEX', '10_MAS', '02_BD']
    # Verify that almost every fuel/sector combination is present in the DataFrame
    all_found = True
    FOUND=False
    found_number = 0#post hoc change. found that since we wrre removing rows of all 0s sometiemes, we would lose some rows we wanted.
    supply_results_df = pd.DataFrame()
    for fuel in supply_fuels:
        for sector in supply_sectors:
            if results_df[(results_df['sectors'] == sector) & (results_df['fuels'] == fuel)].empty:
                all_found = False
                break  # Break out of inner loop if a combination is missing
            else:
                supply_results_df = pd.concat([supply_results_df, results_df[(results_df['sectors'] == sector) & (results_df['fuels'] == fuel)]])
                found_number += 1
        # if not all_found:
        #     break  # Break out of outer loop if a combination is missing
    if found_number > 3:
        all_found = True
    # if economy in ECONOMIES_TO_IGNORE:
    #     return results_df
    if all_found:
        #double check that when you sum up the values for OUTLOOK_BASE_YEAR+1, they are not all 0
        #make all cols strs 
        supply_results_df.columns = supply_results_df.columns.astype(str)
        if supply_results_df[str(OUTLOOK_BASE_YEAR + 1)].sum() == 0:
            breakpoint()
            raise ValueError(f"Supply data seems to be available but all values for {OUTLOOK_BASE_YEAR + 1} are 0. We dont want to be receiving data with supply values but which are all 0s, whether it is from supply modellers or another model.")
        FOUND = True
    #################
    # At this point, FOUND is True if at least one file has all combinations.
    if FOUND:
        # breakpoint()
        # breakpoint()#do we need to include subtotals in the keys?
        # Separate imports and exports
        imp_results_df = results_df.loc[results_df['sectors'] == '02_imports', keys + future_years].copy()
        exp_results_df = results_df.loc[results_df['sectors'] == '03_exports', keys + future_years].copy()
        subtotal_cols = [col for col in results_df.columns if 'subtotal' in col]
        if len(subtotal_cols) >0:
            breakpoint()
            raise Exception("Subtotal columns found, check the data.")
        # Merge on key columns; the suffixes _imp and _exp will allow us to compute net year by year
        merged = pd.merge(imp_results_df, exp_results_df, on=keys, how='outer', suffixes=('_imp', '_exp'))
        
        # For each year calculate the net value (assuming exports are negative already)
        for year in future_years:
            merged[year] = merged[f"{year}_imp"].fillna(0) + merged[f"{year}_exp"].fillna(0)
        #remove the _imp and exp columns then melt the df
        merged = merged.drop(columns=[f"{year}_imp" for year in future_years] + [f"{year}_exp" for year in future_years])
        melted_df = merged.melt(id_vars=keys, value_vars=future_years, var_name='year', value_name='value')
        
        # Decide on which sector to assign: if value >= 0, assign to imports; if net < 0, assign to exports. Then pivot and set nas to 0
        melted_df['sectors'] = melted_df.apply(lambda row: '02_imports' if row['value'] >= 0 else '03_exports', axis=1) 
        wide_df = melted_df.pivot(index=keys + ['sectors'], columns='year', values='value').reset_index()  
        #create separate sectors cols filled with xs for sub1sectors	sub2sectors	sub3sectors	sub4sectors
        wide_df['sub1sectors'] = 'x'
        wide_df['sub2sectors'] = 'x'
        wide_df['sub3sectors'] = 'x'
        wide_df['sub4sectors'] = 'x'
        
        # Retain only key columns, the computed net year columns, and the new sector column.
        # try:
        consolidated = wide_df[keys  + ['sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors'] + future_years]
        # except:
        #     breakpoint()
        #     consolidated = wide_df[keys  + ['sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors'] + future_years]
        consolidated[future_years] = consolidated[future_years].replace(np.nan, 0)
        
        # If desired, you may remove the original two rows from results_df and replace them with the consolidated rows.
        results_df = results_df[~results_df['sectors'].isin(['02_imports', '03_exports'])].copy()
        results_df = pd.concat([results_df, consolidated], ignore_index=True)
        #plot both the old and new imports/exports:
        if PLOTTING:
            #first get them looking the same
            imp_results_df['sectors'] = '02_imports'
            exp_results_df['sectors'] = '03_exports'
            old = pd.concat([imp_results_df, exp_results_df])
            old['dataset'] = 'original'
            new = consolidated.copy()
            new['dataset'] = 'new'
            merged_plotting = pd.concat([old, new])
            #melt it:
            
            merged_plotting = merged_plotting.melt(id_vars=['sectors','scenarios','fuels', 'subfuels', 'dataset'], value_vars=future_years, var_name='year', value_name='value')
            #join fuels and subfuels
            
            merged_plotting['subfuel_fuels_sectors'] = merged_plotting['fuels'].astype(str) + '_' + merged_plotting['subfuels'].astype(str) + '_' + merged_plotting['sectors'].astype(str)
            # breakpoint()#check mas. graph looking wierd
            merged_plotting['year'] = merged_plotting['year'].astype(int)
            for fuel in merged_plotting['fuels'].unique():
                
                fuel_data = merged_plotting[merged_plotting['fuels'] == fuel]
                fuel_data['value'] = fuel_data['value'].replace(np.nan, 0)
                import plotly.express as px
                fig = px.line(fuel_data, x='year', y='value', color='subfuel_fuels_sectors', line_dash='dataset',facet_col='scenarios', title='Imports and Exports Comparison')
                #write to html
                economy= results_df.economy.unique()[0]
                if economy not in ECONOMIES_TO_ADJUST:
                    fig.write_html(f'./plotting_output/import_export_consolidations/NOT_USED_{economy}_{fuel}_supply_consolidated_imports_exports_comparison.html')
                else:
                    fig.write_html(f'./plotting_output/import_export_consolidations/{economy}_{fuel}_supply_consolidated_imports_exports_comparison.html')
        
        # breakpoint()#check its ok#esp with this  df_melted['year'] = df_melted['year'].astype(int)
        # breakpoint()
    else:
        return df_copy
    if economy not in ECONOMIES_TO_ADJUST:#we still create the plot jsut to allow us to check if it is useful later
        # breakpoint()#wats going on with usa exports ofcurde
        return df_copy
    # If not, return the modified DataFrame
    return results_df

# Example usage:
# Suppose df_gas contains the gas supply rows for both imports and exports
# and year_cols is something like ['2022', '2023', '2024'].
#
# consolidated_df = consolidate_imports_exports(df_gas, ['2022','2023','2024'])

def nullify_supply_stock_changes(results_df, PLOTTING=True):
    #stock changes for coal gas and crude are a bit unneccesasry for our projetions and their charts. so we will add them to production where they are positive and to imports where they are negative
    year_cols = [col for col in results_df.columns if re.match(r'^\d{4}$', str(col))]
    future_years = [str(col) for col in results_df.columns if any(str(year) in col for year in range(OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR+1))]
    
    #we will frist itdentify if this si the supply data by searching for stock changes in eitehr coal, curdd or gas in future years. if there is then melt the data so we have a single year column. then separate stock changes. then change the sector to production where it is positive and to imports where it is negative. then merge back to the original data and addthe new values col to the old values col where not na. lastly drop that new colmmn and drop the stock changes rows
    #and return the data.
    
    melted_df = results_df.melt(id_vars=[col for col in results_df.columns if col not in year_cols], value_vars=year_cols, var_name='year', value_name='value')
    melted_df_proj_years = melted_df[melted_df['year'].isin(future_years)].copy()
    
    if (melted_df_proj_years.loc[(melted_df_proj_years['fuels'] == '06_crude_oil_and_ngl') & (melted_df_proj_years['sectors'] == '06_stock_changes') & (melted_df_proj_years['value'] != 0)].shape[0] > 0) or (melted_df_proj_years.loc[(melted_df_proj_years['fuels'] == '08_gas') & (melted_df_proj_years['sectors'] == '06_stock_changes') &  (melted_df_proj_years['value'] != 0)].shape[0] > 0) or (melted_df_proj_years.loc[(melted_df_proj_years['fuels'] == '01_coal') & (melted_df_proj_years['sectors'] == '06_stock_changes') & (melted_df_proj_years['value'] != 0)].shape[0] > 0):
        # breakpoint()
        #remove rows where subfuel is x. these are subtotals in the supply data and since we will sum them up later, we dont need them here. they are quite difficult to calc stock changes for
        results_df = results_df[~((results_df['subfuels'] == 'x'))].copy()
        
        #melt the data so we have a single year column
        melted_df = results_df.melt(id_vars=[col for col in results_df.columns if col not in year_cols], value_vars=year_cols, var_name='year', value_name='value')
        melted_df_copy = melted_df.copy()
        #separate stock changes
        stock_changes = melted_df[melted_df['sectors'] == '06_stock_changes'].copy()
        #change the sector to production where it is positive and to imports where it is negative
        stock_changes.loc[stock_changes['value'] > 0, 'sectors'] = '01_production'
        stock_changes.loc[stock_changes['value'] < 0, 'sectors'] = '02_imports'
        #merge back to the original data
        melted_df = pd.merge(melted_df, stock_changes, on=[col for col in melted_df.columns if col != 'value'], how='outer', suffixes=('', '_y'), indicator=True)
        # breakpoint()
        #inspect lefts and rights
        # right_left_merges = melted_df[melted_df['_merge'] != 'both'].copy()
        # if len(right_left_merges) > 0:
        #     breakpoint()
        #     print("There are some rows that are not in both the left and right dataframes. This is unexpected.")
        #add the new values col to the old values col where not na
        melted_df.loc[melted_df['value_y'].notna(), 'value'] += melted_df.loc[melted_df['value_y'].notna(), 'value_y']
        #drop the new values col and set the stock changes rows to 0
        melted_df.drop(columns=['value_y', '_merge'], inplace=True)
        melted_df.loc[melted_df['sectors'] == '06_stock_changes', 'value'] = 0
        #pvot back to wide
        #checl fpr duplicates first:
        duplicates = melted_df[melted_df.duplicated(subset=[col for col in melted_df.columns if col not in [ 'value']], keep=False)].copy()
        if len(duplicates) > 0:
            breakpoint()
            raise Exception("There are duplicates in the melted dataframe. This is unexpected.")
        new_results_df = melted_df.pivot(index=[col for col in melted_df.columns if col not in ['year', 'value']], columns='year', values='value').reset_index()
        
        #check the reslts by plotting imports exports and production lines using plotly exporess:
        # breakpoint()
        if PLOTTING:
            melted_df_copy['dataset'] = 'original'
            melted_df['dataset'] = 'new'
            melted_df = pd.concat([melted_df_copy, melted_df], ignore_index=True)
            melted_df['year'] = melted_df['year'].astype(int)
            #add sub fuels and fuels to be in same col
            melted_df['fuels'] = melted_df['fuels'].astype(str) + '_' + melted_df['subfuels'].astype(str)
            import plotly.express as px
            fig = px.line(melted_df, x='year', y='value', color='sectors', line_dash='dataset', facet_col='fuels',facet_row='scenarios', title='Stock Changes vs Production vs Imports')
            #save to plotting_output
            if os.path.exists('./plotting_output') == False:
                breakpoint()
                raise Exception("The plotting_output folder does not exist. Please create it.")
            # breakpoint()
            economy= melted_df.economy.unique()[0]
            fig.write_html(f'./plotting_output/stock_changes_vs_production_vs_imports/stock_changes_vs_production_vs_imports_{economy}.html')
            #still need to change layout df later.
            #layout_df = pd.concat([layout_df, new_results_df], ignore_index=True)
        return new_results_df
    else:
        return results_df
            
def power_move_x_in_chp_and_hp_to_biomass(results_df):
    # Anything that has sub1sectors in 18_02_chp_plants, 09_02_chp_plants, 09_x_heat_plants and the sub2sectors col is 'x' should be moved to another sector in same level. we will state that in a dict below:
    corresp_sectors_dict = {}
    corresp_sectors_dict['18_02_chp_plants'] = '18_02_04_biomass'
    corresp_sectors_dict['09_02_chp_plants'] = '09_02_04_biomass'
    corresp_sectors_dict['09_x_heat_plants'] = '09_x_04_biomass'
    corresp_sectors_dict['19_01_chp_plants'] = '19_01_04_biomass'
    
    # List of sub2sectors values to check
    values_to_check = ['x', '19_01_05_others']
    
    for key, value in corresp_sectors_dict.items():
        # Get the rows where sub1sectors is the key and sub2sectors is one of the specified values
        rows_to_change = results_df.loc[(results_df['sub1sectors'] == key) & (results_df['sub2sectors'].isin(values_to_check))].copy()
        # Remove these rows from the original dataframe
        results_df = results_df.loc[~((results_df['sub1sectors'] == key) & (results_df['sub2sectors'].isin(values_to_check)))].copy()
        # Change the sub1sectors to the corresponding value
        rows_to_change['sub2sectors'] = value
        # Append the modified rows back to the results_df
        results_df = pd.concat([results_df, rows_to_change])
    return results_df


def allocate_problematic_x_rows_to_unallocated(results_df, layout_df, years_to_keep_in_results):
        
    #if tehre are rows in the dataframe where the fuel is the key in subfuels_to_solve and the subfuel is in the values in subfuels_to_solve as well as rows where the subfuel for that same fuel is x, then we need to set the rows wehre subfuels is x to '##_unallocated' now so that it doesnt confse the subtotalling funcitons later. 
    sectors_with_issues = ['18_electricity_output_in_gwh', '09_total_transformation_sector', '19_heat_output_in_pj'] #try to be as specfic as possible here to reduce chance of errors
    subfuels_to_solve = {'16_others':['16_x_efuel', '16_x_ammonia', '16_x_hydrogen']}
    new_rows = pd.DataFrame()
    results_df_copy_ = results_df.copy()
    for scenario in results_df_copy_['scenarios'].unique():
        results_df_copy = results_df_copy_[results_df_copy_['scenarios'] == scenario].copy()
        for sector_col in ['sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors']:
            for sector in sectors_with_issues:
                if any(results_df_copy[sector_col]==sector):
                    # breakpoint()
                    #grab that row and check for the subfuel x rows
                    df = results_df_copy[results_df_copy[sector_col]==sector].copy()
                    for fuel, subfuel_list in subfuels_to_solve.items():
                        if any(df['fuels'] == fuel):
                            df_fuel = df[df['fuels'] == fuel].copy()
                            if any(df_fuel['subfuels']=='x') and any(df_fuel['subfuels'].isin(subfuel_list)):
                                ###########REALISED THAT CHECKING FOR SUBTOTALS HERE WAS NOT GOING TO WORK UNLESS IT WAS A LOT MORE COMPREHENSIVE I.E. CHECKING FOR LOWER LEVEL SUBTOTALS FIRST WOULD BE REQUIRED). so instead we will just hope nothign goes wrong!
                                ##############################
                                # breakpoint()#since we already have unallocated in our data it shouldnt be appearing here no?
                                # #check that the row with subfuel x is not a subtotal (ignorign the subottal column since modellers sometimes ignore it)
                                # fuel_sum = df_fuel[(df_fuel['subfuels']!='x')][years_to_keep_in_results].sum().sum()
                                # x_subfuel_sum = df_fuel[df_fuel['subfuels'] == 'x'][years_to_keep_in_results].sum().sum()
                                # if fuel_sum == 0:
                                #     continue
                                # if abs(fuel_sum - x_subfuel_sum) < 0.001*fuel_sum:
                                #     continue#we have a subtotal so we dont need to do anything
                                #if we dont have a subtotal then we need to set the subfuel x row to unallocated and then we can continue to the next fuel
                                #set the subfuel x row to unallocated
                                results_df.loc[(results_df['subfuels'] == 'x') & (results_df['fuels'] == fuel) & (results_df[sector_col]==sector) & (results_df['scenarios'] == scenario), 'subfuels'] = f'{fuel}_unallocated'
                                new_rows = pd.concat([new_rows, results_df.loc[(results_df['subfuels'] == f'{fuel}_unallocated') & (results_df['fuels'] == fuel) & (results_df[sector_col]==sector) & (results_df['scenarios'] == scenario)]], ignore_index=True)
                                #now onto the next sector
    
    if len(new_rows) == 0:
        return results_df, layout_df
    # breakpoint()#is it actually creating them for hcina?
    #add 'is_subtotal' = false to the row:
    new_rows['is_subtotal'] = False
    #double check these new rows are in the layout_df - they shuld be if thy are added in the config, but sometimes they may not be:
    layout_df_years =[str(year) for year in range(EBT_EARLIEST_YEAR, OUTLOOK_LAST_YEAR+1)]
    similar_cols = [col for col in layout_df.columns if col in new_rows.columns]
    similar_cols_non_years = [col for col in similar_cols if col not in layout_df_years]
    missing_cols = [col for col in layout_df.columns if col not in new_rows.columns]
    
    # if any of missing cols are not in layout_df_years then throw an error else, fill all year cols with 0s
    if len([col for col in missing_cols if col not in layout_df_years]) > 0:
        breakpoint()
        raise Exception("hmm there are some missing columns in the layout_df that are not in the results_df. This is unexpected.")
    
    new_rows[layout_df_years] = 0
    #identify rows that are not already in the layout_df
    new_rows = new_rows[layout_df.columns].copy()
    merged_df = new_rows.merge(layout_df[similar_cols_non_years], on=similar_cols_non_years, how='left', indicator=True, suffixes=('', '_layout'))
    
    #drop rows that are already in the layout_df and cols with _layout in them
    merged_df = merged_df[merged_df['_merge'] == 'left_only'].copy()
    merged_df.drop(columns=['_merge'], inplace=True)
    #if tehre are any cols with _layout, throw an error
    if len([col for col in merged_df.columns if '_layout' in str(col)]) > 0:
        breakpoint()
        raise Exception("There are columns with '_layout' in them. This is unexpected.")
    #concat these new rows to the layout_df
    new_layout_df = pd.concat([layout_df, merged_df[layout_df.columns]], ignore_index=True)  
    #if there are no new rows then return the original dfs
    if merged_df[layout_df.columns].shape[0] == 0:
        return results_df, layout_df
    return results_df, new_layout_df    
                            
                         
    
def edit_hydrogen_transfomation_rows(results_df):
    #first double check this is the hydrogen rows by searchfing for 09_13_hydrogen_transformation in sub1sectors, if not, return the df as is, esle we can update the values in the df without worrying that we are changing the wrong rows.
    hydrogen_rows = results_df[(results_df['sub1sectors'] == '09_13_hydrogen_transformation')].copy()
    if len(hydrogen_rows) == 0:
        return results_df
    #the hyrdogen transformtion data was accidrntlaly recorded for some economies as negatives when it should be positive and vice versa. so manully check that 17_x_green_electricity and subfuels=08_01_natural_gas in 09_total_transfomation_sector is negative
    
    #also so that things balance out and there is a valye to be accessed by TPES and it is easier to use these rows, copy the 17_x_green_electricity row and put it in 01_production	x	x	x	x	17_x_green_electricity. this will be as the positive version of the row.
    
    years = [str(col) for col in results_df.columns if any(str(year) in col for year in range(OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR+1))]
    
    #check for where 17_x_green_electricity and subfuels=08_01_natural_gas in 09_total_transfomation_sector is postivie since it should be negative
    rows_to_change = results_df[(results_df['sectors'] == '09_total_transformation_sector') & ((results_df['fuels'] == '17_x_green_electricity') | (results_df['fuels'] == '08_gas'))].copy()
    if rows_to_change.shape[0] > 0:
        
        #change the values to negative if they are postive:
        for year in years:
            rows_to_change[year] = rows_to_change[year].apply(lambda x: x * -1 if x > 0 else x)
            
    #also where there are rows for 08_gas and subfuels=x, we will set the subfuels to 08_01_natural_gassince we know hydrogen sector is using natural gas
    results_df.loc[(results_df['fuels'] == '08_gas') & (results_df['subfuels'] == 'x'), 'subfuels'] = '08_01_natural_gas'
    #then create 17_x_green_electricity production rows:
    new_rows = results_df[(results_df['sectors'] == '09_total_transformation_sector') & (results_df['fuels'] == '17_x_green_electricity') & (results_df['subfuels'] == 'x')].copy()
    new_rows['sectors'] = '01_production'
    new_rows['sub1sectors'] = 'x'
    new_rows['sub2sectors'] = 'x'
    new_rows['sub3sectors'] = 'x'
    new_rows['sub4sectors'] = 'x'
    new_rows.loc[:, years] = new_rows.loc[:, years] * -1
    results_df = pd.concat([results_df, new_rows], ignore_index=True)
    
    return results_df
    
    
################################################
#GAS SUPPLY FUNCTIONS BELOW
################################################
    
def create_transformation_losses_pipeline_rows_for_gas_based_on_supply(results_df, layout_df, SINGLE_ECONOMY_ID, shared_categories, years_to_keep_in_results, PLOT=True):
    """
    Create additional rows for gas supply based on LNG transformation losses and pipeline energy use.
    
    Steps:
      1. Load and merge base-year layout data with the gas supply.
      2. Ensure complete gas trade data exists (imports/exports for both LNG and natural gas).
      3. Estimate losses for LNG transformation by calling 'estimate_losses_for_lng_processes'.
      4. Generate LNG-to-gas transformation rows using the function 'create_lng_transformation_rows'.
      5. Estimate pipeline energy use by calling 'estimate_pipeline_energy_use_based_on_gas_imports_production'.
      6. Concatenate all rows (supply, transformation, losses, and pipeline energy use) and return the result.
    """
    # # Save inputs as pickle files
    # with open('results_df.pkl', 'wb') as f:
    #     pickle.dump(results_df, f)
    # with open('layout_df.pkl', 'wb') as f:
    #     pickle.dump(layout_df, f)
    # with open('SINGLE_ECONOMY_ID.pkl', 'wb') as f:
    #     pickle.dump(SINGLE_ECONOMY_ID, f)
    # with open('shared_categories.pkl', 'wb') as f:
    #     pickle.dump(shared_categories, f)
    # with open('years_to_keep_in_results.pkl', 'wb') as f:
    #     pickle.dump(years_to_keep_in_results, f)
    str_OUTLOOK_BASE_YEAR = str(OUTLOOK_BASE_YEAR)
    years_to_keep_in_results_str = [str(year) for year in years_to_keep_in_results]
    # --- Verify that only one economy is present ---
    if len(results_df['economy'].unique()) > 1:
        breakpoint()
        raise Exception('There is more than one economy in the gas_supply data. This is unexpected and should be rewritten for that.')

    # --- Load and prepare base-year layout data ---
    
    all_economies_layout_df = find_most_recent_file_date_id('results/', filename_part='model_df_wide_tgt', RETURN_DATE_ID=False)
    all_economies_layout_df = pd.read_csv(os.path.join('results/', all_economies_layout_df))
    #drop where is_subtotal is true
    all_economies_layout_df = all_economies_layout_df.loc[all_economies_layout_df['is_subtotal'] == False].copy()
    #drop fuels = 19_total, 21_modern_renewables and 20_total_renewables
    all_economies_layout_df = all_economies_layout_df.loc[~all_economies_layout_df['fuels'].isin(['19_total', '21_modern_renewables', '20_total_renewables'])].copy()
    layout_df = layout_df.loc[~layout_df['fuels'].isin(['19_total', '21_modern_renewables', '20_total_renewables'])].copy()
    #convert all year col names to strs
    all_economies_layout_df.columns = [str(col) for col in all_economies_layout_df.columns]
    all_economies_layout_df = all_economies_layout_df[[str_OUTLOOK_BASE_YEAR] + shared_categories].copy()
    
    # Duplicate layout data for the 'reference' scenario
    all_economies_layout_df_copy = all_economies_layout_df.copy()
    all_economies_layout_df_copy['scenarios'] = 'reference'
    all_economies_layout_df = pd.concat([all_economies_layout_df, all_economies_layout_df_copy], ignore_index=True)
    results_df.columns = [str(col) for col in results_df.columns]
    layout_df.columns = [str(col) for col in layout_df.columns]
    # --- Filter gas supply data (only for fuels '08_gas' with sub1sectors marker 'x') ---
    gas_supply = results_df[(results_df['fuels'] == '08_gas') & (results_df['sub1sectors'] == 'x')].copy()
    if len(gas_supply) == 0:
        return results_df 
    elif (gas_supply[(gas_supply['sectors'] == '02_imports') & (gas_supply['subfuels'] == '08_02_lng')][years_to_keep_in_results_str].sum().sum() == 0 and 
          gas_supply[(gas_supply['sectors'] == '03_exports') & (gas_supply['subfuels'] == '08_02_lng')][years_to_keep_in_results_str].sum().sum() == 0):
        return results_df
    else:
        # Ensure complete gas trade data: if any combination of sector and subfuel is missing, create a row with zeros.
        gas_trade_df = pd.DataFrame()
        for scenario in gas_supply['scenarios'].unique():
            for sector in ['02_imports', '03_exports']:
                for subfuel in ['08_02_lng', '08_01_natural_gas']:
                    if gas_supply[(gas_supply['sectors'] == sector) & (gas_supply['subfuels'] == subfuel) & (gas_supply['scenarios'] == scenario)].empty:
                        row = gas_supply.iloc[0:1].copy()
                        row['sectors'] = sector
                        row['subfuels'] = subfuel
                        row['scenarios'] = scenario
                        #make all year cols 0
                        row[years_to_keep_in_results_str] = 0
                        gas_trade_df = pd.concat([gas_trade_df, row], ignore_index=True)
                    else:
                        rows = gas_supply[(gas_supply['sectors'] == sector) & (gas_supply['subfuels'] == subfuel) & (gas_supply['scenarios'] == scenario)].copy()
                        gas_trade_df = pd.concat([gas_trade_df, rows], ignore_index=True)
        # Merge base-year layout data onto the gas trade data (merging on shared categories, excluding 'economy')
        shared_categories_no_econ = [col for col in shared_categories if col != 'economy']
        
        base_year_lng_and_gas_all_econs = gas_trade_df.merge(all_economies_layout_df, on=shared_categories_no_econ, how='inner', suffixes=('_x', ''))
        base_year_lng_and_gas_all_econs = base_year_lng_and_gas_all_econs[[str_OUTLOOK_BASE_YEAR] + shared_categories].copy()
        base_year_lng_and_gas_all_econs.fillna(0, inplace=True)

    # --- Isolate LNG rows from the gas trade data ---
    lng_supply = gas_trade_df[gas_trade_df['subfuels'] == '08_02_lng'].copy()

    ##################################################################
    
    # --- Estimate losses for LNG transformation ---
    # This function returns updated gas_supply and a DataFrame of LNG losses (for all projection years)
    lng_losses_projection, gas_trade_base_year = estimate_losses_for_lng_processes(lng_supply, layout_df, base_year_lng_and_gas_all_econs, all_economies_layout_df, gas_supply, str_OUTLOOK_BASE_YEAR, years_to_keep_in_results_str, SINGLE_ECONOMY_ID)
    
    gas_supply_minus_losses = subtract_losses_from_larger_source(gas_supply, lng_losses_projection,years_to_keep_in_results_str, SINGLE_ECONOMY_ID)
    #repalce data in layout df with data from gas_trade_base_year in case it was updated in the estimate_losses_for_lng_processes function above
    layout_df = layout_df.merge(gas_trade_base_year, on=['sectors', 'subfuels', 'scenarios', 'economy'], how='outer', suffixes=('', '_y'), indicator=True)
    #where there is data in gas_trade_base_year, replace the data in layout_df with that data
    layout_df[str_OUTLOOK_BASE_YEAR] = np.where(layout_df[str_OUTLOOK_BASE_YEAR+'_y'].notnull(), layout_df[str_OUTLOOK_BASE_YEAR+'_y'], layout_df[str_OUTLOOK_BASE_YEAR])
    layout_df.drop(columns=[str_OUTLOOK_BASE_YEAR+'_y', '_merge'], inplace=True)
        
    ##################################################################
    
    # --- Create LNG-to-gas transformation rows ---
    # Restrict gas supply to LNG rows from imports or exports
    lng_supply = gas_supply_minus_losses.loc[
        ((gas_supply_minus_losses['sectors'] == '02_imports') | (gas_supply_minus_losses['sectors'] == '03_exports')) & 
         (gas_supply_minus_losses['subfuels'] == '08_02_lng')
    ].copy()
    lng_to_gas_transfomation = create_lng_transformation_rows(lng_supply, years_to_keep_in_results_str)
    
    ##################################################################
    
    # --- Estimate pipeline energy use for gas ---
    gas_pipeline_energy_use = estimate_pipeline_energy_use_based_on_gas_imports_production(gas_supply_minus_losses, layout_df, shared_categories, years_to_keep_in_results_str, str_OUTLOOK_BASE_YEAR)
    # Note: Pipeline energy use is added separately since only gas supply data is available for this calculation.
    new_gas_supply = subtract_pipeline_adjustment_from_larger_source(gas_supply_minus_losses,gas_pipeline_energy_use, years_to_keep_in_results_str)
    ##################################################################
    
    # --- Combine all rows (gas supply, transformation, losses, and pipeline energy use) ---
    final_gas_supply = pd.concat([lng_to_gas_transfomation, lng_losses_projection, gas_pipeline_energy_use, new_gas_supply], ignore_index=True)
    # breakpoint()  # Debug breakpoint to verify combined rows
    if PLOT:
        plot_gas_supply_comparison_with_adjustments(results_df, final_gas_supply, years_to_keep_in_results_str, SINGLE_ECONOMY_ID)
    
    #add data back to the results_dfafter removign the original gas supply data. do a merge so we can double check we arent missing any of the original data
    results_df_new = results_df.merge(final_gas_supply, on=shared_categories, how='outer', suffixes=('', '_y'), indicator=True)
    #where _merge is left_only and the  fuels are 06_crude_oil_and_ngl or 01_coal then we need to keep that data
    results_df_new.loc[(results_df_new['_merge'] == 'left_only') & (results_df_new['fuels'].isin(['06_crude_oil_and_ngl', '01_coal'])), '_merge'] = 'both'
    #if there are right onlys then double check they are only where sub1sectors is 09_06_gas_processing_plants 10_01_own_use or 15_05_pipeline_transport
    results_df_new.loc[(results_df_new['_merge'] == 'right_only') & (results_df_new['sub1sectors'].isin(['09_06_gas_processing_plants', '15_05_pipeline_transport', '10_01_own_use'])), '_merge'] = 'both'
    
    if len(results_df_new[results_df_new['_merge'] == 'left_only']) > 0 or len(results_df_new[results_df_new['_merge'] == 'right_only']) > 0:
        breakpoint()
        raise Exception("There are rows in the original results_df_new that are not in the new results_df_new. This is unexpected.")
    #replace the original data with the new data where it exists
    for col in years_to_keep_in_results_str:
        results_df_new[col] = np.where(results_df_new[col+'_y'].notnull(), results_df_new[col+'_y'], results_df_new[col])
    #if there areany other _y cols, drop them
    results_df_new.drop(columns=[col for col in results_df_new.columns if '_y' in col], inplace=True)
    #drop the _merge col
    results_df_new.drop(columns=['_merge'], inplace=True)
    
    ##################################################################
    
    date_id = datetime.now().strftime("%Y%m%d")
    #saave results to  \results\modelled_within_repo\gas_supply
    final_gas_supply.to_csv(f'results/modelled_within_repo/gas_supply/final_gas_supply_{SINGLE_ECONOMY_ID}_{date_id}.csv', index=False)
    
    ##################################################################
    
    return results_df_new

def plot_gas_supply_comparison_with_adjustments(results_df, new_gas_supply, years_to_keep_in_results_str, SINGLE_ECONOMY_ID):
    """
    Create a Plotly line chart comparing original gas supply data (results_df) to the updated gas supply data (new_gas_supply)
    that includes LNG transformation adjustments, losses, and pipeline energy use.
    
    Steps:
      1. Filter the original data for basic supply (production, imports, exports for LNG and natural gas).
      2. For the new data, assign a Category based on sectors:
           - 'Supply' for production, imports, exports.
           - 'Transformation' for LNG transformation rows.
           - 'Losses' for LNG transformation losses.
           - 'Pipeline Energy Use' for pipeline energy use rows.
      3. Label the original rows as "Original" and the new rows as "New".
      4. Combine both datasets and convert from wide format to long format.
      5. Create a line chart with facets by Category.
      6. Save the chart as an HTML file.
    
    Parameters:
      - results_df: Original gas supply DataFrame.
      - new_gas_supply: Updated gas supply DataFrame (includes adjustments, losses, pipelines).
      - years_to_keep_in_results_str: List of year columns (e.g., [2020, 2021, 2022, ...]).
      - SINGLE_ECONOMY_ID: Identifier used in the output filename.
      
    Returns:
      The Plotly figure object.
    """
    # Define output directory and create if not exists.
    output_dir = "plotting_output/missing_sectors_projections"
    os.makedirs(output_dir, exist_ok=True)
    
    # --- Step 1: Filter original gas supply data ---
    supply_sectors = ['01_production', '02_imports', '03_exports']
    supply_subfuels = ['08_02_lng', '08_01_natural_gas']
    original_supply = results_df[
        (results_df['sectors'].isin(supply_sectors)) &
        (results_df['subfuels'].isin(supply_subfuels))
    ].copy()
    original_supply['Type'] = 'Original'
    # For original data, we mark the Category as "Supply"
    original_supply['Category'] = 'Supply'
    
    # --- Step 2: Prepare new gas supply data ---
    new_supply = new_gas_supply.copy()
    new_supply['Type'] = 'New'
    # Assign Category based on sectors.
    def assign_category(row):
        if row['sectors'] in supply_sectors:
            return 'Supply'
        elif row['sectors'] == '09_total_transformation_sector':
            return 'Transformation'
        elif row['sectors'] == '10_losses_and_own_use':
            return 'Losses'
        elif row['sectors'] == '15_transport_sector':
            return 'Pipeline Energy Use'
        else:
            return 'Other'
    new_supply['Category'] = new_supply.apply(assign_category, axis=1)
    
    # --- Step 3: Combine original and new datasets ---
    combined_df = pd.concat([original_supply, new_supply], ignore_index=True)
    
    # --- Step 4: Convert from wide to long format ---
    # We keep common identifier columns plus the new "Type" and "Category" columns.
    id_vars = ['economy', 'scenarios', 'sectors','fuels', 'subfuels', 'Type', 'Category']
    melted_df = combined_df.melt(
        id_vars=id_vars,
        value_vars=years_to_keep_in_results_str,
        var_name='year',
        value_name='value'
    )
    # Convert year to string for proper x-axis ordering.
    melted_df['year'] = melted_df['year'].astype(str)
    
    #add together fueks and subfuels to get a better label for the chart
    melted_df['fuels_subfuels'] = melted_df['fuels'] + '_' + melted_df['subfuels']
    #sum everything up
    melted_df = melted_df.groupby(['economy', 'scenarios','fuels_subfuels', 'Type', 'Category', 'year']).sum().reset_index()
    # --- Step 5: Create the line chart ---
    # We facet by Category so that Supply, Transformation, Losses, and Pipeline Energy Use are shown separately.
    fig = px.line(
        melted_df,
        x='year',
        y='value',
        color='fuels_subfuels',   
        line_dash='Type',    
        facet_row='scenarios',    # Facet by original vs new data
        facet_col='Category',    # Facet by category type
        title='Comparison of Original vs Updated Gas Supply (Including Adjustments)'
    )
    
    # --- Step 6: Save the chart as an HTML file ---
    file_path = os.path.join(output_dir, f"gas_supply_comparison_{SINGLE_ECONOMY_ID}.html")
    fig.write_html(file_path)
    
    return fig

def estimate_losses_for_lng_processes(lng_supply, layout_df, base_year_lng_and_gas_all_econs, all_economies_layout_df, gas_supply, str_OUTLOOK_BASE_YEAR, years_to_keep_in_results_str, SINGLE_ECONOMY_ID):
    """
    Estimate the losses for LNG transformation for all projection years.
    
    Steps:
      1. Check if there is valid LNG trade in the base year.
      2. Load the OECD LNG ratios and compute a losses ratio.
         - If no base-year LNG trade exists, use aggregated layout data.
         - Otherwise, calculate the ratio per economy and average it.
      3. Merge the computed ratios with the LNG supply data.
      4. Multiply the LNG supply values by the ratio (converted to negative) to represent losses.
      5. Group the resulting rows by standard loss identifiers.
    """
    # --- Determine if base-year LNG trade exists ---
    gas_trade_base_year = layout_df[
        ((layout_df['sectors'] == '02_imports') | (layout_df['sectors'] == '03_exports')) &
        (layout_df['subfuels'].isin(['08_02_lng', '08_01_natural_gas']))
    ].copy()
    losses = layout_df[
        (layout_df['sub2sectors'] == '10_01_03_liquefaction_regasification_plants') &
        (layout_df['subfuels'].isin(['08_02_lng', '08_01_natural_gas']))
    ].copy()
    gas_trade_base_year = gas_trade_base_year[['economy', 'scenarios', 'sectors', 'subfuels', str_OUTLOOK_BASE_YEAR]].copy()
    
    # Check for NO_LOSSES_BASE_YEAR in the sector 10_01_03_liquefaction_regasification_plants
    if losses.loc[
            (losses['sub2sectors'] == '10_01_03_liquefaction_regasification_plants'),
            str_OUTLOOK_BASE_YEAR
        ].shape[0] > 0:
        if losses.loc[
            (losses['sub2sectors'] == '10_01_03_liquefaction_regasification_plants'),
            str_OUTLOOK_BASE_YEAR
        ].sum() < 0:#tricky! losses are negative!
            NO_LOSSES_BASE_YEAR = False
        else:
            NO_LOSSES_BASE_YEAR = True
    else:
        NO_LOSSES_BASE_YEAR = True
    #do check for NO_LNG_IMPORTS_BASE_YEAR and NO_LNG_EXPORTS_BASE_YEAR
    if gas_trade_base_year.loc[
            (gas_trade_base_year['subfuels'] == '08_02_lng') & (gas_trade_base_year['sectors'] == '02_imports'),
            str_OUTLOOK_BASE_YEAR
        ].shape[0] > 0:
        
        try:
            if gas_trade_base_year.loc[
                (gas_trade_base_year['subfuels'] == '08_02_lng') & (gas_trade_base_year['sectors'] == '02_imports'),
                str_OUTLOOK_BASE_YEAR
            ].sum() == 0:
                NO_LNG_IMPORTS_BASE_YEAR = True
            else:
                NO_LNG_IMPORTS_BASE_YEAR = False
        except:
            breakpoint()#not sure why this is happening
            
    else:
        NO_LNG_IMPORTS_BASE_YEAR = True
        
    if gas_trade_base_year.loc[
            (gas_trade_base_year['subfuels'] == '08_02_lng') & (gas_trade_base_year['sectors'] == '03_exports'),
            str_OUTLOOK_BASE_YEAR
        ].shape[0] > 0:
        if gas_trade_base_year.loc[
            (gas_trade_base_year['subfuels'] == '08_02_lng') & (gas_trade_base_year['sectors'] == '03_exports'),
            str_OUTLOOK_BASE_YEAR
        ].sum() == 0:
            NO_LNG_EXPORTS_BASE_YEAR = True
        else:
            NO_LNG_EXPORTS_BASE_YEAR = False
    else:
        NO_LNG_EXPORTS_BASE_YEAR = True
    # Check if there is valid LNG trade in the base year
    
    if (NO_LNG_IMPORTS_BASE_YEAR and NO_LNG_EXPORTS_BASE_YEAR) | NO_LOSSES_BASE_YEAR:
        BASE_YEAR_LNG_AVAILABLE = False
    else:
        BASE_YEAR_LNG_AVAILABLE = True

    # --- Load OECD LNG-to-gas ratio data ---
    try:
        lng_ratios = pd.read_csv('data/raw_data/lng_to_pipeline_trade_ratios.csv').rename(
            columns={'Value': 'lng_to_gas_trade_ratio'}
        ).copy()
    except FileNotFoundError:
        breakpoint()
        raise Exception("The file lng_to_pipeline_trade_ratios.csv is missing. Is created in the visualisation system.")
    # --- extract gas trade for base year for oecd econmies if necessary ---
    if not BASE_YEAR_LNG_AVAILABLE:
        
        if SINGLE_ECONOMY_ID in lng_ratios.economy.unique() and lng_ratios.loc[(lng_ratios.economy == SINGLE_ECONOMY_ID)& (lng_ratios.Year == OUTLOOK_BASE_YEAR)].lng_to_gas_trade_ratio.sum() > 0:#if there are lng trade ratios for the single economy in the base year then we can use that to estimate the lng trade ratios for the single economy
            
            # --- We will use OECD LNG ratios to estiamte lng trade ratios for the single economy ---
            lng_ratios = lng_ratios[lng_ratios.economy == SINGLE_ECONOMY_ID].copy()
            gas_trade_base_year = calculate_base_year_lng_trade(gas_trade_base_year, lng_ratios, str_OUTLOOK_BASE_YEAR, ONE_ECON = True)
            BASE_YEAR_LNG_AVAILABLE = True
        else:
            #we will estiamte the average ratio of losses to lng trade in base yearfor all eocnomies. so extract/create data on lng trade fr oecd countries now
            base_year_lng_and_gas_all_econs = calculate_base_year_lng_trade(base_year_lng_and_gas_all_econs, lng_ratios, str_OUTLOOK_BASE_YEAR, ONE_ECON = False)          
    else:
        pass
    # if SINGLE_ECONOMY_ID == '09_ROK':
    #     breakpoint()
    #whats goign on with trade in the base year for rok. it seems our losses are spiking because we arent estimating lng right.
    gas_trade_base_year_copy = gas_trade_base_year.copy()
    # --- Calculate losses ratio for LNG transformation ---
    if (not BASE_YEAR_LNG_AVAILABLE) | NO_LOSSES_BASE_YEAR:
        # --- Calculate ratio for each economy and average ---
        losses_list = []
        for economy in base_year_lng_and_gas_all_econs['economy'].unique():
            #extract losses by fuel/subfuel for the single economy
            losses_economy = all_economies_layout_df[(all_economies_layout_df['economy'] == economy) & (all_economies_layout_df['scenarios'] == 'reference') & (all_economies_layout_df['sub2sectors'] == '10_01_03_liquefaction_regasification_plants')].copy()
            #sum up the losses by fuel/subfuel
            losses_economy = losses_economy.groupby(['economy', 'scenarios', 'fuels', 'subfuels'], as_index=False)[str_OUTLOOK_BASE_YEAR].sum().copy()
            
            # Calculate denominator: absolute sum of base-year LNG trade for the 'reference' scenario, grouped by sectors
            lng_trade_base_year_abs_sum = base_year_lng_and_gas_all_econs[
            (base_year_lng_and_gas_all_econs['economy'] == economy) &
            (base_year_lng_and_gas_all_econs['scenarios'] == 'reference') &
            (base_year_lng_and_gas_all_econs['subfuels'] == '08_02_lng')
            ][str_OUTLOOK_BASE_YEAR].abs().sum().copy()
            
            if not losses_economy.empty and not lng_trade_base_year_abs_sum == 0:
                losses_economy['ratio'] = losses_economy[str_OUTLOOK_BASE_YEAR].abs() / lng_trade_base_year_abs_sum
                losses_economy['ratio'] = losses_economy['ratio'].replace(0, np.nan)
                losses_list.append(losses_economy[['fuels', 'subfuels', 'ratio']])
        
        if not losses_list:
            breakpoint()
            raise Exception('No valid LNG trade data found for any economy. Cannot calculate losses ratio.')
        
        losses = pd.concat(losses_list, ignore_index=True)
        #set any infs and 0s with nan so as not to affect the average
        losses['ratio'] = losses['ratio'].replace([np.inf, 0], np.nan)        
        losses = losses.groupby(['fuels', 'subfuels'], as_index=False)['ratio'].mean().copy()
        #drop nas
        losses.dropna(inplace=True)
        #chekc the ratio is not 0
        if losses['ratio'].sum() == 0:
            breakpoint()
            raise Exception('The losses for LNG transformation in the base year are all 0. This is unexpected and should be investigated.')
    else:
        # --- Use data from the single economy sicne we have it available ---
        losses = layout_df[
            (layout_df['sub2sectors'] == '10_01_03_liquefaction_regasification_plants')
        ]
        losses = losses.groupby(['economy', 'scenarios', 'fuels', 'subfuels'], as_index=False)[str_OUTLOOK_BASE_YEAR].sum().copy()
        # Calculate denominator: absolute sum of base-year LNG trade for the 'reference' scenario, grouped by sectors
        lng_trade_base_year_abs_sum = gas_trade_base_year.loc[
            (gas_trade_base_year.scenarios == 'reference') & (gas_trade_base_year['subfuels'] == '08_02_lng')
        ][str_OUTLOOK_BASE_YEAR].abs().sum()
        # --- Calculate the losses ratio ---
        losses['ratio'] = losses[str_OUTLOOK_BASE_YEAR].abs() /lng_trade_base_year_abs_sum
        #reapce infs and 0s with nans so as not to affect the average
        losses['ratio'] = losses['ratio'].replace([np.inf, 0], np.nan)
        losses = losses.groupby(['fuels', 'subfuels'], as_index=False)['ratio'].mean().copy()
        #repalce nas with 0s
        losses['ratio'] = losses['ratio'].replace(np.nan, 0)
        #drop nas
        losses.dropna(inplace=True)
        #chekc the ratio is not 0
        if losses['ratio'].sum() == 0:
            breakpoint()
            raise Exception('The losses for LNG transformation in the base year are all 0. This is unexpected and should be investigated.')

    # --- Estimate losses for all projection years using the ratio ---
    # Copy the LNG supply data for losses calculation.
    lng_losses_projection = lng_supply.copy()
    
    # Merge the computed losses ratios onto the LNG supply data.
    losses['key']= 0
    lng_losses_projection['key'] = 0
    lng_losses_projection = lng_losses_projection.merge(losses, on=['key'], how='outer', indicator=True, suffixes=('_y', ''))#is outer  merge correct?
    # Check for any missing merge matches.
    if len(lng_losses_projection[lng_losses_projection['_merge'] == 'left_only']) > 0 or len(lng_losses_projection[lng_losses_projection['_merge'] == 'right_only']) > 0:
        breakpoint()
        raise Exception('There are missing values in the losses ratios for LNG transformation. This is unexpected and should be investigated.')
    # Remove the merge indicator and _y columns.
    lng_losses_projection.drop(columns=[col for col in lng_losses_projection.columns if '_y' in col], inplace=True)
    lng_losses_projection.drop(columns=['_merge'], inplace=True)
    # For each projection year, multiply the absolute LNG supply value by the losses ratio,
    # then convert it to a negative value to represent losses.
    for year in years_to_keep_in_results_str:
        lng_losses_projection[year] = -(lng_losses_projection[year].abs() * lng_losses_projection['ratio'])
    # Check for any missing values after the operation.
    if lng_losses_projection[years_to_keep_in_results_str].isna().sum().sum() > 0:
        breakpoint()
        raise Exception('There are missing values in the losses for LNG transformation. This is unexpected and should be investigated.')
    # Set standardized identifiers for losses rows.
    lng_losses_projection['sectors'] = '10_losses_and_own_use'
    lng_losses_projection['sub1sectors'] = '10_01_own_use'
    lng_losses_projection['sub2sectors'] = '10_01_03_liquefaction_regasification_plants'
    lng_losses_projection['sub3sectors'] = 'x'
    lng_losses_projection['sub4sectors'] = 'x'
    # breakpoint()#all cols algood?
    # Group losses rows to sum values across projection years.
    lng_losses_projection = lng_losses_projection.groupby(
        ['economy', 'scenarios', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels'], as_index=False
    )[years_to_keep_in_results_str].sum().copy()
    #drop rows full of 0s
    lng_losses_projection = lng_losses_projection.loc[~(lng_losses_projection[years_to_keep_in_results_str] == 0).all(axis=1)].copy()
    #doubel check there are values remaining
    if lng_losses_projection.empty:
        breakpoint()
        raise Exception('All values in the LNG losses for transformation are 0. This is unexpected and should be investigated.')
    
    return lng_losses_projection, gas_trade_base_year_copy

def calculate_base_year_lng_trade(base_year_lng_and_gas_all_econs, lng_ratios, str_OUTLOOK_BASE_YEAR, ONE_ECON = False):
    #we need to use the aggregated losses from the layout data
    for economy in lng_ratios['economy'].unique():
        ratios_base_year = lng_ratios[
            (lng_ratios['Year'] == OUTLOOK_BASE_YEAR) & (lng_ratios['economy'] == economy)
        ].copy()
        if ratios_base_year.shape[0] == 0:
            breakpoint()
            raise Exception('There is no data for the base year in the lng_to_pipeline_trade_ratios.csv file. This is required to estimate the transformation of lng to gas.')

        gas_trade_base_year = base_year_lng_and_gas_all_econs[['economy', 'scenarios', 'sectors', 'subfuels', str_OUTLOOK_BASE_YEAR]].loc[
            base_year_lng_and_gas_all_econs['economy'] == economy
        ].copy()

        if gas_trade_base_year.empty:
            breakpoint()
            raise Exception('This is unexpected and should be investigated.')
        
        if gas_trade_base_year.loc[gas_trade_base_year['subfuels'] == '08_02_lng', str_OUTLOOK_BASE_YEAR].sum() != 0:
            #we can just use this data instead of doing this calculation
            pass
        else:
            #PVIOT THE subfuels so we can minus the nat gas trade times the ratio from the lng trade
            gas_trade_base_year = gas_trade_base_year.pivot_table(index=['economy', 'scenarios', 'sectors'], columns='subfuels', values=str_OUTLOOK_BASE_YEAR).reset_index()
            gas_trade_base_year = gas_trade_base_year.merge(ratios_base_year[['sectors', 'lng_to_gas_trade_ratio']], on='sectors', how='left')
            gas_trade_base_year['08_02_lng'] = gas_trade_base_year['08_01_natural_gas'] * gas_trade_base_year['lng_to_gas_trade_ratio']
            gas_trade_base_year['08_01_natural_gas'] = gas_trade_base_year['08_01_natural_gas'] - gas_trade_base_year['08_02_lng']#metl it back to the original shape
            gas_trade_base_year.drop(columns=['lng_to_gas_trade_ratio'], inplace=True)
            gas_trade_base_year = gas_trade_base_year.melt(id_vars=['economy', 'scenarios', 'sectors'], value_vars=['08_02_lng', '08_01_natural_gas'], var_name='subfuels', value_name=str_OUTLOOK_BASE_YEAR)
            
        #join the data back to the base_year_lng_and_gas_all_econs
        base_year_lng_and_gas_all_econs = base_year_lng_and_gas_all_econs.merge(
            gas_trade_base_year, 
            on=['economy', 'scenarios', 'sectors', 'subfuels'], 
            how='left', 
            suffixes=('', '_x')
        )

        base_year_lng_and_gas_all_econs[str_OUTLOOK_BASE_YEAR] = np.where(
            base_year_lng_and_gas_all_econs[str_OUTLOOK_BASE_YEAR + '_x'].notna(),
            base_year_lng_and_gas_all_econs[str_OUTLOOK_BASE_YEAR + '_x'],
            base_year_lng_and_gas_all_econs[str_OUTLOOK_BASE_YEAR]
        )
        base_year_lng_and_gas_all_econs.drop(columns=[str_OUTLOOK_BASE_YEAR + '_x'], inplace=True)
    return base_year_lng_and_gas_all_econs
        

def create_lng_transformation_rows(lng_supply, years_to_keep_in_results_str):
    """
    Create transformation rows for LNG-to-gas conversion based on LNG imports and exports data.
    
    For each LNG import/export row:
      - One row is generated for LNG (input) with an inverted value.
      - One row is generated for natural gas (output) with the original value.
      
    The new rows are assigned standardized sector identifiers for transformation.
    """
    # --- Process LNG imports ---
    lng_to_gas_transfomation_for_imports = lng_supply[lng_supply['sectors'] == '02_imports'].copy()
    lng_to_gas_transfomation_for_imports['sectors'] = '09_total_transformation_sector'
    lng_to_gas_transfomation_for_imports['sub1sectors'] = '09_06_gas_processing_plants'
    lng_to_gas_transfomation_for_imports['sub2sectors'] = '09_06_02_liquefaction_regasification_plants'
    
    # Create two rows: one for LNG input (value inverted) and one for natural gas output.
    lng_to_gas_transfomation_for_imports_lng = lng_to_gas_transfomation_for_imports.copy()
    lng_to_gas_transfomation_for_imports_lng[years_to_keep_in_results_str] = -lng_to_gas_transfomation_for_imports_lng[years_to_keep_in_results_str]
    lng_to_gas_transfomation_for_imports_lng['subfuels'] = '08_02_lng'
    
    lng_to_gas_transfomation_for_imports_gas = lng_to_gas_transfomation_for_imports.copy()
    lng_to_gas_transfomation_for_imports_gas['subfuels'] = '08_01_natural_gas'
    
    # --- Process LNG exports ---
    lng_to_gas_transfomation_for_exports = lng_supply[lng_supply['sectors'] == '03_exports'].copy()
    lng_to_gas_transfomation_for_exports['sectors'] = '09_total_transformation_sector'
    lng_to_gas_transfomation_for_exports['sub1sectors'] = '09_06_gas_processing_plants'
    lng_to_gas_transfomation_for_exports['sub2sectors'] = '09_06_02_liquefaction_regasification_plants'
    
    # Create two rows: one for LNG input (value inverted) and one for natural gas output.
    lng_to_gas_transfomation_for_exports_lng = lng_to_gas_transfomation_for_exports.copy()
    lng_to_gas_transfomation_for_exports_lng[years_to_keep_in_results_str] = -lng_to_gas_transfomation_for_exports_lng[years_to_keep_in_results_str]
    lng_to_gas_transfomation_for_exports_lng['subfuels'] = '08_02_lng'
    
    lng_to_gas_transfomation_for_exports_gas = lng_to_gas_transfomation_for_exports.copy()
    lng_to_gas_transfomation_for_exports_gas['subfuels'] = '08_01_natural_gas'
    
    # --- Combine all transformation rows ---
    lng_to_gas_transfomation = pd.concat([
        lng_to_gas_transfomation_for_imports_lng, 
        lng_to_gas_transfomation_for_imports_gas, 
        lng_to_gas_transfomation_for_exports_lng, 
        lng_to_gas_transfomation_for_exports_gas
    ], ignore_index=True)
    
    return lng_to_gas_transfomation
        
        
def estimate_pipeline_energy_use_based_on_gas_imports_production(gas_supply, layout_df, shared_categories, years_to_keep_in_results_str, str_OUTLOOK_BASE_YEAR):
    """
    Estimate pipeline energy use for gas supply based on natural gas imports and production.
    
    Steps:
      1. Filter gas supply activity for natural gas (subfuel '08_01_natural_gas') from imports and production.
      2. Get base-year gas supply data from layout for natural gas.
      3. Verify that data exists.
      4. Retrieve pipeline energy use data from layout and calculate the ratio of pipeline energy use to gas supply.
      5. Merge the ratio onto gas supply activity and multiply each year's value.
    """
    # --- Retrieve pipeline energy use data from layout ---
    pipeline_energy_use = layout_df[(layout_df['sectors'] == '15_transport_sector') & (layout_df['sub1sectors'] == '15_05_pipeline_transport')].copy()
    
    if pipeline_energy_use[pipeline_energy_use.fuels=='18_electricity'][str_OUTLOOK_BASE_YEAR].sum() > 0:
        ECONOMIES_TO_STILL_ESTIMATE_PIPELINE_ENERGY_USE_FOR = ['01_AUS']
        ECONOMIES_TO_NOT_ESTIMATE_PIPELINE_ENERGY_USE_FOR = ['16_RUS']
        if pipeline_energy_use.economy.unique()[0] not in ECONOMIES_TO_STILL_ESTIMATE_PIPELINE_ENERGY_USE_FOR + ECONOMIES_TO_NOT_ESTIMATE_PIPELINE_ENERGY_USE_FOR:
            raise Exception(f'Pipeline energy use for electricity is already present in the layout file for {pipeline_energy_use.economy.unique()[0]}. This is unexpected and should be investigated')
        elif pipeline_energy_use.economy.unique()[0] in ECONOMIES_TO_NOT_ESTIMATE_PIPELINE_ENERGY_USE_FOR:
            print(f'Pipeline energy use for electricity is already present in the layout file for {pipeline_energy_use.economy.unique()[0]}. Skipping estimation.')
            return pd.DataFrame()
        else:
            pass#continue
        
    #if its 0 we will assume there wil be no future pipeline energy use even if gas supply goes from 0 to a non 0 value
    if pipeline_energy_use[str_OUTLOOK_BASE_YEAR].sum() == 0:
        return pd.DataFrame()
    
    # --- Filter gas supply activity for natural gas (imports/production) ---
    gas_supply_activity = gas_supply[((gas_supply['sectors'] == '02_imports') | (gas_supply['sectors'] == '01_production')) & (gas_supply['subfuels'] == '08_01_natural_gas')].copy()
    
    # --- Retrieve base-year gas supply data for natural gas from layout ---
    gas_supply_base_year = layout_df[((layout_df['sectors'] == '02_imports') | (layout_df['sectors'] == '01_production')) & (layout_df['subfuels'] == '08_01_natural_gas')][[str_OUTLOOK_BASE_YEAR] + shared_categories].copy()
    
    # --- Check that necessary data exists ---
    if gas_supply_activity.shape[0] == 0 or gas_supply_base_year.shape[0] == 0:
        breakpoint()
        raise Exception('There are no rows for imports or production of natural gas in the gas supply data. This is unexpected and should be investigated.')
    
    # Use the base year and two preceding years to determine if pipeline energy use data is present.
    if pipeline_energy_use[[str_OUTLOOK_BASE_YEAR, str(OUTLOOK_BASE_YEAR-1), str(OUTLOOK_BASE_YEAR-2)]].sum().sum() == 0:
        return pd.DataFrame()
    elif pipeline_energy_use[str_OUTLOOK_BASE_YEAR].sum().sum() == 0:
        raise Exception('There is no pipeline energy use in the layout file in base year only. This is unexpected and should be investigated.')
    
    # Keep only necessary columns and rename base-year column.
    pipeline_energy_use = pipeline_energy_use[['economy', 'scenarios', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels', str_OUTLOOK_BASE_YEAR]].copy()
    pipeline_energy_use.rename(columns={str_OUTLOOK_BASE_YEAR: 'pipeline_energy_use_base_year'}, inplace=True)
    
    # --- Aggregate gas supply base-year data for natural gas ---
    gas_supply_base_year = gas_supply_base_year.groupby(
        ['economy', 'scenarios', 'fuels', 'subfuels'], as_index=False
    )[str_OUTLOOK_BASE_YEAR].sum().copy()#drop sectors since we are determining that they arent important for the pipeline energy use
    gas_supply_activity = gas_supply_activity.groupby(
        ['economy', 'scenarios', 'fuels', 'subfuels'], as_index=False
    )[years_to_keep_in_results_str].sum().copy()
    
    # Rename the base-year column to enable merging.
    gas_supply_base_year.rename(columns={str_OUTLOOK_BASE_YEAR: 'gas_supply_base_year'}, inplace=True)
    
    # #drop any rows with 0 gas supply
    gas_supply_base_year = gas_supply_base_year.loc[~(gas_supply_base_year['gas_supply_base_year'] == 0)].copy()
    
    # --- Merge pipeline energy use data with gas supply base-year data ---
    pipeline_energy_ratio = gas_supply_base_year.merge(pipeline_energy_use, on=['economy', 'scenarios'], how='outer', suffixes=('_x', ''))
    # breakpoint()  # Debug breakpoint to verify merge results.
    
    # --- Calculate the ratio of pipeline energy use to gas supply ---
    pipeline_energy_ratio['ratio'] = pipeline_energy_ratio['pipeline_energy_use_base_year'] / pipeline_energy_ratio['gas_supply_base_year']
    
    if pipeline_energy_ratio['ratio'].isna().sum() > 0:
        breakpoint()
        raise Exception('There are missing values in the pipeline energy demand ratios. This is unexpected and should be investigated.')
    #kkeep only where ratio is not 0
    pipeline_energy_ratio = pipeline_energy_ratio.loc[~(pipeline_energy_ratio['ratio'] == 0)].copy()
    
    #keep fuels and subfuels, economy, scenarios and ratio. this is because we want to keep the origingal sector for which the ratio was calculated, as well as the fuels and subfuels for which the ratio was calculated.. ie this give us data for electricity use for gas in the imports
    pipeline_energy_ratio = pipeline_energy_ratio[['economy', 'scenarios', 'fuels', 'subfuels', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors','ratio']].copy()
    
    if pipeline_energy_ratio.empty:
        breakpoint()
        raise Exception('All values in the pipeline energy demand ratios are 0. This is unexpected and should be investigated.')
    
    # --- Merge the ratio onto the gas supply activity data ---
    gas_supply_activity_new = gas_supply_activity.drop(columns=['fuels', 'subfuels']).merge(
        pipeline_energy_ratio, 
        on=['economy', 'scenarios'], 
        how='outer', suffixes=('_x', '')#,'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels'
    )
    # --- Multiply each projection year's value by the pipeline energy use ratio ---
    for year in years_to_keep_in_results_str:
        gas_supply_activity_new[year] = gas_supply_activity_new[year] * gas_supply_activity_new['ratio']
        
    if gas_supply_activity_new[years_to_keep_in_results_str].isna().sum().sum() > 0:
        breakpoint()
        raise Exception('There are missing values in the pipeline energy demand. This is unexpected and should be investigated.')

    #drop ratio
    gas_supply_activity_new.drop(columns=['ratio'], inplace=True)
    #drop anything for 08_gas x since thats just a bit confusing 
    gas_supply_activity_new = gas_supply_activity_new.loc[~((gas_supply_activity_new['fuels'] == '08_gas') & (gas_supply_activity_new['subfuels'] == 'x'))].copy()
    #check its not empty
    if gas_supply_activity_new.empty:
        breakpoint()
        raise Exception('All values in the pipeline energy demand are 0. This is unexpected and should be investigated.')
    return gas_supply_activity_new

#%%

def subtract_pipeline_adjustment_from_larger_source(gas_supply, gas_pipeline_energy_use, years_to_keep_in_results_str):
    """
    For each economy, scenario, and fuels group, add the pipeline energy use 
    (from rows with sectors '15_transport_sector' and sub1sectors '15_05_pipeline_transport')
    to whichever is larger between:
      - Natural gas production (rows with sectors '01_production' and subfuels '08_01_natural_gas')
      - Natural gas imports (rows with sectors '02_imports' and subfuels '08_02_lng')
      
    This function is intended to be called immediately after the pipeline energy use calculation.
    It makes it so there is extra fuel available for pipeline energy use, without affecting 
    the overall gas (or other fuels) supply for the rest of the economy.
    
    Note: Calculations are performed for each year column listed in years_to_keep_in_results_str.
    """
    # breakpoint()
    if len(gas_pipeline_energy_use) == 0:#if there is no pipeline energy use then we dont need to do anything
        return gas_supply
    # --- Identify pipeline energy use rows ---
    pipeline_mask = (
        (gas_pipeline_energy_use['sectors'] == '15_transport_sector') &
        (gas_pipeline_energy_use['sub1sectors'] == '15_05_pipeline_transport') &
        (gas_pipeline_energy_use['subfuels'] == '08_01_natural_gas') &
        (gas_pipeline_energy_use['fuels'] == '08_gas')
    )
    pipeline_df = gas_pipeline_energy_use.loc[pipeline_mask].copy()
    if pipeline_df.empty:
        return gas_supply
    # breakpoint()
    # Sum pipeline energy use by economy, scenarios, and fuels
    pipeline_sum = pipeline_df.groupby(['economy', 'scenarios', 'fuels','subfuels'], as_index=False)[years_to_keep_in_results_str].sum()

    # --- Identify natural gas production rows ---
    prod_mask = (
        (gas_supply['sectors'] == '01_production') &
        (gas_supply['subfuels'] == '08_01_natural_gas') &
        (gas_supply['fuels'] == '08_gas')
    )
    prod_df = gas_supply[prod_mask].copy()
    prod_group = prod_df.groupby(['economy', 'scenarios', 'fuels', 'subfuels'], as_index=False)[years_to_keep_in_results_str].sum()
    prod_group.rename(columns={y: f"{y}_prod" for y in years_to_keep_in_results_str}, inplace=True)

    # --- Identify natural gas import rows ---
    imp_mask = (
        (gas_supply['sectors'] == '02_imports') &
        (gas_supply['subfuels'] == '08_01_natural_gas') &
        (gas_supply['fuels'] == '08_gas')
    )
    imp_df = gas_supply[imp_mask].copy()
    imp_group = imp_df.groupby(['economy', 'scenarios', 'fuels', 'subfuels'], as_index=False)[years_to_keep_in_results_str].sum()
    imp_group.rename(columns={y: f"{y}_imp" for y in years_to_keep_in_results_str}, inplace=True)

    # --- Merge production and imports, then merge the pipeline energy use sums ---
    prod_imp = prod_group.merge(imp_group, on=['economy', 'scenarios', 'fuels','subfuels'], how='outer').fillna(0)
    prod_imp = prod_imp.merge(pipeline_sum, on=['economy', 'scenarios', 'fuels','subfuels'], how='outer').fillna(0)

    # --- For each year, add pipeline energy use to the larger supply source ---
    for y in years_to_keep_in_results_str:
        prod_col = f"{y}_prod"
        imp_col = f"{y}_imp"
        # The pipeline adjustment for year y is in column y (from pipeline_sum merge)
        larger_mask = prod_imp[imp_col] >= prod_imp[prod_col]
        prod_imp.loc[larger_mask, prod_col] = prod_imp.loc[larger_mask, prod_col] + prod_imp.loc[larger_mask, y]
        prod_imp.loc[~larger_mask, imp_col] = prod_imp.loc[~larger_mask, imp_col] + prod_imp.loc[~larger_mask, y]

    # --- Build updated production and import rows ---
    updated_prod = prod_imp[['economy', 'scenarios', 'fuels','subfuels']].copy()
    for y in years_to_keep_in_results_str:
        updated_prod[y] = prod_imp[f"{y}_prod"]
    updated_prod['sectors'] = '01_production'
    updated_prod['sub1sectors'] = 'x'
    updated_prod['sub2sectors'] = 'x'
    updated_prod['sub3sectors'] = 'x'
    updated_prod['sub4sectors'] = 'x'

    updated_imp = prod_imp[['economy', 'scenarios', 'fuels','subfuels']].copy()
    for y in years_to_keep_in_results_str:
        updated_imp[y] = prod_imp[f"{y}_imp"]
    updated_imp['sectors'] = '02_imports'
    updated_imp['sub1sectors'] = 'x'
    updated_imp['sub2sectors'] = 'x'
    updated_imp['sub3sectors'] = 'x'
    updated_imp['sub4sectors'] = 'x'
    
    # --- Remove the old rows from gas_supply. we will do it by merging on the key cols: economy, scenarios, sectors, fuels, subfuels which allows us to include any fuels/subfuels inserted into the data---
    gas_supply_no_old = gas_supply.merge(updated_prod, on=['economy', 'scenarios', 'sectors', 'fuels','subfuels'], how='outer', indicator=True, suffixes=('', '_x'))
    gas_supply_no_old = gas_supply_no_old[gas_supply_no_old['_merge'] == 'left_only'].copy()
    gas_supply_no_old.drop(columns=['_merge']+ [col for col in gas_supply_no_old.columns if '_x' in col], inplace=True)
    gas_supply_no_old = gas_supply_no_old.merge(updated_imp, on=['economy', 'scenarios', 'sectors', 'fuels','subfuels'], how='outer', indicator=True, suffixes=('', '_x'))
    gas_supply_no_old = gas_supply_no_old[gas_supply_no_old['_merge'] == 'left_only'].copy()
    gas_supply_no_old.drop(columns=['_merge']+ [col for col in gas_supply_no_old.columns if '_x' in col], inplace=True)
    # Append the updated rows
    final_gas_supply = pd.concat([gas_supply_no_old, updated_prod, updated_imp], ignore_index=True)
    #make sure there are no pipeline rows in gas_supply_no_old
    final_gas_supply = final_gas_supply.loc[~((final_gas_supply['sectors'] == '15_transport_sector') & (final_gas_supply['sub1sectors'] == '15_05_pipeline_transport'))].copy()
    
    return final_gas_supply

def subtract_losses_from_larger_source(gas_supply, lng_losses_projection,years_to_keep_in_results_str, SINGLE_ECONOMY_ID):
    """
    For each economy, scenario, and fuels group (here '08_gas'), subtract the
    liquefaction/regasification losses (from rows with sectors '10_losses_and_own_use' 
    and sub2sectors '10_01_03_liquefaction_regasification_plants') from whichever is larger 
    between natural gas production (sectors '01_production', subfuels '08_01_natural_gas')
    and LNG imports (sectors '02_imports', subfuels '08_02_lng').

    This function is intended to be called immediately after the losses function.
    """
    if SINGLE_ECONOMY_ID == '15_PHL':
        breakpoint()#if economy is phlipines, need to make it so the gas use is coming from imports even thouh that is lower. 
    MANUAL_SETTINGS = {
        '15_PHL': {
            'sectors': '02_imports',
            'subfuels': '08_02_lng'
        }
    }
    # --- Identify losses rows ---
    losses_mask = (
        (gas_supply['sectors'] == '10_losses_and_own_use') &
        (gas_supply['sub2sectors'] == '10_01_03_liquefaction_regasification_plants') &
        (gas_supply['fuels'] == '08_gas')
    )
    losses_df = gas_supply[losses_mask].copy()
    if losses_df.empty:
        return gas_supply  # Nothing to do if no losses data

    # Sum losses by economy, scenarios, and fuels (losses rows usually have a common fuel)
    losses_sum = losses_df.groupby(['economy', 'scenarios', 'fuels', 'subfuels'], as_index=False)[years_to_keep_in_results_str].sum()

    # --- Identify production rows (natural gas) ---
    prod_mask = (
        (gas_supply['sectors'] == '01_production') &
        (gas_supply['subfuels'] == '08_01_natural_gas') &
        (gas_supply['fuels'] == '08_gas')
    )
    prod_df = gas_supply[prod_mask].copy()
    prod_group = prod_df.groupby(['economy', 'scenarios', 'fuels', 'subfuels'], as_index=False)[years_to_keep_in_results_str].sum()
    prod_group.rename(columns={y: f"{y}_prod" for y in years_to_keep_in_results_str}, inplace=True)

    # --- Identify import rows (LNG) ---
    imp_mask = (
        (gas_supply['sectors'] == '02_imports') &
        (gas_supply['subfuels'] == '08_02_lng') &
        (gas_supply['fuels'] == '08_gas')
    )
    imp_df = gas_supply[imp_mask].copy()
    imp_group = imp_df.groupby(['economy', 'scenarios', 'fuels', 'subfuels'], as_index=False)[years_to_keep_in_results_str].sum()
    imp_group.rename(columns={y: f"{y}_imp" for y in years_to_keep_in_results_str}, inplace=True)

    # --- Merge production and imports ---
    prod_imp = prod_group.merge(imp_group, on=['economy', 'scenarios', 'fuels', 'subfuels'], how='outer').fillna(0)
    # Merge the losses sums (which are per year) into the production/import data
    prod_imp = prod_imp.merge(losses_sum, on=['economy', 'scenarios', 'fuels', 'subfuels'], how='left').fillna(0)
    if SINGLE_ECONOMY_ID in MANUAL_SETTINGS.keys():
        breakpoint()
    # --- For each year, subtract losses from whichever is larger ---
    for y in years_to_keep_in_results_str:
        prod_col = f"{y}_prod"
        imp_col = f"{y}_imp"
        # Here, the losses adjustment for year y is in column y (from losses_sum merge)
        larger_mask = prod_imp[prod_col] >= prod_imp[imp_col]
        # Subtract from production if production is larger
        prod_imp.loc[larger_mask, prod_col] = (prod_imp.loc[larger_mask, prod_col] - prod_imp.loc[larger_mask, y]).clip(lower=0)
        # Otherwise, subtract from imports
        prod_imp.loc[~larger_mask, imp_col] = (prod_imp.loc[~larger_mask, imp_col] - prod_imp.loc[~larger_mask, y]).clip(lower=0)

    # --- Build updated production and import rows ---
    updated_prod = prod_imp[['economy', 'scenarios', 'fuels', 'subfuels']].copy()
    for y in years_to_keep_in_results_str:
        updated_prod[y] = prod_imp[f"{y}_prod"]
    updated_prod['sectors'] = '01_production'
    updated_prod['subfuels'] = '08_01_natural_gas'
    # (Fill in default values for sub-sector markers if needed)
    updated_prod['sub1sectors'] = 'x'
    updated_prod['sub2sectors'] = 'x'
    updated_prod['sub3sectors'] = 'x'
    updated_prod['sub4sectors'] = 'x'

    updated_imp = prod_imp[['economy', 'scenarios', 'fuels', 'subfuels']].copy()
    for y in years_to_keep_in_results_str:
        updated_imp[y] = prod_imp[f"{y}_imp"]
    updated_imp['sectors'] = '02_imports'
    updated_imp['subfuels'] = '08_02_lng'
    updated_imp['sub1sectors'] = 'x'
    updated_imp['sub2sectors'] = 'x'
    updated_imp['sub3sectors'] = 'x'
    updated_imp['sub4sectors'] = 'x'

    # --- Remove the old production and import rows from gas_supply ---
    gas_supply_no_old = gas_supply.merge(updated_prod, on=['economy', 'scenarios', 'sectors', 'fuels','subfuels'], how='outer', indicator=True, suffixes=('', '_x'))
    gas_supply_no_old = gas_supply_no_old[gas_supply_no_old['_merge'] == 'left_only'].copy()
    gas_supply_no_old.drop(columns=['_merge']+ [col for col in gas_supply_no_old.columns if '_x' in col], inplace=True)
    gas_supply_no_old = gas_supply_no_old.merge(updated_imp, on=['economy', 'scenarios', 'sectors', 'fuels','subfuels'], how='outer', indicator=True, suffixes=('', '_x'))
    gas_supply_no_old = gas_supply_no_old[gas_supply_no_old['_merge'] == 'left_only'].copy()
    gas_supply_no_old.drop(columns=['_merge']+ [col for col in gas_supply_no_old.columns if '_x' in col], inplace=True)
    
    # Append the updated rows
    final_gas_supply = pd.concat([gas_supply_no_old, updated_prod, updated_imp], ignore_index=True)

    return final_gas_supply
#%%

################################################
#GAS SUPPLY FUNCTIONS ABOVE
###############################################
# if os.getcwd() == 'c:\\Users\\finbar.maunsell\\github\\Outlook9th_EBT\\workflow\\scripts':
#     os.chdir('../../')

# # Load the pickled data
# with open('results_df.pkl', 'rb') as f:
#     results_df = pickle.load(f)
# with open('layout_df.pkl', 'rb') as f:
#     layout_df = pickle.load(f)
# with open('SINGLE_ECONOMY_ID.pkl', 'rb') as f:
#     SINGLE_ECONOMY_ID = pickle.load(f)
# with open('shared_categories.pkl', 'rb') as f:
#     shared_categories = pickle.load(f)
# with open('years_to_keep_in_results.pkl', 'rb') as f:
#     years_to_keep_in_results = pickle.load(f)
    
# # Call the main function to adjust gas supply data
# results_df = create_transformation_losses_pipeline_rows_for_gas_based_on_supply(results_df, layout_df, SINGLE_ECONOMY_ID, shared_categories, years_to_keep_in_results)

# %%


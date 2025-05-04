import os
import re
import pandas as pd 
import numpy as np
import glob
from datetime import datetime
from utility_functions import *
import yaml 

    
#and also we will need a fucntion for adding on supply for new data that is projected (over the whole projeciton year). this will also be sueful for making sure that demand and supply match > we can add on supply even for data that is not projected in this repo, just in case a supply modeller didnt do it exactly right.
#note that this will need to be the last step of any adjustments to the modelled data because otherwise we risk creating a loop where increasing demand leads to icnreased supply which leads to increased demand etc.

#this will work by iterating thrpough each fuel,subfuel and counting up the total rquired energy supply as the sum of demand sectors (14_industry_sector 15_transport_sector 16_other_sector), absolute of own use and non eenrgy use (17_nonenergy_use, 10_losses_and_own_use) and ohters like that and the minus of the sum of 09_total_transformation_sector  (so inputs (negatives made postivie) are treated as extra demnd and outputs (postives made negative) will take awayfrom required supply)

#we will then use the 07_total_primary_energy_supply and subtract the total required energy supply to get the total energy supply that is missing or extra. Then if we know there is extra supply, we can add that to 03_exports or minus from 01_production(depending on what is larger for that fuel in that econmoy.) or if there is missing supply we can add that to 02_imports or 01_production (depending on what is larger for that fuel in that econmoy.) - there is also the case where there is NO supply for a fuel in an economy, in this case let the user know and they can decide what to do (expecting 17_electricity to be in here and in this case it would require actually adding teh required electricity supply to the transformation sector but we'll do that in a separate funtion).

#there will be some cases where the value needs to change by a lot. We will identify these using the proportional differnece and let the user know in case they want to handle it manually.
    
def create_statistical_discrepancy_df(group_copy, economy, scenario, fuel, subfuel,year_columns):
    #create statistical discrepancies df in case we need to add some. 
    statistical_discrepancies = pd.DataFrame({'economy': [economy], 'scenarios': [scenario], 'fuels': [fuel], 'subfuels': [subfuel], 'sectors': ['11_statistical_discrepancy'], 'sub1sectors': ['x'], 'sub2sectors': ['x'], 'sub3sectors': ['x'], 'sub4sectors': ['x'], 'subtotal_results': [False]})
    for year in year_columns:
        statistical_discrepancies[year] = 0
    #double check it has the same cols as group copy when you ignore the year cols
    different_cols = set(statistical_discrepancies.columns) - set(group_copy.columns)
    different_cols = [col for col in different_cols if col not in year_columns]
    if different_cols:
        breakpoint()
        raise ValueError("Statistical discrepancies DataFrame does not match group DataFrame.")
    return statistical_discrepancies

def handle_statistical_discrepancies_for_transformation_output(group_copy, economy, scenario, fuel, subfuel, year,statistical_discrepancies_df, extra_supply_from_transformation_output, reported_supply, total_required, transfomation_supply, error_df):
    #we can add some shortcuts here to fix the inbalance if it matches specified cases
    SPECIFIED_ALLOWED_STATISTICAL_DISCREPANCIES = {
        # # will be tuples of economy, fuel, subfuel  that have statsitical discrepancies that are allowed:
        ('ALL', '02_coal_products', 'x'),#since there are a lot of statistical discreapncies in the coal products we'll just leave themsincethey are tough to untangle (would require adjusting transformation and in turn the supply of the fuel used in that transforamtion)
        ('ALL', '18_heat', 'x'),#heat is jsut difficult. its ok
        ##############################
        ('15_PHL', '16_others', '16_x_hydrogen'),#in 2031 there is a slight discrepancy. its a bit diffcult to adjust because we dont know the elec to hydrogen ratio. so we'll just leave it as is. also tbh could just get rid of hydrogen in air and replace with jet fuel.
        ('15_PHL', '16_others', '15_03_charcoal'),#i think we could have some way of adjusting the transcfomatioin output for these less important fuels but for now we'll just leave it as is

        ##############################
        ('09_ROK','07_petroleum_products','07_09_lpg'),#seems to be associated with changing own use and there not being any exports or imports to increase/decrease to handle the diff.. its ok,
        ('09_ROK','07_petroleum_products','07_10_refinery_gas_not_liquefied'),#seems to be associated with changing own use and there not being any exports or imports to increase/decrease to handle the diff.. its ok,
        ('09_ROK','07_petroleum_products','07_x_other_petroleum_products'),
        ('09_ROK','07_petroleum_products','07_03_naphtha'),
         #refining model seems to have soem inaccuracies sometimes but this was only in 2044 from what i could tell? need to check the stat discreps later
        ('09_ROK','08_gas',	'08_gas_unallocated'),#this si fixed and will need to be removed from this list a week after 10 march 2025
        ('09_ROK','17_x_green_electricity',	'x'),
        ##############################
        ('21_VN','15_solid_biomass', '15_03_charcoal'),
    }#note that a statisical discrepancy is negative if thereis extra supply, postivie if there is missing supply
    # breakpoint()
    OKAY=False
    
    #first, check if the difference is significant or its a div by 0 siutation, if not just add it as a discrepancy. else do a check 
    if total_required == 0:
        OKAY = False 
    elif abs(extra_supply_from_transformation_output) / abs(total_required) < 0.05:
        OKAY = True 
    else:
        OKAY = False
    if OKAY == False:
        for tuples in SPECIFIED_ALLOWED_STATISTICAL_DISCREPANCIES:
            if tuples[0] == 'ALL':
                #jsut assume its this econ
                if fuel == tuples[1] and subfuel == tuples[2]:
                    OKAY=True
                    break
            else:
                if economy == tuples[0] and fuel == tuples[1] and subfuel == tuples[2]:
                    OKAY=True
                    break
    if not OKAY:
        df_to_check = group_copy[['sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels', 'economy', 'scenarios'] + [year]].copy()
        #add the statistical discrepancy to the df
        # breakpoint()
        if '11_statistical_discrepancy' not in df_to_check['sectors'].unique():
            #create row for the statistical discrepancy
            df_to_check = pd.concat([df_to_check, pd.DataFrame({'sectors': ['11_statistical_discrepancy'], 'sub1sectors': ['x'], 'sub2sectors': ['x'], 'sub3sectors': ['x'], 'sub4sectors': ['x'], 'fuels': [fuel], 'subfuels': [subfuel], 'economy': [economy], 'scenarios': [scenario], year: -extra_supply_from_transformation_output})], ignore_index=True)
        else:
            df_to_check.loc[df_to_check['sectors'] == '11_statistical_discrepancy', year] = -extra_supply_from_transformation_output
        #save to data/temp/error_checking 
        
        #add to the df, we'll throw a error later.
        error_df = pd.merge(error_df, df_to_check, how='outer', on=['sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels', 'economy', 'scenarios'], suffixes=('', '_y'))
        
        #where we have _y's, check for nas in original col, if there is, save this value to a temp col, then replace the y col with na and repalce na in original col with teh value in temp, then delte _y cols which are full of nas
        for col in error_df.columns:
            if '_y' in str(col):
                col_no_y = col.replace('_y', '')
                temp_col = f'{col_no_y}_temp'
                error_df[temp_col] = error_df[col]
                #where the original col is na, replace the value in the _y col with na
                error_df.loc[error_df[col_no_y].isna(), col] = np.nan
                #where the orignal col is na, replace the value with the saved temp value (which was orignially in y col but was set to na)
                
                error_df.loc[error_df[col_no_y].isna(), col_no_y] = error_df[error_df[col_no_y].isna()][temp_col]
                #now delete the _y cols where all values are na, indicating that all roginals were replaced since they were na. and then delete the temp cols
                if error_df[col].isna().all():
                    error_df.drop(col, axis=1, inplace=True)
                else:
                    #set y col to have the temp col values
                    error_df[col] = error_df[temp_col]
                error_df.drop(temp_col, axis=1, inplace=True)
        # breakpoint()#where is the stat discin the error df?
        # df_to_check.to_csv(os.path.join('data', 'temp', 'error_checking', f'{economy}_extra_supply_from_transformation_output_{fuel}_{subfuel}_{year}.csv'), index=False)
        # breakpoint()    
        # raise ValueError(f"Supply of this fuel is too high, but specifically transformation output makes up so much of supply that to match demand, the transformation output would need to be decreased for {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year}. This is too complex to model here so I suggest adding it to the SPECIFIED_ALLOWED_STATISTICAL_DISCREPANCIES dict so the amount is treated as a statistical discrepancy, if its not too high.")   
    else:
        #set the statistical discrepancy to the negative of diff to indicate that there is extra supply
        #but also check there is a statistical discrepancy row, if so set it to the negative of diff, if there is not then add a row with the negative of diff
        if '11_statistical_discrepancy' in group_copy['sectors'].unique():
            group_copy.loc[group_copy['sectors'] == '11_statistical_discrepancy', year] = -extra_supply_from_transformation_output
        #also add the value to the statistical discrepancy df for this year
        statistical_discrepancies_df[year] = -extra_supply_from_transformation_output
    
    
    return statistical_discrepancies_df, group_copy, error_df     


def adjust_projected_supply_to_balance_demand(df, economy, ERRORS_DAIJOUBU=False,adjustment_threshold=0.01):
    """
    Adjust supply for projected data so that overall demand and supply balance.

    Two major effects (illustrated with the gas example):
    
      1. Increased coal use for transformation (extra supply required):
         - To meet increased domestic demand, first reduce exports (freeing up domestic supply).
         - If more supply is needed, increase domestic supply by either increasing production_for_domestic_use
           (i.e. production - exports) or imports. The choice is made by comparing production_for_domestic_use to imports:
             • If production_for_domestic_use is larger than imports, increase production.
             • Otherwise, increase imports.
             
      2. Decreased gas supply (less supply required):
         - To reduce domestic supply, first reduce imports.
         - If that’s not enough, then if no imports exist, try to export the surplus.
         - If neither imports nor exports are available, reduce production.
         
    In all cases, if the proportional difference (|diff|/|required_supply|) exceeds the adjustment_threshold,
    a warning is printed and the adjustment is logged.

    Error checks:
      - After reducing production or imports, if their values become negative an error is raised.
      - After increasing exports, if exports become positive an error is raised.
      - A breakpoint is set immediately before each raise error for debugging.

    Parameters:
      df (pd.DataFrame): DataFrame containing energy data in wide format with year columns (4-digit strings)
                         and columns including 'economy', 'scenarios', 'fuels', 'subfuels', and 'sectors'.
      OUTLOOK_BASE_YEAR (int or str): The base year (e.g., 2020). Projection years are those with year >= this.
      adjustment_threshold (float): If the proportional difference exceeds this value, a warning is issued.

    Returns:
      pd.DataFrame: The DataFrame with adjusted values for the supply-related sectors.
    """
    df_copy = df.copy()
    import re
    str_OUTLOOK_BASE_YEAR = str(OUTLOOK_BASE_YEAR)
    # Identify year columns (assumed to be 4-digit strings)
    year_columns = [col for col in df.columns if re.match(r'^\d{4}$', str(col))]
    # Use projection years (year >= OUTLOOK_BASE_YEAR)
    projection_years = [year for year in year_columns if int(year) > int(OUTLOOK_BASE_YEAR)]
    #filter for the projection years only
    #first fouble check the cols are as expected:
    if df.columns.difference(['economy', 'scenarios', 'fuels', 'subfuels', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'subtotal_results', 'subtotal_layout'] + year_columns).any():
        breakpoint()
        raise ValueError("Columns in DataFrame do not match expected columns.")
    
    df = df[['economy', 'scenarios', 'fuels', 'subfuels', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'subtotal_results'] + projection_years]

    error_df = pd.DataFrame(columns=['sectors','sub1sectors','sub2sectors','sub3sectors','sub4sectors','fuels','subfuels','economy','scenarios',OUTLOOK_BASE_YEAR+1])
    
    # Define sector names
    supply_sector = "07_total_primary_energy_supply"
    demand_sectors = {"14_industry_sector", "15_transport_sector", "16_other_sector"}
    own_nonenergy_sectors = {"17_nonenergy_use", "10_losses_and_own_use"}
    transformation_sector = "09_total_transformation_sector"
    production_sector = "01_production"
    exports_sector = "03_exports"
    imports_sector = "02_imports"
    bunkers_sectors = {"04_international_marine_bunkers", "05_international_aviation_bunkers"}

    # Log adjustments where the proportional difference is high.
    all_diffs = pd.DataFrame(columns=['economy', 'scenarios', 'fuels', 'subfuels', 'year', 'diff'])
    statistical_discrepancies_df_agg = pd.DataFrame(columns=['economy', 'scenarios', 'fuels', 'subfuels', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'subtotal_results'] + projection_years)
    # Group by key identifiers
    group_cols = ['economy', 'scenarios', 'fuels', 'subfuels']
    for key, group in df.groupby(group_cols, as_index=False):
        economy, scenario, fuel, subfuel = key
        if fuel in['20_total_renewables', '21_modern_renewables', '19_total']:
            continue
        # if fuel =='17_electricity' and subfuel == 'x':
        #     breakpoint()
        # Create a mask for this group and work on a copy
        group_mask = (df['economy'] == economy) & (df['scenarios'] == scenario) & (df['fuels'] == fuel) & (df['subfuels'] == subfuel) & (df['subtotal_results'] == False)
        group_copy = df.loc[group_mask].copy()
        statistical_discrepancies_df = create_statistical_discrepancy_df(group_copy, economy, scenario, fuel, subfuel,year_columns)
        for year in projection_years:
            #if everything is 0 then we will just skip this as there is no  data to work with here
            if group_copy[year].sum() == 0:
                continue
            
            # Compute total required supply:
            demand_val = group_copy[group_copy['sectors'].isin(demand_sectors)][year].sum()
            own_nonenergy_val = group_copy[group_copy['sectors'].isin(own_nonenergy_sectors)][year].abs().sum()
            # For transformation, negative values are inputs and postivie are essentially supply. So we need to grab the negative of the sum. If this is less than 0 we should treat it as extra supply. if it is positve we should treat it as extra demand. 
            # only consider negative values (multiplied by -1)
            trans_adjustment = - group_copy[group_copy['sectors'] == transformation_sector][year].sum()
            if trans_adjustment > 0:
                transformation_demand = trans_adjustment
                transfomation_supply = 0
            else:
                transformation_demand = 0
                transfomation_supply = abs(trans_adjustment)
            total_required = demand_val + own_nonenergy_val + transformation_demand

            # Get reported primary energy supply:
            supply_rows = group_copy[group_copy['sectors'] == supply_sector]
            if supply_rows.empty:
                breakpoint()
                raise ValueError(f"No primary supply data for {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year}.")
            reported_supply = supply_rows[year].sum() + transfomation_supply
            diff = reported_supply - total_required
            
            if diff == 0:
                continue
            
            # double check that we will not need to cahnge transformation output as that is too difficult and so we'll either  throw an error or add the amount as a statistical discrepancy
            # this would occur in the case where there is too much remaining fuel (so diff>0) but also the sum of abs(exports)+imports+production is < than the diff and transfomation_supply >0 (which would always be true if the other two are tru btw), indicating that we would need to decrease transformation output to cater for the extra supply. but we will not do this here
            ##INGORE THIS ONE BELOW since in that case you would just increase exports
            # or in the case where there is too much dmenad and exports (so diff<0) but the sum of abs(exports)+imports+production is < than the diff and transofrmation output > than the diff, inidciating that transformation output is the main source for this fuel, and its exports cannot be decreased to cater for its diff and therefore decreasing transformation output would be the best way to handle this - but we will not do this here
            ##INGORE THIS ONE ABOVE
            abs_supply_sum = abs(group_copy[group_copy['sectors'].isin([exports_sector, imports_sector, production_sector])][year]).sum()#note that this doesnt include bunkers. but they cant be changed so we shouldnt include them
            if diff > 0 and abs_supply_sum < diff:
                if transfomation_supply <= 0:
                    #throw an error. this is not expected:
                    breakpoint()
                    raise ValueError(f"Extra supply for {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year} but no transformation output to adjust.")
                else:
                    statistical_discrepancies_df, group_copy, error_df = handle_statistical_discrepancies_for_transformation_output(group_copy, economy, scenario, fuel, subfuel, year,statistical_discrepancies_df, diff, reported_supply, total_required, transfomation_supply, error_df)
                    continue#next year
            ADJUSTMENT_THRESHOLD_REACHED = False
            # Warn if the proportional difference is high.
            if total_required != 0:
                prop_diff = abs(diff) / abs(total_required)
                if prop_diff > adjustment_threshold:
                    ADJUSTMENT_THRESHOLD_REACHED = True
                    # print(f"Warning: For {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year} a large adjustment is needed (proportional difference: {prop_diff:.2f}).")
                    all_diffs = pd.concat([all_diffs, pd.DataFrame({
                        'economy': [economy],
                        'scenarios': [scenario],
                        'fuels': [fuel],
                        'subfuels': [subfuel],
                        'year': [year],
                        'diff': [diff],
                        'prop_diff': [prop_diff]
                    })], ignore_index=True)

            # --- CASE 1: Missing supply (extra supply required) → diff < 0 ---
            if diff < 0 and ADJUSTMENT_THRESHOLD_REACHED:
                needed = abs(diff)
                # First, reallocate from exports (reduce exports to free up domestic supply)
                exp_rows = group_copy[group_copy['sectors'] == exports_sector]
                exports_val = abs(exp_rows[year].sum()) if not exp_rows.empty else 0
                reduction_from_exports = min(needed, exports_val)
                if reduction_from_exports > 0:
                    group_copy.loc[group_copy['sectors'] == exports_sector, year] += reduction_from_exports#exports are negative so we add to them
                    new_exports = group_copy.loc[group_copy['sectors'] == exports_sector, year].sum()
                    if new_exports > 0:
                        breakpoint()
                        raise ValueError(f"After reducing exports, exports became positive for {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year}.")
                remaining_needed = needed - reduction_from_exports

                if remaining_needed > 0:
                    # Determine production_for_domestic_use = production - (exports after adjustment)
                    prod_rows = group_copy[group_copy['sectors'] == production_sector]
                    production_val = prod_rows[year].sum() if not prod_rows.empty else 0
                    new_exports_val = max(exports_val - reduction_from_exports, 0)
                    prod_domestic = production_val - new_exports_val
                    imp_rows = group_copy[group_copy['sectors'] == imports_sector]
                    imports_val = imp_rows[year].sum() if not imp_rows.empty else 0
                    # Rule: if production_for_domestic_use > imports, then increase production; otherwise, increase imports.
                    if prod_domestic > imports_val:
                        group_copy.loc[group_copy['sectors'] == production_sector, year] += remaining_needed
                        new_prod = group_copy.loc[group_copy['sectors'] == production_sector, year].sum()
                        if new_prod < 0:
                            breakpoint()
                            raise ValueError(f"Production became negative for {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year}.")
                    else:
                        group_copy.loc[group_copy['sectors'] == imports_sector, year] += remaining_needed
                        new_imp = group_copy.loc[group_copy['sectors'] == imports_sector, year].sum()
                        if new_imp < 0:
                            breakpoint()
                            raise ValueError(f"Imports became negative for {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year}.")

            # --- CASE 2: Surplus supply exists (less supply required) → diff > 0 ---
            elif diff > 0 and ADJUSTMENT_THRESHOLD_REACHED:
                surplus = diff
                # First, remove supply from imports (reduce imports to lower domestic supply)
                imp_rows = group_copy[group_copy['sectors'] == imports_sector]
                imports_val = imp_rows[year].sum() if not imp_rows.empty else 0
                reduction_from_imports = min(surplus, imports_val)
                if reduction_from_imports > 0:
                    group_copy.loc[group_copy['sectors'] == imports_sector, year] -= reduction_from_imports
                    new_imp = group_copy.loc[group_copy['sectors'] == imports_sector, year].sum()
                    if new_imp < 0:
                        breakpoint()
                        raise ValueError(f"After reducing imports, imports became negative for {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year}.")
                remaining_surplus = surplus - reduction_from_imports
                #udpate imports val in case we use in next step
                imports_val = group_copy.loc[group_copy['sectors'] == imports_sector, year].sum() if not group_copy[group_copy['sectors'] == imports_sector].empty else 0       
                        
                if remaining_surplus > 0:
                    # If no imports (or insufficient imports), then try to export the extra supply.
                    if imports_val == 0:
                        exp_rows = group_copy[group_copy['sectors'] == exports_sector]
                        exports_val = abs(exp_rows[year].sum()) if not exp_rows.empty else 0
                        if exports_val != 0:
                            group_copy.loc[group_copy['sectors'] == exports_sector, year] -= remaining_surplus
                            new_exports = group_copy.loc[group_copy['sectors'] == exports_sector, year].sum()
                            if new_exports > 0:
                                breakpoint()
                                raise ValueError(f"After increasing exports, exports became positive for {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year}.")
                        else:
                            group_copy.loc[group_copy['sectors'] == production_sector, year] -= remaining_surplus
                            new_prod = group_copy.loc[group_copy['sectors'] == production_sector, year].sum()
                            if new_prod < 0:
                                breakpoint()
                                raise ValueError(f"After reducing production, production became negative for {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year}.")
                    else:
                        group_copy.loc[group_copy['sectors'] == production_sector, year] -= remaining_surplus
                        new_prod = group_copy.loc[group_copy['sectors'] == production_sector, year].sum()
                        if new_prod < 0:
                            breakpoint()
                            raise ValueError(f"After reducing production, production became negative for {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year}.")

        # After processing all projection years for this group, update the original df rows.
        df.loc[group_copy.index] = group_copy
        if '11_statistical_discrepancy' not in group_copy['sectors'].unique():
            df = pd.concat([df, statistical_discrepancies_df], ignore_index=True)
        # add all the statistical_discrepancies_df to one too so we can track that in a csv later
        statistical_discrepancies_df_agg = pd.concat([statistical_discrepancies_df_agg, statistical_discrepancies_df], ignore_index=True)
        # error_df = pd.concat([error_df, statistical_discrepancies_df], ignore_index
        # =True)

    #change 09_total_transformation_sector_positive and 09_total_transformation_sector_negative to 09_total_transformation_sector
    df['sectors'] = df['sectors'].replace({'09_total_transformation_sector_positive': '09_total_transformation_sector', '09_total_transformation_sector_negative': '09_total_transformation_sector'})
    dateid = datetime.now().strftime("%Y%m%d")
    #save teh differences to a file in results\modelled_within_repo\final_data_adjustments
    all_diffs.to_csv(os.path.join('results', 'modelled_within_repo', 'final_data_adjustments', f'{economy}_{dateid}_supply_adjustments.csv'), index=False)
    
    statistical_discrepancies_df_agg.to_csv(os.path.join('results', 'modelled_within_repo', 'final_data_adjustments', f'{economy}_{dateid}_statistical_discrepancies.csv'), index=False)
    if len(error_df) > 0:
        #order the cols in error df so we have non years first and then the years are in order
        ordered_columns = [col for col in ['sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels', 'economy', 'scenarios'] + [OUTLOOK_BASE_YEAR+1] + sorted(projection_years) if col in error_df.columns]
        projection_years_in_error_df = [col for col in sorted(projection_years) if col in error_df.columns]
        missing_cols = sorted(list(set(error_df.columns) - set(ordered_columns)))
        ordered_columns += missing_cols
        error_df = error_df[ordered_columns]
        #and drop rows thatare all 0s and nas for the years and missing columns
        # breakpoint()
        error_df = error_df.loc[~(error_df[sorted(projection_years_in_error_df+missing_cols)].isna()| error_df[sorted(projection_years_in_error_df+missing_cols)].eq(0)).all(axis=1)]
        
        error_df.to_csv(os.path.join('data', 'temp', 'error_checking', f'{economy}_statistical_discrepancies.csv'), index=False)
        if not ERRORS_DAIJOUBU:
            breakpoint()
            raise ValueError(f"Errors where transformation output is too high are found in data checks. The extra output is suggested to be put in statistical discrepancies. See {os.path.join('data', 'temp', 'error_checking', f'{economy}_statistical_discrepancies.csv')} for more details. If you are okay with the discrepancies, add them to the SPECIFIED_ALLOWED_STATISTICAL_DISCREPANCIES dict in the function  adjust_projected_supply_to_balance_demand.")
    
    return df

def make_manual_changes_to_rows(final_energy_df, economy, scenario):
    #this will just be a messy function to make manual changes to the finaldata as is needed. Probelm is that these could be all sorts of little changes so i think it will be easier to have this be quite messy and just use whatever code we need to make the changes. just make sure to clearly label the changes and the reasons for them.
    
    ##########
    #ROK shift all nautral gas imports to lng for projected years#this is caused by the adjust projected supply to balance demand function which sees there is not enough gas to supply the demand and so it creates imports of gas, even though technically they should be lng. Could think of no other way t make this change and have it useful for other similar cases.
    if economy == '09_ROK':
        
        gas_imports  = final_energy_df.loc[(final_energy_df['subfuels'] == '08_01_natural_gas') & (final_energy_df['sectors'] == '02_imports') & (final_energy_df['economy'] == economy) & (final_energy_df['scenarios'] == scenario)]
        #get the years that are projected
        projected_years = [str(year) for year in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1)]
        gas_imports = gas_imports[projected_years].copy()
        #checl gas imports is > 0 
        if gas_imports[projected_years].sum().sum() > 0:
            #then set lng imports to have added gas imports for the projected years
            final_energy_df.loc[(final_energy_df['subfuels'] == '08_02_lng') & (final_energy_df['sectors'] == '02_imports') & (final_energy_df['economy'] == economy) & (final_energy_df['scenarios'] == scenario), projected_years] += gas_imports[projected_years].values
            #set gas imports to be 0 for the projected years
            final_energy_df.loc[(final_energy_df['subfuels'] == '08_01_natural_gas') & (final_energy_df['sectors'] == '02_imports') & (final_energy_df['economy'] == economy) & (final_energy_df['scenarios'] == scenario), projected_years] = 0
    
    return final_energy_df
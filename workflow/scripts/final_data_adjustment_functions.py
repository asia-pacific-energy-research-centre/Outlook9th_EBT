import os
import re
import pandas as pd
import numpy as np
import glob
from datetime import datetime
from utility_functions import *
import yaml
import merging_functions
import plotly.express as px

#and also we will need a fucntion for adding on supply for new data that is projected (over the whole projeciton year). this will also be sueful for making sure that demand and supply match > we can add on supply even for data that is not projected in this repo, just in case a supply modeller didnt do it exactly right.
#note that this will need to be the last step of any adjustments to the modelled data because otherwise we risk creating a loop where increasing demand leads to icnreased supply which leads to increased demand etc.

#this will work by iterating thrpough each fuel,subfuel and counting up the total rquired energy supply as the sum of demand sectors (14_industry_sector 15_transport_sector 16_other_sector), absolute of own use and non eenrgy use (17_nonenergy_use, 10_losses_and_own_use) and ohters like that and the minus of the sum of 09_total_transformation_sector  (so inputs (negatives made postivie) are treated as extra demnd and outputs (postives made negative) will take awayfrom required supply)

#we will then use the 07_total_primary_energy_supply and subtract the total required energy supply to get the total energy supply that is missing or extra. Then if we know there is extra supply, we can add that to 03_exports or minus from 01_production(depending on what is larger for that fuel in that econmoy.) or if there is missing supply we can add that to 02_imports or 01_production (depending on what is larger for that fuel in that econmoy.) - there is also the case where there is NO supply for a fuel in an economy, in this case let the user know and they can decide what to do (expecting 17_electricity to be in here and in this case it would require actually adding teh required electricity supply to the transformation sector but we'll do that in a separate funtion).

#there will be some cases where the value needs to change by a lot. We will identify these using the proportional differnece and let the user know in case they want to handle it manually.
def create_demand_supply_discrepancy_rows(group_copy, economy, scenario, fuel, subfuel,year_columns):
    #first check we dont already ahve a statistical discrepancy for this fuel/subfuel
    if group_copy.loc[group_copy['sectors'] == '22_demand_supply_discrepancy'].shape[0] > 0:
        # return group_copy
        # #if we already have a statistical discrepancy for this year, just use that
        demand_supply_discrepancies = group_copy.loc[group_copy['sectors'] == '22_demand_supply_discrepancy'].copy()
    else:
        #create statistical discrepancies df in case we need to add some.
        demand_supply_discrepancies = pd.DataFrame({'economy': [economy], 'scenarios': [scenario], 'fuels': [fuel], 'subfuels': [subfuel], 'sectors': ['22_demand_supply_discrepancy'], 'sub1sectors': ['x'], 'sub2sectors': ['x'], 'sub3sectors': ['x'], 'sub4sectors': ['x'], 'subtotal_results': [False]})
        for year in year_columns:
            demand_supply_discrepancies[year] = 0
        #double check it has the same cols as group copy when you ignore the year cols
        different_cols = set(demand_supply_discrepancies.columns) - set(group_copy.columns)
        different_cols = [col for col in different_cols if col not in year_columns]
        if different_cols:
            breakpoint()
            raise ValueError("22_demand_supply_discrepancy discrepancies DataFrame does not match group DataFrame.")
        
    return demand_supply_discrepancies

def create_statistical_discrepancy_rows(group_copy, economy, scenario, fuel, subfuel,year_columns):
    #first check we dont already ahve a statistical discrepancy for this fuel/subfuel
    if group_copy.loc[group_copy['sectors'] == '11_statistical_discrepancy'].shape[0] > 0:
        return group_copy
        # #if we already have a statistical discrepancy for this year, just use that
        # statistical_discrepancies = group_copy.loc[group_copy['sectors'] == '11_statistical_discrepancy'].copy()
    else:
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
        
        #then concat it to the group copy
        group_copy = pd.concat([group_copy, statistical_discrepancies], ignore_index=True)
    return group_copy

def handle_demand_supply_discrepancies_for_transformation_output(group_copy, economy, scenario, fuel, subfuel, year,demand_supply_discrepancies_df, extra_supply_from_transformation_output, reported_supply, total_required, transfomation_supply, error_df):
    #we can add some shortcuts here to fix the inbalance if it matches specified cases
    SPECIFIED_ALLOWED_DEMAND_SUPPLY_DISCREPANCIES = {
        # # will be tuples of economy, fuel, subfuel  that have statsitical discrepancies that are allowed:
        # # ('ALL', '02_coal_products', 'x'),#since there are a lot of statistical discreapncies in the coal products we'll just leave themsincethey are tough to untangle (would require adjusting transformation and in turn the supply of the fuel used in that transforamtion)
        # # ('ALL', '18_heat', 'x'),#heat is jsut difficult. its ok
        ###############################note i double commented everything in this dict to test what happens if we remove them on 5/20/2025
        # # ('15_PHL', '16_others', '16_x_hydrogen'),#in 2031 there is a slight discrepancy. its a bit diffcult to adjust because we dont know the elec to hydrogen ratio. so we'll just leave it as is. also tbh could just get rid of hydrogen in air and replace with jet fuel.
        # # ('15_PHL', '16_others', '15_03_charcoal'),#i think we could have some way of adjusting the transcfomatioin output for these less important fuels but for now we'll just leave it as is
        ##############################
        # # ('02_BD', '17_electricity', 'x'),#its quite small and we cant really do anything about it at this stage.
        ##############################
        # # ('09_ROK','07_petroleum_products','07_09_lpg'),#seems to be associated with changing own use and there not being any exports or imports to increase/decrease to handle the diff.. its ok,
        # # ('09_ROK','07_petroleum_products','07_10_refinery_gas_not_liquefied'),#seems to be associated with changing own use and there not being any exports or imports to increase/decrease to handle the diff.. its ok,
        # # ('09_ROK','07_petroleum_products','07_x_other_petroleum_products'),
        # # ('09_ROK','07_petroleum_products','07_03_naphtha'),
        # #  #refining model seems to have soem inaccuracies sometimes but this was only in 2044 from what i could tell? need to check the stat discreps later
        # # ('09_ROK','08_gas',	'08_gas_unallocated'),#this si fixed and will need to be removed from this list a week after 10 march 2025
        # # ('09_ROK','17_x_green_electricity',	'x'),
        ('09_ROK','06_x_other_hydrocarbons','x'),
        ##############################
        # # ('21_VN','15_solid_biomass', '15_03_charcoal'),
        
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
        for tuples in SPECIFIED_ALLOWED_DEMAND_SUPPLY_DISCREPANCIES:
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
        #add the demand supply discrepancy to the df
        # breakpoint()
        if '22_demand_supply_discrepancy' not in df_to_check['sectors'].unique():
            #create row for the demand supply discrepancy
            df_to_check = pd.concat([df_to_check, pd.DataFrame({'sectors': ['22_demand_supply_discrepancy'], 'sub1sectors': ['x'], 'sub2sectors': ['x'], 'sub3sectors': ['x'], 'sub4sectors': ['x'], 'fuels': [fuel], 'subfuels': [subfuel], 'economy': [economy], 'scenarios': [scenario], year: -extra_supply_from_transformation_output})], ignore_index=True)
        else:
            df_to_check.loc[df_to_check['sectors'] == '22_demand_supply_discrepancy', year] -= extra_supply_from_transformation_output
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
        #but also check there is a demand supply discrepancy row, if so set it to the negative of diff, if there is not then add a row with the negative of diff
        if '22_demand_supply_discrepancy' in group_copy['sectors'].unique():
            group_copy.loc[group_copy['sectors'] == '22_demand_supply_discrepancy', year] -= extra_supply_from_transformation_output
        #also add the value to the demand supply discrepancy df for this year
        demand_supply_discrepancies_df[year] -= extra_supply_from_transformation_output


    return demand_supply_discrepancies_df, group_copy, error_df


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
    transfers_sector = "08_transfers"
    statistical_discrepancies_sector = "11_statistical_discrepancy"
    bunkers_sectors = ["04_international_marine_bunkers", "05_international_aviation_bunkers"]
    FUELS_TO_NOT_IMPORT_EXPORT_PRODUCE = ['17_electricity', '18_heat']#these are fuels we dont want to import/export/produce... FOR EXAMPLE, we dont want to import electricity since we by default want to prodce it from the grid.. even if the economy does import electricity.. its jsut bad to be automatically choosing to saitsy imbalances by importing it

    ECONOMIES_TO_SKIP_FOR_BALANCING_FOR_CERTAIN_FUELS = {
        '20_USA': ['06_crude_oil_and_ngl'],  # these are fuels we dont want to import/export/produce... FOR EXAMPLE, we dont want to import electricity since we by default want to prodce it from the grid.. even if the economy does import electricity.. its jsut bad to be automatically choosing to saitsy imbalances by importing it
    }  # we will still do the calcaultions in this fuinciton but will return the original df
    # ECONOMIES_TO_SKIP

    # Log adjustments where the proportional difference is high.
    all_diffs = pd.DataFrame(columns=['economy', 'scenarios', 'fuels', 'subfuels', 'year', 'diff'])
    demand_supply_discrepancies_df_agg = pd.DataFrame(columns=['economy', 'scenarios', 'fuels', 'subfuels', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'subtotal_results'] + projection_years)
    # Group by key identifiers
    group_cols = ['economy', 'scenarios', 'fuels', 'subfuels']
    # breakpoint()#ref exports of crude just disappeard?
    for key, group in df.groupby(group_cols, as_index=False):
        economy, scenario, fuel, subfuel = key
        if fuel in['20_total_renewables', '21_modern_renewables', '19_total']:
            continue
        if economy in ECONOMIES_TO_SKIP_FOR_BALANCING_FOR_CERTAIN_FUELS.keys():
            if fuel in ECONOMIES_TO_SKIP_FOR_BALANCING_FOR_CERTAIN_FUELS[economy]:
                breakpoint()
                continue#skipping this one
        # if fuel =='17_electricity' and subfuel == 'x':
        #     breakpoint()
        # Create a mask for this group and work on a copy
        group_mask = (df['economy'] == economy) & (df['scenarios'] == scenario) & (df['fuels'] == fuel) & (df['subfuels'] == subfuel) & (df['subtotal_results'] == False)
        group_copy = df.loc[group_mask].copy()
        group_copy = create_statistical_discrepancy_rows(group_copy, economy, scenario, fuel, subfuel,year_columns)
        demand_supply_discrepancies_df = create_demand_supply_discrepancy_rows(group_copy, economy, scenario, fuel, subfuel,year_columns)
        total_required = 0
        for year in projection_years:
            # if int(year) >= 2056 and subfuel == '16_x_ammonia' and scenario == 'target':
            #     breakpoint()
            #if everything is 0 then we will just skip this as there is no  data to work with here
            if group_copy[year].sum() == 0:
                continue

            # Compute total required supply:
            demand_val = group_copy[group_copy['sectors'].isin(demand_sectors)][year].sum()

            bunkers_val = group_copy[group_copy['sectors'].isin(bunkers_sectors)][year].abs().sum()
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
            total_required = demand_val + own_nonenergy_val + transformation_demand + bunkers_val#add bunkers val since itS bascially a form of demand and we want to get a better picture of the demand for this fuel

            # Get reported primary energy supply:
            supply_rows = group_copy[group_copy['sectors'] == supply_sector]
            transfers = group_copy[group_copy['sectors'] == transfers_sector]
            pre_exisiting_statistical_discrepancies = group_copy[group_copy['sectors'] == statistical_discrepancies_sector][year].sum()
            if supply_rows.empty:
                breakpoint()
                raise ValueError(f"No primary supply data for {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year}.")
            reported_supply = supply_rows[year].sum() + transfomation_supply + transfers[year].sum() + pre_exisiting_statistical_discrepancies + bunkers_val#add bunkers val since it orignally took away from the supply
            diff = reported_supply - total_required

            if diff == 0:
                continue

            # double check that we will not need to cahnge transformation output as that is too difficult and so we'll either  throw an error or add the amount as a statistical discrepancy
            # this would occur in the case where there is too much remaining fuel (so diff>0) but also the sum of abs(exports)+imports+production is < than the diff and transfomation_supply >0 (which would always be true if the other two are tru btw), indicating that we would need to decrease transformation output to cater for the extra supply. but we will not do this here
            ##INGORE THIS ONE BELOW since in that case you would just increase exports
            # or in the case where there is too much dmenad and exports (so diff<0) but the sum of abs(exports)+imports+production is < than the diff and transofrmation output > than the diff, inidciating that transformation output is the main source for this fuel, and its exports cannot be decreased to cater for its diff and therefore decreasing transformation output would be the best way to handle this - but we will not do this here
            ##INGORE THIS ONE ABOVE
            abs_supply_sum = abs(group_copy[group_copy['sectors'].isin([exports_sector, imports_sector, production_sector])][year]).sum()##note we dont include  + pre_exisiting_statistical_discrepancies + transfers[year].sum() + bunkers_sectors in these because we are looking for the sum of supply that is available to be used to adjust the diff. We dont want to adjust those sources of supply so we dont want to include them in the abs_supply_sum
            if total_required == 0:
                # If total required is 0, we can't calculate a proportional difference.
                prop_diff = 0
            else:
                prop_diff = abs(diff) / abs(total_required)
            if (diff > 0 and abs_supply_sum < diff) or (prop_diff > adjustment_threshold and fuel in FUELS_TO_NOT_IMPORT_EXPORT_PRODUCE and total_required != 0):
                if fuel =='16_others' and subfuel == '16_x_hydrogen' and economy == '15_PHL' and diff > 10 and scenario == 'target':
                    breakpoint()#whats going on with phl hdyrogen. check the numbers are correct.
                # if transfomation_supply <= 0:
                #     #throw an error. this is not expected:
                #     breakpoint()
                #     raise ValueError(f"Extra supply for {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year} but no transformation output to adjust.")
                # else:
                demand_supply_discrepancies_df, group_copy, error_df = handle_demand_supply_discrepancies_for_transformation_output(group_copy, economy, scenario, fuel, subfuel, year,demand_supply_discrepancies_df, diff, reported_supply, total_required, transfomation_supply, error_df)
                continue#next year

            REDUCTION_FROM_EXPORTS = 0
            REDUCTION_FROM_IMPORTS = 0
            REDUCTION_FROM_PRODUCTION = 0
            INCREASE_IN_EXPORTS = 0
            INCREASE_IN_PRODUCTION = 0
            imports_val = 0
            exports_val = 0
            production_val = 0

            # # if fuel =='17_electricity':
            # #     breakpoint()#check how we can ignore it and make it handled by stat discrepancies
            ADJUSTMENT_THRESHOLD_REACHED = False
            # # Warn if the proportional difference is high.
            if total_required != 0:
                if prop_diff > adjustment_threshold:
                    ADJUSTMENT_THRESHOLD_REACHED = True
            #         # print(f"Warning: For {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year} a large adjustment is needed (proportional difference: {prop_diff:.2f}).")
            #         all_diffs = pd.concat([all_diffs, pd.DataFrame({
            #             'economy': [economy],
            #             'scenarios': [scenario],
            #             'fuels': [fuel],
            #             'subfuels': [subfuel],
            #             'year': [year],
            #             'change in supply': [diff],
            #             'proportion of demand': [prop_diff]
            #         })], ignore_index=True)

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
                    REDUCTION_FROM_EXPORTS = reduction_from_exports
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
                        INCREASE_IN_PRODUCTION = remaining_needed
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
                    REDUCTION_FROM_IMPORTS = reduction_from_imports
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
                            INCREASE_IN_EXPORTS = remaining_surplus
                            if new_exports > 0:
                                breakpoint()
                                raise ValueError(f"After increasing exports, exports became positive for {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year}.")
                        else:
                            group_copy.loc[group_copy['sectors'] == production_sector, year] -= remaining_surplus
                            new_prod = group_copy.loc[group_copy['sectors'] == production_sector, year].sum()
                            REDUCTION_FROM_PRODUCTION = -remaining_surplus
                            if new_prod < 0:
                                breakpoint()
                                raise ValueError(f"After reducing production, production became negative for {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year}.")
                    else:
                        group_copy.loc[group_copy['sectors'] == production_sector, year] -= remaining_surplus
                        new_prod = group_copy.loc[group_copy['sectors'] == production_sector, year].sum()
                        REDUCTION_FROM_PRODUCTION = -remaining_surplus
                        if new_prod < 0:
                            breakpoint()
                            raise ValueError(f"After reducing production, production became negative for {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year}.")

        # After processing all projection years for this group, update the original df rows.
        #first drop the rows for group_mask then add the new rows
        df = df.loc[~group_mask]
        #then add the new rows
        df = pd.concat([df, group_copy], ignore_index=True)
        #then add the demand supply discrepancies df if there are any
        if '22_demand_supply_discrepancy' not in group_copy['sectors'].unique() and len(demand_supply_discrepancies_df) > 0:
            df = pd.concat([df, demand_supply_discrepancies_df], ignore_index=True)
        # add all the demand_supply_discrepancies_df to one too so we can track that in a csv later
        demand_supply_discrepancies_df_agg = pd.concat([demand_supply_discrepancies_df_agg, demand_supply_discrepancies_df], ignore_index=True)
        # error_df = pd.concat([error_df, statistical_discrepancies_df], ignore_index
        # =True)

        #reocrd the change if it was large enough
        ADJUSTMENT_THRESHOLD_REACHED = False
        # Warn if the proportional difference is high.
        if total_required != 0:

            if prop_diff > adjustment_threshold:
                ADJUSTMENT_THRESHOLD_REACHED = True
                # print(f"Warning: For {economy}, {scenario}, fuel {fuel}, subfuel {subfuel} in year {year} a large adjustment is needed (proportional difference: {prop_diff:.2f}).")
                all_diffs = pd.concat([all_diffs, pd.DataFrame({
                    'economy': [economy],
                    'scenarios': [scenario],
                    'fuels': [fuel],
                    'subfuels': [subfuel],
                    'year': [year],
                    'change in supply': [diff],
                    'proportion of demand': [prop_diff],
                    'reduction from exports': [REDUCTION_FROM_EXPORTS],
                    'reduction from imports': [REDUCTION_FROM_IMPORTS],
                    'reduction from production': [REDUCTION_FROM_PRODUCTION],
                    'increase in production': [INCREASE_IN_PRODUCTION],
                    'increase in exports': [INCREASE_IN_EXPORTS],
                    'previous imports': [imports_val],
                    'previous exports': [exports_val],
                    'previous production': [production_val]
                })], ignore_index=True)
    #change 09_total_transformation_sector_positive and 09_total_transformation_sector_negative to 09_total_transformation_sector
    df['sectors'] = df['sectors'].replace({'09_total_transformation_sector_positive': '09_total_transformation_sector', '09_total_transformation_sector_negative': '09_total_transformation_sector'})
    dateid = datetime.now().strftime("%Y%m%d")
    #save teh differences to a file in results\modelled_within_repo\final_data_adjustments

    #find fuels in all_diffs that are in ECONOMIES_TO_SKIP_FOR_BALANCING_FOR_CERTAIN_FUELS and set their 'USED' to 'FALSE' and True if not.
    if economy in ECONOMIES_TO_SKIP_FOR_BALANCING_FOR_CERTAIN_FUELS.keys():
        fuels_to_skip = all_diffs['fuels'].unique()
        all_diffs['USED'] = True
        all_diffs.loc[all_diffs['fuels'].isin(fuels_to_skip), 'USED'] = False
        
    all_diffs.to_csv(os.path.join('results', 'modelled_within_repo', 'final_data_adjustments', f'{economy}_{dateid}_supply_adjustments.csv'), index=False)
    # breakpoint()#why does solar and gas have weird adjustements for brunei
    #save the demand supply discrepancies to a file in results\modelled_within_repo\final_data_adjustments
    demand_supply_discrepancies_df_agg.to_csv(os.path.join('results', 'modelled_within_repo', 'final_data_adjustments', f'{economy}_{dateid}_demand_supply_discrepancies.csv'), index=False)
    # breakpoint()#create plottingscripts
    plot_changes_made_to_supply(df, df_copy, all_diffs, economy, scenario, '', ECONOMIES_TO_SKIP_FOR_BALANCING_FOR_CERTAIN_FUELS)
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
        
        error_df.to_csv(os.path.join('data', 'temp', 'error_checking', f'{economy}_demand_supply_discrepancies.csv'), index=False)
        plot_demand_supply_discrepancies(error_df, economy, scenario, '', ECONOMIES_TO_SKIP_FOR_BALANCING_FOR_CERTAIN_FUELS)
        if not ERRORS_DAIJOUBU:
            breakpoint()
            raise ValueError(f"Errors where transformation output is too high are found in data checks. The extra output is suggested to be put in demand supply discrepancies. See {os.path.join('data', 'temp', 'error_checking', f'{economy}_demand_supply_discrepancies.csv')} for more details. If you are okay with the discrepancies, add them to the SPECIFIED_ALLOWED_DEMAND_SUPPLY_DISCREPANCIES dict in the function  adjust_projected_supply_to_balance_demand.")

    # if economy in ECONOMIES_TO_SKIP_FOR_BALANCING_FOR_CERTAIN_FUELS:
    #     # breakpoint()
    #     #drop all historical years and the subtotal_layout col
    #     historical_year_cols = [col for col in df_copy.columns if re.match(r'^\d{4}$', str(col)) and int(col) <= OUTLOOK_BASE_YEAR]
    #     df_copy.drop(columns=['subtotal_layout']+historical_year_cols, inplace=True)
    #     # df_copy = df_copy[df_copy.]
    #     return df_copy
    # else:
    return df


def clean_data_for_plotting(df, df_copy, all_diffs, economy, scenario):
    #first filter the data:
    adjusted_data_plot = df.copy()
    #drop , '11_statistical_discrepancy' since it is plotted separately
    adjusted_data_plot = adjusted_data_plot[~adjusted_data_plot['sectors'].isin(['22_demand_supply_discrepancy'])]
    # (adjusted_data_plot['sectors'].isin(['01_production', '02_imports', '03_exports'])) & (adjusted_data_plot['sectors'].isin(['01_production', '02_imports', '03_exports'])) & 
    adjusted_data_plot = adjusted_data_plot[(adjusted_data_plot['fuels'].isin(all_diffs['fuels'].unique())) & (adjusted_data_plot['subfuels'].isin(all_diffs['subfuels'].unique()))]
    future_year_cols = [col for col in adjusted_data_plot.columns if re.match(r'^\d{4}$', str(col)) and int(col) > OUTLOOK_BASE_YEAR]
    historical_year_cols = [col for col in adjusted_data_plot.columns if re.match(r'^\d{4}$', str(col)) and int(col) <= OUTLOOK_BASE_YEAR]
    # Filter out the year columns
    # breakpoint()#subtotal_layout'] not in index"
    adjusted_data_plot_future = adjusted_data_plot[['economy', 'scenarios', 'fuels', 'subfuels', 'sectors', 'subtotal_results'] + future_year_cols]
    # adjusted_data_plot_historical = adjusted_data_plot[['economy', 'scenarios', 'fuels', 'subfuels', 'sectors', 'subtotal_layout'] + historical_year_cols]
    #filter for only subtotals = false
    adjusted_data_plot_future = adjusted_data_plot_future[adjusted_data_plot_future['subtotal_results'] == False].drop(columns=['subtotal_results'])
    # adjusted_data_plot_historical = adjusted_data_plot_historical[adjusted_data_plot_historical['subtotal_layout'] == False].drop(columns=['subtotal_layout'])
    #combine the two dataframes
    adjusted_data_plot = adjusted_data_plot_future.copy()#pd.concat([, adjusted_data_plot_historical], ignore_index=True)
    #melt
    adjusted_data_plot_melt = adjusted_data_plot.melt(
        id_vars=['economy', 'scenarios', 'fuels', 'subfuels', 'sectors'],
        value_vars=future_year_cols,
        var_name='year',
        value_name='value'
    )
    adjusted_data_plot_melt['year'] = adjusted_data_plot_melt['year'].astype(int)
    #and now do that for the original data
    original_data_plot = df_copy.copy()
    original_data_plot = original_data_plot[(original_data_plot['fuels'].isin(all_diffs['fuels'].unique())) & (original_data_plot['subfuels'].isin(all_diffs['subfuels'].unique()))]
    original_data_plot_future = original_data_plot[['economy', 'scenarios', 'fuels', 'subfuels', 'sectors', 'subtotal_results'] + future_year_cols]
    original_data_plot_historical = original_data_plot[['economy', 'scenarios', 'fuels', 'subfuels', 'sectors', 'subtotal_layout'] + historical_year_cols]
    #filter for only subtotals = false
    original_data_plot_future = original_data_plot_future[original_data_plot_future['subtotal_results'] == False].drop(columns=['subtotal_results'])
    original_data_plot_historical = original_data_plot_historical[original_data_plot_historical['subtotal_layout'] == False].drop(columns=['subtotal_layout'])
    #combine the two dataframes
    original_data_plot = pd.concat([original_data_plot_future, original_data_plot_historical], ignore_index=True)
    #melt
    original_data_plot_melt = original_data_plot.melt(
        id_vars=['economy', 'scenarios', 'fuels', 'subfuels', 'sectors'],
        value_vars=historical_year_cols + future_year_cols,
        var_name='year',
        value_name='value'
    )
    original_data_plot_melt['year'] = original_data_plot_melt['year'].astype(int)
    
    ##############################

    # # concat them
    # adjusted_data_plot_melt['dataset'] = 'adjusted'
    # original_data_plot_melt['dataset'] = 'original'
    #drop nas in value cols in both dfs
    
    # breakpoint()
    adjusted_data_plot_melt = adjusted_data_plot_melt.dropna(subset=['value'])
    original_data_plot_melt = original_data_plot_melt.dropna(subset=['value'])
    #sum it all up by the cols just so we dont ahve too maby rows
    adjusted_data_plot_melt = adjusted_data_plot_melt.groupby(['economy', 'scenarios', 'fuels', 'subfuels', 'sectors', 'year'], as_index=False).sum()
    original_data_plot_melt = original_data_plot_melt.groupby(['economy', 'scenarios', 'fuels', 'subfuels', 'sectors', 'year'], as_index=False).sum()
    # breakpoint()
    #join using a merge
    combined_data = pd.merge(adjusted_data_plot_melt, original_data_plot_melt, on=['economy', 'scenarios', 'fuels', 'subfuels', 'sectors', 'year'], suffixes=('_adjusted', '_original'), how='outer')
    #where oriignal is the same as adjusted, set the adjusted to nan. then melt so we have a long format
    #set nans to 0
    combined_data['value_adjusted'] = combined_data['value_adjusted'].fillna(0)
    combined_data['value_original'] = combined_data['value_original'].fillna(0)
    combined_data.loc[combined_data['value_adjusted'] == combined_data['value_original'], 'value_adjusted'] = np.nan
    # breakpoint()
    #and now we can melt the data so we have a long format
    combined_data = combined_data.melt(
        id_vars=['economy', 'scenarios', 'fuels', 'subfuels', 'sectors', 'year'],
        value_vars=['value_adjusted', 'value_original'],
        var_name='dataset',
        value_name='value'
    )
    #rename value adjusted to adjusted and value_original to original
    combined_data['dataset'] = combined_data['dataset'].replace({'value_adjusted': 'adjusted', 'value_original': 'original'})
    #and now we can drop the value cols that are all nans
    combined_data = combined_data.dropna(subset=['value'])
    # combined_data = pd.concat([adjusted_data_plot_melt, original_data_plot_melt], ignore_index=True)
    #sum it all up by the cols just so we dont ahve duplicates
    combined_data = combined_data.groupby(['economy', 'scenarios', 'fuels', 'subfuels', 'sectors', 'year', 'dataset'], as_index=False).sum()
    
    #drop wher ethe sectris in 12_total_final_consumption 13_total_final_consumption 18_electricity_output_in_gwh, 19_heat_output_in_pj
    combined_data = combined_data[~combined_data['sectors'].isin(['12_total_final_consumption', '13_total_final_energy_consumption', '18_electricity_output_in_gwh', '19_heat_output_in_pj', '07_total_primary_energy_supply'])]
    #drop anhitorical years for adjusted
    combined_data = combined_data[~((combined_data['year'] <= OUTLOOK_BASE_YEAR) & (combined_data['dataset'] == 'adjusted'))]
    # if '11_statistical_discrepancy' in combined_data['sectors'].unique():
    #     breakpoint()#whats going on wiuth this. why is it showing in adjusted col? is it cause of nas?
    # breakpoint()
    return combined_data


def plot_changes_made_to_supply(df, df_copy, all_diffs, economy, scenario, USED_IN_RESULTS, ECONOMIES_TO_SKIP_FOR_BALANCING_FOR_CERTAIN_FUELS):
    """
    Plot the net change in supply (diff) for every fuel/subfuel over the projection years
    using Plotly Express, with facets by fuel (columns) and subfuel (rows).
    """
    #go through each unique scneario then each fuel and subfuel in all_diffs and then plot their exports/imports/production from df and df_copy

    ################################
    # breakpoint()#whay do we sometimes get stat discrepancies that cause us to have graphs wehn we dont need? i think its cause they have nas in the orignal data?
    combined_data = clean_data_for_plotting(df, df_copy, all_diffs, economy, scenario)
    final_df = pd.DataFrame()
    # for scenario in all_diffs['scenarios'].unique():
    for fuel in all_diffs['fuels'].unique():
        # Filter for the current scenario
        combined_data_scen = combined_data[(combined_data['fuels'] == fuel)]#(combined_data['scenarios'] == scenario) & 
        #now plot the data
        if len(combined_data_scen) == 0:
            # breakpoint()
            continue
        
        #where there is only original data, also continue
        if len(combined_data_scen[combined_data_scen['dataset'] == 'original']) == len(combined_data_scen):
            # breakpoint()
            continue
        fig = px.line(
            combined_data_scen,
            x='year',
            y='value',
            color='sectors',
            line_dash='dataset',
            facet_col='scenarios',
            facet_row='subfuels',
            title=f"Supply Adjustments — {economy} / {fuel}",
            labels={'value': 'Supply (PJ)', 'year': 'Year'},
        )
        # Write HTML
        if os.path.exists(f"./plotting_output/supply_adjustments/{economy}/") == False:
            os.makedirs(f"./plotting_output/supply_adjustments/{economy}/")
        if economy in ECONOMIES_TO_SKIP_FOR_BALANCING_FOR_CERTAIN_FUELS:
            if fuel in ECONOMIES_TO_SKIP_FOR_BALANCING_FOR_CERTAIN_FUELS[economy]:
                USED_IN_RESULTS = '_UNUSED'
                
        fig.write_html(f"./plotting_output/supply_adjustments/{economy}/supply_adjustments_{economy}_{fuel}{USED_IN_RESULTS}.html")

        final_df = pd.concat([final_df, combined_data_scen], ignore_index=True)
    
    if len(final_df) > 0:
        final_df.to_csv(f'./plotting_output/supply_adjustments/{economy}/supply_adjustments_{economy}.csv')

def plot_demand_supply_discrepancies(error_df, economy, scenario,USED_IN_RESULTS, ECONOMIES_TO_SKIP_FOR_BALANCING_FOR_CERTAIN_FUELS):
    """
    Just plot all the values in the demand supply discreps df with a facet for each fuel/subfuel combo. And we will create a col which has label for where '22_demand_supply_discrepancy' is the sector.
    """

    import re
    final_df = pd.DataFrame()
    for fuel in error_df['fuels'].unique():
    # for scenario in error_df['scenarios'].unique():
        
        # Filter for the current scenario
        error_df_scen = error_df[error_df['fuels'] == fuel]

        year_cols = [c for c in error_df_scen.columns if re.match(r'^\d{4}$', str(c))]
        df = error_df_scen.melt(
           id_vars=['economy', 'scenarios', 'fuels', 'subfuels', 'sectors',
                     'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors'
                    ],
           value_vars=year_cols,
           var_name='year',
           value_name='value'
        )
        df['year'] = df['year'].astype(int)
        df['sectors_'] = df['sectors'] + " " + df['sub1sectors'] + " " + df['sub2sectors'] + " " + df['sub3sectors'] + " " + df['sub4sectors']
        df.loc[df['sectors'] == '22_demand_supply_discrepancy', 'line_dash'] = 'demand_supply_discrepancy'
        df.loc[df['sectors'] != '22_demand_supply_discrepancy', 'line_dash'] = 'original data'
        if len(df)>0:
            # Create line chart
            fig = px.line(
                df,
                x='year',
                y='value',
                color='sectors_',
                line_dash='line_dash',
                facet_col='scenarios',
                facet_row='subfuels',
                title=f"Demand Supply Discrepancies — {economy} / {fuel}"
            )
            if economy in ECONOMIES_TO_SKIP_FOR_BALANCING_FOR_CERTAIN_FUELS:
                if fuel in ECONOMIES_TO_SKIP_FOR_BALANCING_FOR_CERTAIN_FUELS[economy]:
                    USED_IN_RESULTS = '_UNUSED'
            # Write HTML
            fig.write_html(f"./plotting_output/supply_adjustments/{economy}/demand_supply_discrepancies_{economy}_{fuel}{USED_IN_RESULTS}.html")
            final_df = pd.concat([final_df, df], ignore_index=True)
    if len(final_df) > 0:
        final_df.to_csv(f'./plotting_output/supply_adjustments/{economy}/demand_supply_discrepancies_{economy}.csv')

def make_manual_changes_to_rows(final_energy_df, economy, scenario):
    #this will just be a messy function to make manual changes to the finaldata as is needed. Probelm is that these could be all sorts of little changes so i think it will be easier to have this be quite messy and just use whatever code we need to make the changes. just make sure to clearly label the changes and the reasons for them.
    final_energy_df_copy = final_energy_df.copy()
    projected_years = [str(year) for year in range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1)]
    ##########
    #ROK shift all nautral gas imports to lng for projected years#this is caused by the adjust projected supply to balance demand function which sees there is not enough gas to supply the demand and so it creates imports of gas, even though technically they should be lng. Could think of no other way t make this change and have it useful for other similar cases.
    CHANGES_MADE = False
    if economy == '09_ROK':
        CHANGES_MADE = True
        # breakpoint()
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
    
    #whrre there is charcoal production in the projected years, set the sectors to 09_total_transformation_sector and sub1sectors to 09_11_charcoal_processing it to be 0
    charcoal_production = final_energy_df.loc[(final_energy_df['subfuels'] == '15_03_charcoal') & (final_energy_df['sectors'] == '01_production') & (final_energy_df['economy'] == economy) & (final_energy_df['scenarios'] == scenario)]
    if charcoal_production[projected_years].sum().sum() > 0:
        #NOTE THAT WE ARENT HANDLING SUBTOTALS CORRECLTY HERE BUT IT DIDNT SEEM THAT IMPROTANT TO ADD A SMAL AMOUNT OF TRANSOFMATION TO THE TRANSFOMRAITON TOTAL
        #todo adjust tpes because of this
        CHANGES_MADE = True
        #double check there are no preexisint transformation sectors for charcoal
        charcoal_transfomation = final_energy_df.loc[(final_energy_df['subfuels'] == '15_03_charcoal') & (final_energy_df['sectors'] == '09_total_transformation_sector') & (final_energy_df['economy'] == economy) & (final_energy_df['scenarios'] == scenario)]
        if charcoal_transfomation[projected_years].sum().sum() > 0:
            breakpoint()
            raise ValueError(f"Charcoal transformation output is not 0 for {economy}, {scenario}")
        #drop any prexisintg rows for hcharcoal transformation
        final_energy_df = final_energy_df[~((final_energy_df['subfuels'] == '15_03_charcoal') & (final_energy_df['sectors'] == '09_total_transformation_sector') & (final_energy_df['economy'] == economy) & (final_energy_df['scenarios'] == scenario))]
        #set the sectors to be 09_total_transformation_sector and sub1sectors to be 09_11_charcoal_processing
        ##
        #first extract it for use later
        charcoal_production = final_energy_df.loc[(final_energy_df['subfuels'] == '15_03_charcoal') & (final_energy_df['sectors'] == '01_production') & (final_energy_df['economy'] == economy) & (final_energy_df['scenarios'] == scenario), projected_years].copy()
        ##
        final_energy_df.loc[(final_energy_df['subfuels'] == '15_03_charcoal') & (final_energy_df['sectors'] == '01_production') & (final_energy_df['economy'] == economy) & (final_energy_df['scenarios'] == scenario), 'sectors'] = '09_total_transformation_sector'#if there happens to be transformation input of charcoal then this will handle it ok
        final_energy_df.loc[(final_energy_df['subfuels'] == '15_03_charcoal') & (final_energy_df['sectors'] == '01_production') & (final_energy_df['economy'] == economy) & (final_energy_df['scenarios'] == scenario), 'sub1sectors'] = '09_11_charcoal_processing'
        
        #decrease sectors == 07_total_primary_energy_supply, for fuel == 15_solid_biomass, subfuel == x, subtotal_results = True  as well as sectors == 07_total_primary_energy_supply, for fuel == 15_solid_biomass, subfuel == 15_03_charcoal, subtotal_results = False by the production amount to simulate that the charcoal is being used in the transformation sector instead of the primary energy supply sector
        # breakpoint()#double chekc that this works ok
        final_energy_df.loc[(final_energy_df['subfuels'] == '15_03_charcoal') & (final_energy_df['sectors'] == '07_total_primary_energy_supply') & (final_energy_df['economy'] == economy) & (final_energy_df['scenarios'] == scenario) & (final_energy_df['subtotal_results'] == False), projected_years] -= charcoal_production[projected_years].values
        final_energy_df.loc[(final_energy_df['subfuels'] == 'x') &(final_energy_df['fuels'] == '15_solid_biomass') & (final_energy_df['sectors'] == '07_total_primary_energy_supply') & (final_energy_df['economy'] == economy) & (final_energy_df['scenarios'] == scenario) & (final_energy_df['subtotal_results'] == True), projected_years] -= charcoal_production[projected_years].values
        # breakpoint()#check results
    if economy  == '17_SGP':
        #there are subtotals in the electricity geneation in the historical data that are not being labelled correctly. its not clear why so just going to quickly fix it here:
        # scenarios	economy	sectors	sub1sectors	sub2sectors	sub3sectors	sub4sectors	fuels	subfuels	subtotal_layout
        # reference	17_SGP	18_electricity_output_in_gwh	x	x	x	x	07_petroleum_products	x	FALSE
        # reference	17_SGP	18_electricity_output_in_gwh	x	x	x	x	08_gas	x	FALSE
        rows_to_change = final_energy_df.loc[(final_energy_df['sectors'] == '18_electricity_output_in_gwh') & (final_energy_df['economy'] == economy) & (final_energy_df['scenarios'] == scenario) & (final_energy_df['subtotal_layout'] == False) & (final_energy_df['fuels'].isin(['07_petroleum_products', '08_gas'])) & (final_energy_df['subfuels'] == 'x') & (final_energy_df['sub1sectors'] == 'x')]
        if len(rows_to_change) > 0:
            CHANGES_MADE = True
            final_energy_df.loc[(final_energy_df['sectors'] == '18_electricity_output_in_gwh') & (final_energy_df['economy'] == economy) & (final_energy_df['scenarios'] == scenario) & (final_energy_df['subtotal_layout'] == False) & (final_energy_df['fuels'].isin(['07_petroleum_products', '08_gas'])) & (final_energy_df['subfuels'] == 'x') & (final_energy_df['sub1sectors'] == 'x'), 'subtotal_layout'] = True
            

    if CHANGES_MADE:
        #and save the file sincxe this is a last step.
        shared_categories_w_subtotals = shared_categories + ['subtotal_layout', 'subtotal_results']
        previous_merged_df_filename=None

        merging_functions.save_merged_file(final_energy_df, economy, previous_merged_df_filename, shared_categories_w_subtotals,
        folder_path=f'results/{economy}/merged', old_folder_path=f'results/{economy}/merged/old',
        COMPARE_TO_PREVIOUS_MERGE = False)
    return final_energy_df


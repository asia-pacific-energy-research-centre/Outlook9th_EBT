# Now begin to subset

import numpy as np
import pandas as pd
import re
import os
from datetime import datetime

# Change the working drive
wanted_wd = 'Outlook9th_EBT'
os.chdir(re.split(wanted_wd, os.getcwd())[0] + wanted_wd)

# interim save
interim_path = './data/interim/'
os.makedirs(interim_path, exist_ok = True)

merged_df_clean_wide = pd.read_csv(interim_path + 'interim.csv')

###############################################################################################
# Mat addition: subset the sector_fuel_layout df

year_list = list(range(1980, 2021, 1))
year_str = list(map(str, year_list))

# Read in data of fuels used by the different sector models
fuel_df = pd.read_excel('./data/fuels_used_by_modellers.xlsx')
fuel_df = fuel_df.iloc[1:,10:]

# Industry and non-energy layout
ine_vector = ['14_industry_sector', '17_nonenergy_use']

ine_fuel = fuel_df.loc[:,'Industry and non-energy.1'].dropna()

ine_fuels = list(ine_fuel[(ine_fuel.str.count('\d') <= 2) & (ine_fuel.str.contains('_x_') == False)])
ine_subfuels = list(ine_fuel[(ine_fuel.str.count('\d') > 2) | (ine_fuel.str.contains('_x_') == True)])
ine_subfuels.append('x')

ine_df = merged_df_clean_wide[merged_df_clean_wide['sectors'].isin(ine_vector)].copy()

first_subset = merged_df_clean_wide[~merged_df_clean_wide['sectors'].isin(ine_vector)]

ine_df1 = ine_df[(ine_df['fuels'].isin(ine_fuels)) &
                 (ine_df['subfuels'].isin(ine_subfuels))].copy()

# Now keep the non-zero and non na rows in the rest of the frame
ine_df2 = ine_df[~((ine_df['fuels'].isin(ine_fuels)) &
                  (ine_df['subfuels'].isin(ine_subfuels)))].copy()

# drop if all na
ine_df2 = ine_df2.dropna(subset = year_str).copy()
# drop if all zero
ine_df2 = ine_df2.loc[~(ine_df2.select_dtypes(include = ['number']) == 0).all(axis = 'columns'), :].copy()

ine_df = pd.concat([ine_df1, ine_df2]).copy()

# Buildings
bld_vector = ['16_01_buildings']

bld_fuel = fuel_df.loc[:,'Buildings.1'].dropna()

bld_fuels = list(bld_fuel[(bld_fuel.str.count('\d') <= 2) & (bld_fuel.str.contains('_x_') == False)])
bld_subfuels = list(bld_fuel[(bld_fuel.str.count('\d') > 2) | (bld_fuel.str.contains('_x_') == True)])
bld_subfuels.append('x')

bld_df = first_subset[first_subset['sub1sectors'].isin(bld_vector)].copy()

second_subset = first_subset[~first_subset['sub1sectors'].isin(bld_vector)].copy()

bld_df1 = bld_df[(bld_df['fuels'].isin(bld_fuels)) &
                 (bld_df['subfuels'].isin(bld_subfuels))].copy()

# Now keep the non-zero and non na rows in the rest of the frame
bld_df2 = bld_df[~((bld_df['fuels'].isin(bld_fuels)) &
                  (bld_df['subfuels'].isin(bld_subfuels)))].copy()

# drop if all na
bld_df2 = bld_df2.dropna(subset = year_str).copy()
# drop if all zero
bld_df2 = bld_df2.loc[~(bld_df2.select_dtypes(include = ['number']) == 0).all(axis = 'columns'), :].copy()

bld_df = pd.concat([bld_df1, bld_df2]).copy()

# Transport
trn_vector = ['15_transport_sector']

trn_fuel = fuel_df.loc[:,'Transport.1'].dropna()

trn_fuels = list(trn_fuel[(trn_fuel.str.count('\d') <= 2) & (trn_fuel.str.contains('_x_') == False)])
trn_subfuels = list(trn_fuel[(trn_fuel.str.count('\d') > 2) | (trn_fuel.str.contains('_x_') == True)])
trn_subfuels.append('x')

trn_df = second_subset[second_subset['sectors'].isin(trn_vector)].copy()

third_subset = second_subset[~second_subset['sectors'].isin(trn_vector)].copy()

trn_df1 = trn_df[(trn_df['fuels'].isin(trn_fuels)) &
                 (trn_df['subfuels'].isin(trn_subfuels))].copy()

# Now keep the non-zero and non na rows in the rest of the frame
trn_df2 = trn_df[~((trn_df['fuels'].isin(trn_fuels)) &
                  (trn_df['subfuels'].isin(trn_subfuels)))].copy()

# drop if all na
trn_df2 = trn_df2.dropna(subset = year_str).copy()
# drop if all zero
trn_df2 = trn_df2.loc[~(trn_df2.select_dtypes(include = ['number']) == 0).all(axis = 'columns'), :].copy()

trn_df = pd.concat([trn_df1, trn_df2]).copy()

# Ag and other
ag_vector = ['16_02_agriculture_and_fishing', '16_05_nonspecified_others']

ag_fuel = fuel_df.loc[:,'Ag, fish & non-spec.1'].dropna()

ag_fuels = list(ag_fuel[(ag_fuel.str.count('\d') <= 2) & (ag_fuel.str.contains('_x_') == False)])
ag_subfuels = list(ag_fuel[(ag_fuel.str.count('\d') > 2) | (ag_fuel.str.contains('_x_') == True)])
ag_subfuels.append('x')

ag_df = third_subset[third_subset['sub1sectors'].isin(ag_vector)].copy()

fourth_subset = third_subset[~third_subset['sub1sectors'].isin(ag_vector)].copy()

ag_df1 = ag_df[(ag_df['fuels'].isin(ag_fuels)) &
               (ag_df['subfuels'].isin(ag_subfuels))].copy()

# Now keep the non-zero and non na rows in the rest of the frame
ag_df2 = ag_df[~((ag_df['fuels'].isin(ag_fuels)) &
                  (ag_df['subfuels'].isin(ag_subfuels)))].copy()

# drop if all na
ag_df2 = ag_df2.dropna(subset = year_str).copy()
# drop if all zero
ag_df2 = ag_df2.loc[~(ag_df2.select_dtypes(include = ['number']) == 0).all(axis = 'columns'), :].copy()

ag_df = pd.concat([ag_df1, ag_df2]).copy()

# Power
pow_vector = ['09_01_electricity_plants', '09_02_chp_plants', '09_x_heat_plants']

pow_fuel = fuel_df.loc[:,'Power.1'].dropna()

pow_fuels = list(pow_fuel[(pow_fuel.str.count('\d') <= 2) & (pow_fuel.str.contains('_x_') == False)])
pow_subfuels = list(pow_fuel[(pow_fuel.str.count('\d') > 2) | (pow_fuel.str.contains('_x_') == True)])
pow_subfuels.append('x')

pow_df = fourth_subset[fourth_subset['sub1sectors'].isin(pow_vector)].copy()

fifth_subset = fourth_subset[~fourth_subset['sub1sectors'].isin(pow_vector)].copy()

pow_df1 = pow_df[(pow_df['fuels'].isin(pow_fuels)) &
                 (pow_df['subfuels'].isin(pow_subfuels))].copy()

# Now keep the non-zero and non na rows in the rest of the frame
pow_df2 = pow_df[~((pow_df['fuels'].isin(pow_fuels)) &
                  (pow_df['subfuels'].isin(pow_subfuels)))].copy()

# drop if all na
pow_df2 = pow_df2.dropna(subset = year_str).copy()
# drop if all zero
pow_df2 = pow_df2.loc[~(pow_df2.select_dtypes(include = ['number']) == 0).all(axis = 'columns'), :].copy()

pow_df = pd.concat([pow_df1, pow_df2]).copy()

# Refining
ref_vector = ['09_07_oil_refineries']

ref_fuel = fuel_df.loc[:,'Refining (oil & biofuels).1'].dropna()

ref_fuels = list(ref_fuel[(ref_fuel.str.count('\d') <= 2) & (ref_fuel.str.contains('_x_') == False)])
ref_subfuels = list(ref_fuel[(ref_fuel.str.count('\d') > 2) | (ref_fuel.str.contains('_x_') == True)])
ref_subfuels.append('x') 

ref_df = fifth_subset[fifth_subset['sub1sectors'].isin(ref_vector)].copy()

sixth_subset = fifth_subset[~fifth_subset['sub1sectors'].isin(ref_vector)].copy()

ref_df1 = ref_df[(ref_df['fuels'].isin(ref_fuels)) &
                 (ref_df['subfuels'].isin(ref_subfuels))].copy()

# Now keep the non-zero and non na rows in the rest of the frame
ref_df2 = ref_df[~((ref_df['fuels'].isin(ref_fuels)) &
                  (ref_df['subfuels'].isin(ref_subfuels)))].copy()

# drop if all na
ref_df2 = ref_df2.dropna(subset = year_str).copy()
# drop if all zero
ref_df2 = ref_df2.loc[~(ref_df2.select_dtypes(include = ['number']) == 0).all(axis = 'columns'), :].copy()

ref_df = pd.concat([ref_df1, ref_df2]).copy()

# Hydrogen
hyd_vector = ['09_13_hydrogen_transformation']

hyd_fuel = fuel_df.loc[:,'Hydrogen.1'].dropna()

hyd_fuels = list(hyd_fuel[(hyd_fuel.str.count('\d') <= 2) & (hyd_fuel.str.contains('_x_') == False)])
hyd_subfuels = list(hyd_fuel[(hyd_fuel.str.count('\d') > 2) | (hyd_fuel.str.contains('_x_') == True)])
hyd_subfuels.append('x') 

hyd_df = sixth_subset[sixth_subset['sub1sectors'].isin(hyd_vector)].copy()

seventh_subset = sixth_subset[~sixth_subset['sub1sectors'].isin(hyd_vector)].copy()

hyd_df = hyd_df[(hyd_df['fuels'].isin(hyd_fuels)) &
                (hyd_df['subfuels'].isin(hyd_subfuels))].copy()

merged_df_clean_wide = pd.concat([seventh_subset, ine_df, trn_df, bld_df, ag_df,
                                  pow_df, ref_df, hyd_df]).copy().reset_index(drop = True)

############################################################################################################

# Now subset and remove transformation data rows that have not been requested as per above
# I.e. remove all zero and np.nan rows in these categories
# Level 0
subset0 = ['08_transfers', '11_statistical_discrepancy']

split_df = merged_df_clean_wide[merged_df_clean_wide['sectors'].isin(subset0)].copy()
remain_df = merged_df_clean_wide[~merged_df_clean_wide['sectors'].isin(subset0)].copy()

split_df = split_df.dropna(subset = year_str).copy()
split_df = split_df.loc[~(split_df.select_dtypes(include = ['number']) == 0).all(axis = 'columns'), :].copy()

merged_df_clean_wide = pd.concat([split_df, remain_df]).copy()


# Level 1
subset1 = ['09_03_heat_pumps', '09_04_electric_boilers', '09_05_chemical_heat_for_electricity_production',
          '09_06_gas_processing_plants', '09_09_petrochemical_industry', '09_11_charcoal_processing', 
          '09_12_nonspecified_transformation', '17_01_transformation_sector', '17_02_industry_sector',
          '17_03_transport_sector', '17_04_other_sector']

split_df = merged_df_clean_wide[merged_df_clean_wide['sub1sectors'].isin(subset1)].copy()
remain_df = merged_df_clean_wide[~merged_df_clean_wide['sub1sectors'].isin(subset1)].copy()

split_df = split_df.dropna(subset = year_str).copy()
split_df = split_df.loc[~(split_df.select_dtypes(include = ['number']) == 0).all(axis = 'columns'), :].copy()

merged_df_clean_wide = pd.concat([split_df, remain_df]).copy()

# Level 2
subset2 = ['10_01_02_gas_works_plants', '10_01_05_natural_gas_blending_plants',
           '10_01_06_gastoliquids_plants', '10_01_07_gas_separation', '10_01_11_patent_fuel_plants',
           '10_01_12_bkb_pb_plants', '10_01_13_liquefaction_plants_coal_to_oil', '10_01_17_nuclear_industry',
           '10_01_18_nonspecified_own_uses']

split_df = merged_df_clean_wide[merged_df_clean_wide['sub2sectors'].isin(subset2)].copy()
remain_df = merged_df_clean_wide[~merged_df_clean_wide['sub2sectors'].isin(subset2)].copy()

split_df = split_df.dropna(subset = year_str).copy()
split_df = split_df.loc[~(split_df.select_dtypes(include = ['number']) == 0).all(axis = 'columns'), :].copy()

merged_df_clean_wide = pd.concat([split_df, remain_df]).copy()

# Trade 
trade_vector = ['02_imports', '03_exports']
trade_fuels = ['01_coal', '02_coal_products', '03_peat', '04_peat_products',
             '05_oil_shale_and_oil_sands', '06_crude_oil_and_ngl', '07_petroleum_products', '08_gas',
             '15_solid_biomass', '16_others', '17_electricity', '18_heat', '19_total',
             '20_total_renewables', '21_modern_renewables'] 

trade_df = merged_df_clean_wide[merged_df_clean_wide['sectors'].isin(trade_vector)].copy()
notrade_df = merged_df_clean_wide[~merged_df_clean_wide['sectors'].isin(trade_vector)].copy()

trade_df = trade_df[trade_df['fuels'].isin(trade_fuels)].copy()

merged_df_clean_wide = pd.concat([trade_df, notrade_df]).copy()

# Bunkers 
b_vector = ['04_international_marine_bunkers', '05_international_aviation_bunkers']
b_fuels = ['07_petroleum_products', '16_others', '19_total', '20_total_renewables', '21_modern_renewables'] 

b_df = merged_df_clean_wide[merged_df_clean_wide['sectors'].isin(b_vector)].copy()
nob_df = merged_df_clean_wide[~merged_df_clean_wide['sectors'].isin(b_vector)].copy()

b_df = b_df[b_df['fuels'].isin(b_fuels)].copy()

merged_df_clean_wide = pd.concat([b_df, nob_df]).copy()

# Stock
stock_vector = ['06_stock_changes']
s_fuels = ['01_coal', '02_coal_products', '03_peat', '04_peat_products', '05_oil_shale_and_oil_sands', 
           '06_crude_oil_and_ngl', '07_petroleum_products', '08_gas', '15_solid_biomass', '16_others', 
           '19_total', '20_total_renewables', '21_modern_renewables'] 

s_df = merged_df_clean_wide[merged_df_clean_wide['sectors'].isin(stock_vector)].copy()
nos_df = merged_df_clean_wide[~merged_df_clean_wide['sectors'].isin(stock_vector)].copy()

s_df = s_df[s_df['fuels'].isin(s_fuels)].copy()

merged_df_clean_wide = pd.concat([s_df, nos_df]).copy()

# Remove coal products (amybe reverse this decision)
coal_p = ['02_01_coke_oven_coke', '02_02_gas_coke', '02_03_coke_oven_gas', '02_04_blast_furnace_gas',
          '02_05_other_recovered_gases', '02_06_patent_fuel', '02_07_coal_tar', '02_08_bkb_pb']

merged_df_clean_wide = merged_df_clean_wide[~merged_df_clean_wide['subfuels'].isin(coal_p)]\
    .copy().reset_index(drop = True)

# Merge scenarios

scen = pd.read_excel('./data/scenario_list.xlsx')

merged_df_clean_wide = pd.merge(scen, merged_df_clean_wide, how = 'cross')

# Sort subfuels and sub1sectors
ordered = pd.read_csv('./data/order_sector_fuels.csv')

order1 = list(ordered['subfuels'])
order2 = list(ordered['sub1sectors'])

order2 = list(filter(lambda x: pd.notna(x), order2)) #dropping the 'nan' category from the list

merged_df_clean_wide['subfuels'] = pd.Categorical(merged_df_clean_wide['subfuels'], 
                                                      categories = order1, 
                                                      ordered = True)

merged_df_clean_wide['sub1sectors'] = pd.Categorical(merged_df_clean_wide['sub1sectors'],
                                                     categories = order2,
                                                     ordered = True)


merged_df_clean_wide = merged_df_clean_wide.sort_values(['scenarios', 'economy', 'sectors', 'fuels', 'sub1sectors', 'subfuels'])\
    .copy().reset_index(drop = True)

# Required years
projected_years = list(range(2021, 2070+1, 1))

for i in projected_years:
    merged_df_clean_wide[i] = np.nan

# Replace the ```value_not_in_the_range``` with ```np.nan```
# - In principle, rows without historical data will be assign np.nan rather than 0.
# ---
# - Notice that we set 0 for hydrogen and ammonia before concating.
# - They became ```np.nan``` during the process.
# - To do the pivot properly, ther became value_not_in_the_range.
# - And now they are going to become np.nan again.

reference_df = merged_df_clean_wide[merged_df_clean_wide['scenarios'] == 'reference'].copy().reset_index(drop = True)
target_df = merged_df_clean_wide[merged_df_clean_wide['scenarios'] == 'target'].copy().reset_index(drop = True)

# Export data
date_today = datetime.now().strftime('%Y%m%d')

folder_path = './results'
os.makedirs(folder_path, exist_ok=True)

file_name = 'model_df_wide_' + date_today +'.csv'
result_path = os.path.join(folder_path, file_name)
merged_df_clean_wide.to_csv(result_path, index = False)

# Scenario dataframes
reference_df.to_csv(folder_path + '/model_df_wide_ref_' + date_today + '.csv', index = False)
target_df.to_csv(folder_path + '/model_df_wide_tgt_' + date_today + '.csv', index = False)
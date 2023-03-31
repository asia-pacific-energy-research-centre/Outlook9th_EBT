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

# Industry and non-energy layout
ine_vector = ['14_industry_sector', '17_nonenergy_use']
ine_fuels = ['01_coal', '02_coal_products', '06_crude_oil_and_ngl', '07_petroleum_products', '08_gas',
             '15_solid_biomass', '16_others', '17_electricity', '18_heat', '19_total',
             '20_total_renewables', '21_modern_renewables'] 

ine_subfuels = ['01_01_coking_coal', '01_x_thermal_coal', '07_01_motor_gasoline', '07_03_naphtha',
                '07_06_kerosene', '07_07_gas_diesel_oil', '07_08_fuel_oil', '07_09_lpg', 
                '07_10_refinery_gas_not_liquefied', '07_11_ethane', '07_x_other_petroleum_products',
                '08_01_natural_gas', '16_01_biogas', '16_02_industrial_waste', 
                '16_03_municipal_solid_waste_renewable', '16_04_municipal_solid_waste_nonrenewable',
                '16_05_biogasoline', '16_06_biodiesel', '16_08_other_liquid_biofuels', '16_09_other_sources', 
                '16_x_hydrogen', 'x']

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
bld_fuels = ['01_coal', '02_coal_products', '03_peat', '04_peat_products',
             '07_petroleum_products', '08_gas', '12_solar',
             '15_solid_biomass', '16_others', '17_electricity', '18_heat', '19_total',
             '20_total_renewables', '21_modern_renewables']

bld_subfuels = ['07_06_kerosene', '07_09_lpg', 'x']

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
trn_fuels = ['01_coal', '07_petroleum_products', '08_gas',
             '16_others', '17_electricity', '19_total',
             '20_total_renewables', '21_modern_renewables'] 

trn_subfuels = ['01_x_thermal_coal', '01_05_lignite', '07_01_motor_gasoline', '07_02_aviation_gasoline', 
                '07_03_naphtha',
                '07_06_kerosene', '07_07_gas_diesel_oil', '07_08_fuel_oil', '07_09_lpg', 
                '07_11_ethane', '07_x_jet_fuel', '07_x_other_petroleum_products',
                '08_01_natural_gas', '08_02_lng', '08_03_gas_works_gas', '16_01_biogas',
                '16_05_biogasoline', '16_06_biodiesel', '16_07_bio_jet_kerosene', 
                '16_08_other_liquid_biofuels', '16_09_other_sources', '16_x_ammonia',
                '16_x_hydrogen', 'x']

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

ag_fuels = ['01_coal', '02_coal_products', '03_peat', '04_peat_products', '06_crude_oil_and_ngl', 
            '07_petroleum_products', '08_gas', '11_geothermal', '12_solar',
            '15_solid_biomass', '16_others', '17_electricity', '18_heat', '19_total',
            '20_total_renewables', '21_modern_renewables'] 

ag_subfuels = ['01_x_thermal_coal', '01_05_lignite', '02_01_coke_oven_coke', '02_03_coke_oven_gas',
               '02_08_bkb_pb', '06_01_crude_oil', 
               '07_01_motor_gasoline', '07_06_kerosene', '07_07_gas_diesel_oil', '07_08_fuel_oil', '07_09_lpg', 
               '07_x_jet_fuel', '07_x_other_petroleum_products', '08_01_natural_gas', '15_01_fuelwood_and_woodwaste',
               '15_02_bagasse', '15_03_charcoal', '15_05_other_biomass', '16_01_biogas', '16_02_industrial_waste', 
               '16_03_municipal_solid_waste_renewable', '16_04_municipal_solid_waste_nonrenewable',
               '16_05_biogasoline', '16_06_biodiesel', '16_x_hydrogen', 'x']

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

pow_fuels = ['01_coal', '02_coal_products', '03_peat', '04_peat_products', '06_crude_oil_and_ngl', 
            '07_petroleum_products', '08_gas', '09_nuclear', '10_hydro', '11_geothermal', '12_solar',
            '13_tide_wave_ocean', '14_wind', '15_solid_biomass', '16_others', '17_electricity', '18_heat',
            '19_total', '20_total_renewables', '21_modern_renewables'] 

pow_subfuels = ['01_x_thermal_coal', '01_05_lignite', '06_01_crude_oil', 
               '07_07_gas_diesel_oil', '07_08_fuel_oil', '07_x_other_petroleum_products', 
               '08_01_natural_gas', '12_01_of_which_photovoltaics', '12_x_other_solar', 
               '16_x_ammonia', '16_x_hydrogen', 'x']

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

ref_fuels = ['06_crude_oil_and_ngl', '07_petroleum_products', '13_tide_wave_ocean', '15_solid_biomass', '16_others', 
             '19_total', '20_total_renewables', '21_modern_renewables'] 

ref_subfuels = ['06_01_crude_oil', '06_02_natural_gas_liquids', '06_x_other_hydrocarbons', '07_01_motor_gasoline', 
                '07_02_aviation_gasoline', '07_03_naphtha', '07_06_kerosene', '07_07_gas_diesel_oil', '07_08_fuel_oil', 
                '07_09_lpg', '07_10_refinery_gas_not_liquefied', '07_11_ethane', '07_x_jet_fuel',
                '07_x_other_petroleum_products', '15_02_bagasse', '16_05_biogasoline', '16_06_biodiesel',
                '16_07_bio_jet_kerosene', '16_08_other_liquid_biofuels', '16_09_other_sources', 'x']

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

hyd_fuels = ['01_coal', '02_coal_products', '08_gas', '15_solid_biomass', '16_others', '17_electricity',
             '19_total', '20_total_renewables', '21_modern_renewables'] 

hyd_subfuels = ['16_x_ammonia', '16_x_hydrogen', 'x']

hyd_df = sixth_subset[sixth_subset['sub1sectors'].isin(hyd_vector)].copy()

seventh_subset = sixth_subset[~sixth_subset['sub1sectors'].isin(hyd_vector)].copy()

hyd_df = hyd_df[(hyd_df['fuels'].isin(hyd_fuels)) &
                (hyd_df['subfuels'].isin(hyd_subfuels))].copy()

merged_df_clean_wide = pd.concat([seventh_subset, ine_df, trn_df, bld_df, ag_df,
                                  pow_df, ref_df, hyd_df]).copy().reset_index(drop = True)

############################################################################################################

# Now subset and remove transformation
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

# Remove coal products
coal_p = ['02_01_coke_oven_coke', '02_02_gas_coke', '02_03_coke_oven_gas', '02_04_blast_furnace_gas',
          '02_05_other_recovered_gases', '02_06_patent_fuel', '02_07_coal_tar', '02_08_bkb_pb']

merged_df_clean_wide = merged_df_clean_wide[~merged_df_clean_wide['subfuels'].isin(coal_p)]\
    .copy().reset_index(drop = True)

# Merge scenarios

scen = pd.read_excel('./data/scenario_list.xlsx')

merged_df_clean_wide = pd.merge(scen, merged_df_clean_wide, how = 'cross')

merged_df_clean_wide = merged_df_clean_wide.sort_values(['scenarios', 'economy', 'sectors', 'fuels', 'sub1sectors'])\
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
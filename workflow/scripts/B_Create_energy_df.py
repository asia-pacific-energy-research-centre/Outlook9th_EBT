#%%
# # Script created by Wu, Taiwan researcher, to take EBT EGEDA data and build a large dataset that will contain all possible variable combinations
# that will be populated by the 9th modelling. This involves removing some EBT fuels and sectors combos and adding in specific modelling combos
# that are not in the EBT (such as type of road transport)

import numpy as np
import pandas as pd
import re
import os
from datetime import datetime
from utility_functions import *

set_working_directory()#from utility_functions.py

def create_energy_df(df_no_year_econ_index):

    # interim save
    interim_path = './data/interim/'
    os.makedirs(interim_path, exist_ok = True)

    # # read interim data
    # if USE_SINGLE_ECONOMY:
    #     df_no_year_econ_index = pd.read_csv(interim_path + f'EBT_long_{SINGLE_ECONOMY}.csv')
    # else:
    #     df_no_year_econ_index = pd.read_csv(interim_path + 'EBT_long.csv')

    # Import
    fuel_mapping = pd.read_excel('./config/reference_table_fuel.xlsx', usecols = [0, 1])
    sector_mapping = pd.read_excel('./config/reference_table_sector.xlsx', usecols = [0, 1])

    # Notice that the values in "fuels" and "clean_egeda_fuel_name" are the same
    # You can check with the following code
    #test = df_fuel.loc[df_fuel['fuels'] != df_fuel['clean_egeda_fuel_name']]
    #test

    df_fuel = pd.merge(df_no_year_econ_index, 
                    fuel_mapping, 
                    how = 'left', 
                    left_on = 'fuels', 
                    right_on = 'clean_egeda_fuel_name')

    ## Replace the fules series
    # 1. create "replace_materials" series that is based on "unique_the_end_of_fuels" series with NaN filled by fuels
    # 2. The content of "replace_materials" series ```df_fuel['replace_materials'] = df_fuel['unique_the_end_of_fuels'].fillna(df_fuel['fuels'])
    # ```
    #     - "fuels"-null; "unique_the_end_of_fuels"-notnull → use "unique_the_end_of_fuels"
    #     - "fuels"-notnull; "unique_the_end_of_fuels"-notnull → use "unique_the_end_of_fuels" 
    #     - "fuels"-notnull; "unique_the_end_of_fuels"-null → use "fuels"

    # | fules    | replace | uni    |
    # | -------- | :-----: | :---   |
    # | Alice→   |  Alice  | nan    |
    # | nan      |  Bob    | ←Bob   |
    # | Charlie  |  David  | ←David |


    # 3. Use "replace_materials" series to replace the original 'fuels' series
    #     - Although it's impossible to have nan "replace_materials" series, we still use  ```.where(df_fuel['replace_materials'].notnull()``` in the script
    # | fules  | replace |
    # | ------ | :------ |
    # | Alice  | Alice   |
    # | Bob    | ←Bob    |
    # | David  | ←David  |

    df_fuel['replace_materials'] = df_fuel['unique_the_end_of_fuels'].fillna(df_fuel['fuels'])
    df_fuel['fuels'] = df_fuel['replace_materials'].where(df_fuel['replace_materials'].notnull(), df_fuel['fuels'])

    # # Check the result: If we succesfully replace the series, we should not filter out '01_03_lignite' (the item number is changed).
    # df_fuel[df_fuel['fuels']=='01_05_lignite']  

    df_fuel.drop(['clean_egeda_fuel_name', 'unique_the_end_of_fuels', 'replace_materials'], axis= 1, inplace = True)

    ## Replace the data in "sectors" series with "unique_the_end_of_sectors" series

    df_fuel_sector = pd.merge(df_fuel, sector_mapping, how = 'left', left_on = 'sectors', right_on = 'clean_egeda_sector_name')

    df_fuel_sector['replace_materials'] = df_fuel_sector['unique_the_end_of_sectors'].fillna(df_fuel_sector['sectors'])
    df_fuel_sector['sectors'] = df_fuel_sector['replace_materials'].where(df_fuel_sector['replace_materials'].notnull(), df_fuel_sector['sectors'])

    df_fuel_sector.drop(['clean_egeda_sector_name', 'unique_the_end_of_sectors', 'replace_materials'], axis = 1, inplace = True)

    # Self defined layout
    #TODO. THIS NEEDS TO BE CLEANED UP. PREFERABLY WE ARCHIVE OLD LAYOUTS RATHER THAN DATEMARKING THEM
    fuel_layout = pd.read_excel('./config/EBT_column_fuels.xlsx', 
                                sheet_name =FUEL_LAYOUT_SHEET, usecols = [0, 1])
    sector_layout = pd.read_excel('./config/EBT_row_sectors.xlsx', 
                                sheet_name = SECTOR_LAYOUT_SHEET, usecols = [0, 1, 2, 3, 4])

    # Clean the data again FFS

    for i in fuel_layout.columns:
        fuel_layout[i] =  fuel_layout[i].str.replace(' ', '_', regex = False)\
                                        .str.replace('.', '_', regex = False)\
                                        .str.replace('/', '_', regex = False)\
                                        .str.replace('(', '', regex = False)\
                                        .str.replace(')', '', regex = False)\
                                        .str.replace('-', '', regex = False)\
                                        .str.replace(',', '', regex = False)\
                                        .str.replace('&', 'and', regex = False)\
                                        .str.replace('___', '_', regex = False)\
                                        .str.replace('__', '_', regex = False)\
                                        .str.replace(':', '', regex = False)\
                                        .str.replace('liqour', 'liquor', regex = False)\
                                        .str.rstrip('_')\
                                            .replace('\s+', ' ', regex = True)\
                                            .str.lower()
        
    for i in sector_layout.columns:
        sector_layout[i] =  sector_layout[i].str.replace(' ', '_', regex = False)\
                                        .str.replace('.', '_', regex = False)\
                                        .str.replace('/', '_', regex = False)\
                                        .str.replace('(', '', regex = False)\
                                        .str.replace(')', '', regex = False)\
                                        .str.replace('-', '', regex = False)\
                                        .str.replace(',', '', regex = False)\
                                        .str.replace('&', 'and', regex = False)\
                                        .str.replace('___', '_', regex = False)\
                                        .str.replace('__', '_', regex = False)\
                                        .str.replace(':', '', regex = False)\
                                        .str.replace('liqour', 'liquor', regex = False)\
                                        .str.rstrip('_')\
                                            .replace('\s+', ' ', regex = True)\
                                            .str.lower()
        
    # Create key col for you to connect the multi cols and EGEDA data 
    # - key column is the lowest level of our self-define layout
    # - you can get the lowest level in excel and python.
    # - I did both anyway.

    # subfuels is the lowest level
    # We create a new col base on it
    fuel_layout['fuel_key_col'] = fuel_layout['subfuels']

    for i in range(len(fuel_layout)):
        if fuel_layout.loc[i, 'subfuels'] == 'x':
            fuel_layout.loc[i, 'fuel_key_col'] = fuel_layout.loc[i, 'fuels'] 

    # sub4sectors is the lowest level
    # We create a new col base on it

    sector_layout['sector_key_col'] = sector_layout['sub4sectors']

    for i in range(len(sector_layout)):
        if (sector_layout.loc[i, 'sub4sectors'] == 'x') & (sector_layout.loc[i, 'sub3sectors'] != 'x'):
            sector_layout.loc[i, 'sector_key_col'] = sector_layout.loc[i, 'sub3sectors']
        else:
            if (sector_layout.loc[i, 'sub3sectors'] == 'x') & (sector_layout.loc[i, 'sub2sectors'] != 'x'):
                sector_layout.loc[i, 'sector_key_col'] = sector_layout.loc[i, 'sub2sectors']
            else:
                if (sector_layout.loc[i, 'sub2sectors'] == 'x') & (sector_layout.loc[i, 'sub1sectors'] != 'x'):
                    sector_layout.loc[i, 'sector_key_col'] = sector_layout.loc[i, 'sub1sectors']
                else:
                    if (sector_layout.loc[i, 'sub1sectors'] == 'x') & (sector_layout.loc[i, 'sectors'] != 'x'):
                        sector_layout.loc[i, 'sector_key_col'] = sector_layout.loc[i, 'sectors']

    # Aggregation, Disaggregation, New rows (相加相減作業)
    # - Notice that we deal with fuel, concating and excluding the targeted rows. 
    # - Based on the result, we deal with sectors, concating and excluding the trageted rows.
    # - DO NOT deal with these two together first, then concating and excluding the trageted rows. It may cause failure of merging or missing data.
    # ---
    # - BTW, df_fuel_sector is the latest, cleaned dataframe of the EGEDA historical data.

    # Fuel

    thermal_coal = df_fuel_sector[df_fuel_sector['fuels'].isin(['01_02_other_bituminous_coal', 
                                                                '01_03_subbituminous_coal', 
                                                                '01_04_anthracite'])]

    thermal_coal_g = thermal_coal.groupby(['economy', 'year', 'sectors'])['value'].sum().reset_index()\
        .assign(fuels = '01_x_thermal_coal')

    other_hydrocarbons = df_fuel_sector[df_fuel_sector['fuels'].isin(['06_03_refinery_feedstocks', 
                                                                    '06_04_additives_oxygenates', 
                                                                    '06_05_other_hydrocarbons'])]

    other_hydrocarbons_g = other_hydrocarbons.groupby(['economy', 'year', 'sectors'])['value'].sum().reset_index()\
        .assign(fuels = '06_x_other_hydrocarbons')

    jet_fuel = df_fuel_sector[df_fuel_sector['fuels'].isin(['07_04_gasoline_type_jet_fuel', 
                                                            '07_05_kerosene_type_jet_fuel'])]

    jet_fuel_g = jet_fuel.groupby(['economy', 'year', 'sectors'])['value'].sum().reset_index()\
        .assign(fuels = '07_x_jet_fuel')

    other_petroleum_products = df_fuel_sector[df_fuel_sector['fuels'].isin(['07_12_white_spirit_sbp', 
                                                                            '07_13_lubricants', 
                                                                            '07_14_bitumen',\
                                                                            '07_15_paraffin_waxes', 
                                                                            '07_16_petroleum_coke', 
                                                                            '07_17_other_products'])]

    other_petroleum_products_g = other_petroleum_products.groupby(['economy', 'year', 'sectors'])['value']\
        .sum().reset_index().assign(fuels = '07_x_other_petroleum_products')

    other_solar = df_fuel_sector[df_fuel_sector['fuels'].isin(['12_solar', '12_01_of_which_photovoltaics'])].copy()

    other_solar_a = other_solar[other_solar['fuels'] == '12_solar'].copy()
    other_solar_b = other_solar[other_solar['fuels'] == '12_01_of_which_photovoltaics'].copy()
    other_solar_c = other_solar_b.copy()

    other_solar_b['value'] = other_solar_c['value'] * -1

    other_solar = pd.concat([other_solar_a, other_solar_b], axis = 0)

    other_solar_g = other_solar.groupby(['economy', 'year', 'sectors'])['value']\
        .sum().reset_index().assign(fuels = '12_x_other_solar')

    # Hydrogen and ammonia and efuel

    temp_for_new = other_solar_g.copy()
    temp_for_new['value'] = 0
    temp_for_new['fuels'] = 0

    hydrogen = temp_for_new.copy()
    hydrogen['fuels'] = '16_x_hydrogen'

    ammonia = temp_for_new.copy()
    ammonia['fuels'] = '16_x_ammonia'

    efuel = temp_for_new.copy()
    efuel['fuels'] = '16_x_efuel'

    # Concat 
    # - exclude the data that are used to do the aggregation 
    # - include the data you just create 

    # Remember to remove the peat and peat product because we decided to let thme back to the list. (20230317)
    df_fuel_sector = df_fuel_sector[~df_fuel_sector['fuels'].isin(['01_02_other_bituminous_coal', 
                                                                '01_03_subbituminous_coal', 
                                                                '01_04_anthracite',
                                                                '06_03_refinery_feedstocks', 
                                                                '06_04_additives_oxygenates', 
                                                                '06_05_other_hydrocarbons',
                                                                '07_04_gasoline_type_jet_fuel', 
                                                                '07_05_kerosene_type_jet_fuel', 
                                                                '07_12_white_spirit_sbp', 
                                                                '07_13_lubricants',
                                                                '07_14_bitumen', 
                                                                '07_15_paraffin_waxes', 
                                                                '07_16_petroleum_coke', 
                                                                '07_17_other_products'])]

    df_fuel_sector = pd.concat([df_fuel_sector, thermal_coal_g, other_hydrocarbons_g, jet_fuel_g, 
                                other_petroleum_products_g, other_solar_g, hydrogen, ammonia,efuel], axis = 0)\
                                    .reset_index(drop = True)

    # Sectors

    ele_tf = df_fuel_sector[df_fuel_sector['sectors'].isin(['09_01_01_electricity_plants', 
                                                            '09_02_01_electricity_plants'])]

    ele_tf_g = ele_tf.groupby(['economy', 'year', 'fuels'])['value']\
        .sum().reset_index().assign(sectors = '09_01_electricity_plants')

    chp_tf = df_fuel_sector[df_fuel_sector['sectors'].isin(['09_01_02_chp_plants', 
                                                            '09_02_02_chp_plants'])]

    chp_tf_g = chp_tf.groupby(['economy', 'year', 'fuels'])['value']\
        .sum().reset_index().assign(sectors = '09_02_chp_plants')

    heat_tf = df_fuel_sector[df_fuel_sector['sectors'].isin(['09_01_03_heat_plants', 
                                                            '09_02_03_heat_plants'])]

    heat_tf_g = heat_tf.groupby(['economy', 'year', 'fuels'])['value']\
        .sum().reset_index().assign(sectors = '09_x_heat_plants')

    bldg = df_fuel_sector[df_fuel_sector['sectors'].isin(['16_01_commercial_and_public_services', 
                                                        '16_02_residential'])] # notice that the item numbers are replaced by self-defined version.

    bldg_g = bldg.groupby(['economy', 'year', 'fuels'])['value']\
        .sum().reset_index().assign(sectors = '16_01_buildings')

    agfi = df_fuel_sector[df_fuel_sector['sectors'].isin(['16_03_agriculture', 
                                                        '16_04_fishing'])] # notice that the item numbers are replaced by self-defined version.

    agfi_g = agfi.groupby(['economy', 'year', 'fuels'])['value']\
        .sum().reset_index().assign(sectors = '16_02_agriculture_and_fishing')

    ele_gwh = df_fuel_sector[df_fuel_sector['sectors'].isin(['18_01_map_electricity_plants', 
                                                            '18_03_ap_electricity_plants'])] 

    ele_gwh_g = ele_gwh.groupby(['economy', 'year', 'fuels'])['value']\
        .sum().reset_index().assign(sectors = '18_01_electricity_plants')

    chp_gwh = df_fuel_sector[df_fuel_sector['sectors'].isin(['18_02_map_chp_plants', 
                                                            '18_04_ap_chp_plants'])] 

    chp_gwh_g = chp_gwh.groupby(['economy', 'year', 'fuels'])['value']\
        .sum().reset_index().assign(sectors = '18_02_chp_plants')

    chp_pj = df_fuel_sector[df_fuel_sector['sectors'].isin(['19_01_map_chp_plants', 
                                                            '19_03_ap_chp_plants'])] 

    chp_pj_g = chp_pj.groupby(['economy', 'year', 'fuels'])['value']\
        .sum().reset_index().assign(sectors = '19_01_chp plants')

    heat_pj = df_fuel_sector[df_fuel_sector['sectors'].isin(['19_02_map_heat_plants', 
                                                            '19_04_ap_heat_plants'])] 

    heat_pj_g = heat_pj.groupby(['economy', 'year', 'fuels'])['value']\
        .sum().reset_index().assign(sectors = '19_02_heat_plants')

    # concat for sectors
    # - exclude the data that are used to do the aggreation 
    # - include the data you just create 

    df_fuel_sector = df_fuel_sector[~df_fuel_sector['sectors'].isin(['09_01_main_activity_producer', 
                                                                    '09_02_autoproducers',
                                                                    '09_01_01_electricity_plants', 
                                                                    '09_02_01_electricity_plants',
                                                                    '09_01_02_chp_plants',
                                                                    '09_02_02_chp_plants',
                                                                    '09_01_03_heat_plants',
                                                                    '09_02_03_heat_plants',
                                                                    '18_01_map_electricity_plants',
                                                                    '18_03_ap_electricity_plants',
                                                                    '18_02_map_chp_plants',
                                                                    '18_04_ap_chp_plants'])]

    df_fuel_sector_temp = pd.concat([df_fuel_sector,
                                    ele_tf_g, chp_tf_g, heat_tf_g,
                                    bldg_g, agfi_g, 
                                    ele_gwh_g, chp_gwh_g, 
                                    chp_pj_g, heat_pj_g], 
                                    axis = 0).reset_index(drop = True)

    # Merge layouts
    sector_fuel_layout = pd.merge(sector_layout, fuel_layout, how = 'cross') 
    #sector_fuel_layout.to_csv('./data/self_defined_layout/sector_fuel_layout.csv', index = False) 

    economy_df = pd.DataFrame(df_fuel_sector_temp['economy'].unique(), columns=['economy'])
    year_df = pd.DataFrame(df_fuel_sector_temp['year'].unique(), columns=['year'])

    df_econ_year = pd.merge(economy_df, year_df, how = 'cross')

    sector_fuel_econ_year_layout =  pd.merge(df_econ_year, sector_fuel_layout, how = 'cross')

    merged_df = pd.merge(df_fuel_sector_temp, sector_fuel_econ_year_layout, how = 'right', \
                        left_on = ['economy', 'year', 'fuels', 'sectors'], \
                        right_on = ['economy', 'year', 'fuel_key_col', 'sector_key_col'])  

    # Drop useless columns
    merged_df_clean = merged_df.drop(['fuels_x', 'sectors_x', 'sector_key_col', 'fuel_key_col'], axis = 1)

    merged_df_clean = merged_df_clean.rename(columns={'sectors_y': 'sectors', 'fuels_y': 'fuels'})

    merged_df_clean = merged_df_clean.reindex(columns = ['economy', 'year', 'sectors', 'sub1sectors', 'sub2sectors', 
                                                        'sub3sectors', 'sub4sectors', 'fuels', 'subfuels', 'value'])

    # Export merged

    ## pivot the data first to save time
    # - You have to fillna with some numerical number first, otherwise the row with np.nan will be ignore by ```pivot_table``` 
    # - ```value_not_in_the_range``` is assigned to a value that is differnt from the current dataframe. (series) 
    # - We will replace them with np.nan after ```pivot_table```
    # - BTW, replacing with string may cause problem in ```pivot_table```

    #value_not_in_the_range = merged_df_clean['value'].min() - 1
    #merged_df_clean = merged_df_clean.fillna(value_not_in_the_range)

    merged_df_clean = merged_df_clean.fillna(0) #replacing empty cells with '0'

    merged_df_clean_wide = merged_df_clean.pivot_table(index = ['economy', 'sectors', 'sub1sectors', 'sub2sectors', 
                                                                'sub3sectors', 'sub4sectors', 'fuels', 'subfuels'],
                                                                columns = 'year', values = 'value').reset_index(drop = False)

    merged_df_clean_wide.replace(value_not_in_the_range, np.nan, inplace = True)

    # # Save file and subset in next script
    # if USE_SINGLE_ECONOMY:
    #     merged_df_clean_wide.to_csv(interim_path + f'interim_{SINGLE_ECONOMY}.csv', index = False)
    # else:
    #     merged_df_clean_wide.to_csv(interim_path + 'interim.csv', index = False)

    return merged_df_clean_wide
import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from utility_functions import *
ZERO_EMISSION_FUELS = [
    '09_nuclear',
    '10_hydro',
    '11_geothermal',
    '12_solar',
    '13_tide_wave_ocean',
    '14_wind',
    '16_09_other_sources',
    '16_x_ammonia',
    '16_x_efuel',
    '16_x_hydrogen',
    '17_electricity',
    '18_heat',
    '19_total',
    '20_total_renewables',
    '21_modern_renewables',
    '17_x_green_electricity',
]
ZERO_NET_EMISSION_FUELS = [
    '15_solid_biomass',
    '15_01_fuelwood_and_woodwaste',
    '15_02_bagasse',
    '16_09_other_sources',
    '15_03_charcoal',
    '15_04_black_liquor',
    '15_05_other_biomass',
    '16_others',
    '16_01_biogas',
    '16_02_industrial_waste',
    '16_03_municipal_solid_waste_renewable',
    '16_04_municipal_solid_waste_nonrenewable',
    '16_05_biogasoline',
    '16_06_biodiesel',
    '16_07_bio_jet_kerosene',
    '16_08_other_liquid_biofuels'
]

def calculate_emissions(final_df, SINGLE_ECONOMY_ID, INCLUDE_ZERO_NET_EMISSION_FUELS = False):
    # Make a copy of the final_df to avoid unintended modifications on the original
    final_df_copy = final_df.copy()

    if final_df.isnull().any().any():
        #identify what columns the nans are in
        cols_with_nans = final_df.columns[final_df.isnull().any()].tolist()
        breakpoint()
        #if they are from 
        raise Exception('There are some NaNs in the dataframe before calculating emissions. The cols are {}'.format(cols_with_nans))
    
    # Read in emissions factors
    emissions_factors = pd.read_csv('data/9th_edition_emissions_factors_all_gases_simplified.csv')

    # Melt the final_df on all cols except year cols
    year_cols = [col for col in final_df.columns if str(col).isnumeric()]
    non_year_and_value_cols = [col for col in final_df.columns if col not in year_cols]

    final_df_copy = pd.melt(final_df_copy, id_vars=non_year_and_value_cols, value_vars=year_cols, var_name='year', value_name='value')

    # Set any NaNs in the value col to 0
    final_df_copy['value'] = final_df_copy['value'].fillna(0)

    #within emisisons factors, filter for the correct values in tehse cols: 'GWP_type', 'GWP', then drop these cols
    emissions_factors = emissions_factors.loc[(emissions_factors['GWP_type'] == 'GWP_100')].copy()
    emissions_factors.drop(columns=['GWP_type', 'GWP'], inplace=True)
    # Merge emissions factors based on fuels and sectors cols
    
    final_df_copy = pd.merge(final_df_copy, emissions_factors, how='outer', on=['fuels', 'subfuels', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors'], indicator=True)
    
    # Handle duplicate rows after merging
    if final_df_copy.duplicated(subset=['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels', 'subtotal_layout', 'subtotal_results', 'year', 'Unit', 'Gas', 'CO2e emissions factor', 'Sector not applicable', 'Fuel not applicable', 'No expected energy use', '_merge']).any():
        breakpoint()
        raise Exception('There are some duplicate rows in the merged dataframe.')

    # Check for rows that did not merge correctly
    if len(final_df_copy.loc[final_df_copy['_merge'] == 'left_only']) > 0:
        final_df_copy = create_dummy_emissions_factors_rows(final_df_copy, emissions_factors, ZERO_EMISSION_FUELS, ZERO_NET_EMISSION_FUELS, INCLUDE_ZERO_NET_EMISSION_FUELS, SINGLE_ECONOMY_ID)

    # Remove right_only rows
    final_df_copy = final_df_copy[final_df_copy['_merge'] != 'right_only'].drop(columns=['_merge'])

    # Set emissions factor to 0 where sectors or fuels are not applicable
    condition = (final_df_copy['Sector not applicable'] == True) | (final_df_copy['Fuel not applicable'] == True) | (final_df_copy['No expected energy use'] == True)
    final_df_copy.loc[condition, 'CO2e emissions factor'] = 0

    # Drop unnecessary columns
    final_df_copy = final_df_copy.drop(columns=['Unit', 'Sector not applicable', 'Fuel not applicable', 'No expected energy use'])

    # Check for duplicate rows again
    if final_df_copy.duplicated(subset=non_year_and_value_cols + ['year', 'Gas']).any():
        raise Exception('There are some duplicate rows in the final emissions dataframe.')

    def split_ccs_rows(df):
        # Reset index to ensure clean indexing
        df = df.reset_index(drop=True)

        # Define conditions for CCS rows
        industry_ccs_condition = df['sub3sectors'].str.endswith('_ccs') & df['sub3sectors'].str.startswith('14_') & (df['Gas'] == 'CARBON DIOXIDE')
        power_ccs_condition = df['sub2sectors'].str.endswith('_ccs') & df['sub2sectors'].str.startswith('09_') & (df['Gas'] == 'CARBON DIOXIDE')
        breakpoint()#why is 09 not hacing any emisisons output when we attached _captured emissions to it?
        # Create a copy for manipulation
        df_copy = df.loc[industry_ccs_condition | power_ccs_condition].copy()

        #and remove asny positive values in 09_total_transformation_sector since they are not combustion emissions, they are produced energy
        df_copy.loc[(df_copy['sectors'] == '09_total_transformation_sector') & (df_copy['value'] > 0), 'value'] = 0
        #then set all values in 09_total_transformation_sector to positive 
        df_copy.loc[(df_copy['sectors'] == '09_total_transformation_sector'), 'value'] = abs(df_copy.loc[(df_copy['sectors'] == '09_total_transformation_sector'), 'value'])
        #check for any remaining negative values in the df_copy
        if df_copy.loc[df_copy['value'] < 0].shape[0] > 0:
            breakpoint()
            raise Exception('There are some negative CO2e emissions factors in the CCS rows. Please check the data.')
        
        # Apply sector-specific factors
        df_copy.loc[industry_ccs_condition, 'CO2e emissions factor'] *= -(1-INDUSTRY_CCS_ABATEMENT_RATE)
        df_copy.loc[power_ccs_condition, 'CO2e emissions factor'] *= -(1-POWER_CCS_ABATEMENT_RATE)
        #THEN SINCE WE NEED TO KNOW EXACTLY WHERE THESE SECTORS ARE THAT HAVE THEIR EMSSONS CAPTURED, ADD 'CAPTURED EMISSIONS' TO THEIR NAME. 
        df_copy.loc[industry_ccs_condition, 'sub3sectors'] += '_captured_emissions'
        df_copy.loc[power_ccs_condition, 'sub2sectors'] += '_captured_emissions'

        # # Adjust original df for the non-captured portion #NOTE I RMEOVED THIS BECAUSE IT SEEMED LIKE THE BEST METHOD SHOULD BE TO ALLOW THE USER TO MINUS THE CAPTURED PORTION FROM THE TOTAL EMISSIONS TEHMSELVES
        # df.loc[industry_ccs_condition, 'CO2e emissions factor'] *= INDUSTRY_CCS_ABATEMENT_RATE
        # df.loc[power_ccs_condition, 'CO2e emissions factor'] *= POWER_CCS_ABATEMENT_RATE

        # Concatenate the modified copy back with the original dataframe
        df_final = pd.concat([df, df_copy], ignore_index=True)

        return df_final
    
    # remove asny positive values in 09_total_transformation_sector since they are not combustion emissions, they are produced energy
    final_df_copy.loc[(final_df_copy['sectors'] == '09_total_transformation_sector') & (final_df_copy['value'] > 0), 'CO2e emissions factor'] = 0
    
    # Apply the emissions factor adjustments for CCS
    final_df_copy = split_ccs_rows(final_df_copy)

    # Set emissions factors for bunkers sectors to 0
    final_df_copy.loc[final_df_copy['sectors'] == '05_international_aviation_bunkers', 'CO2e emissions factor'] = 0
    final_df_copy.loc[final_df_copy['sectors'] == '06_international_marine_bunkers', 'CO2e emissions factor'] = 0
    
    # #and set 10_02_transmission_and_distribution_losses to 0
    # final_df_copy.loc[(final_df_copy['sub1sectors'] == '10_02_transmission_and_distribution_losses'), 'CO2e emissions factor'] = 0#actually nope, this is already excluded in the emissions factors since it has Sector not applicable = True (same as the non applicable tranformation sectors)
    
    #set any ZERO_NET_EMISSION_FUELS to 0 - it is useful to have their emisisons available but not for this output:
    if INCLUDE_ZERO_NET_EMISSION_FUELS == False:
        final_df_copy.loc[final_df_copy['fuels'].isin(ZERO_NET_EMISSION_FUELS), 'CO2e emissions factor'] = 0
    
    # Calculate emissions
    # final_df_copy['value'] = abs(final_df_copy['value']) (change any negative values to absolute first))
    final_df_copy['CO2e emissions (Mt/PJ)'] = (final_df_copy['value'] * final_df_copy['CO2e emissions factor'])
    # Drop unnecessary columns
    final_df_copy.drop(columns=['CO2e emissions factor', 'value'], inplace=True)
    
    ###############
    final_df_copy = calculate_aggregate_total_combustion_values(final_df_copy, non_year_and_value_cols)
    ###############
    final_df_copy = aggregate_co2e_emissions(final_df_copy, non_year_and_value_cols, SINGLE_ECONOMY_ID)
    ###############
    
    # Check for duplicates and NaNs
    if final_df_copy.duplicated(subset=non_year_and_value_cols + ['year', 'Gas']).any():
        breakpoint()
        raise Exception('There are some duplicate rows in the final emissions dataframe.')
    if final_df_copy.isnull().any().any():
        #identify what columns the nans are in
        cols_with_nans = final_df_copy.columns[final_df_copy.isnull().any()].tolist()
        breakpoint()
        raise Exception('There are some NaNs in the final emissions dataframe.')

    # Pivot data to make it wide format
    final_df_copy = final_df_copy.pivot_table(index=non_year_and_value_cols + ['Gas'], columns='year', values='CO2e emissions (Mt/PJ)').reset_index()
    
    # Save emissions data
    folder_path = f'results/{SINGLE_ECONOMY_ID}/emissions/'
    old_folder_path = f'{folder_path}/old'
    if not os.path.exists(folder_path) and isinstance(SINGLE_ECONOMY_ID, str):
        os.makedirs(folder_path)
    if not os.path.exists(old_folder_path):
        os.makedirs(old_folder_path)

    gas_to_filename = {'CO2e': 'emissions_co2e', 'CARBON DIOXIDE': 'emissions_co2', 'METHANE': 'emissions_ch4', 'NITROUS OXIDE': 'emissions_no2'}
    for gas in gas_to_filename.keys():
        gas_df = final_df_copy.loc[final_df_copy['Gas'] == gas].copy()
        gas_df.drop(columns=['Gas'], inplace=True)
        filename = gas_to_filename[gas]

        # Move previous emissions file to 'old' folder if exists
        previous_emissions_filename = next((file for file in os.listdir(folder_path) if file.startswith(f'{filename}_{SINGLE_ECONOMY_ID}') and file.endswith('.csv')), None)
        if previous_emissions_filename:
            old_file_path = f'{folder_path}/{previous_emissions_filename}'
            new_old_file_path = f'{old_folder_path}/{previous_emissions_filename}'
            if os.path.exists(new_old_file_path):
                os.remove(new_old_file_path)
            os.rename(old_file_path, new_old_file_path)

        # Save the new emissions data
        date_today = datetime.now().strftime('%Y%m%d')
        if isinstance(SINGLE_ECONOMY_ID, str):
            gas_df.to_csv(f'{folder_path}/{filename}_{SINGLE_ECONOMY_ID}_{date_today}.csv', index=False)
        else:
            gas_df.to_csv(f'results/{filename}_{date_today}.csv', index=False)

    return final_df_copy


def calculate_aggregate_total_combustion_values(final_df_copy, non_year_and_value_cols):
    #also, since its a useful input, we want to extract and calc the total combustion emissions by gas type  we will sum these up and set the sector to '20_total_combustion_emissions' so we have an easy way to extract the total combustion emissions for each gas type
    selected_categories = [
    "09_total_transformation_sector",
    "10_losses_and_own_use",
    "14_industry_sector",
    "15_transport_sector",
    "16_other_sector"
    ]
    historical_data = final_df_copy.loc[final_df_copy['year'].astype(int) <= OUTLOOK_BASE_YEAR].copy()
    projected_data = final_df_copy.loc[final_df_copy['year'].astype(int)  > OUTLOOK_BASE_YEAR].copy()
    total_emissions_historical = historical_data.loc[historical_data['sectors'].isin(selected_categories) & (historical_data['subtotal_layout']==False)].copy()
    total_emissions_projected = projected_data.loc[projected_data['sectors'].isin(selected_categories) & (projected_data['subtotal_results']==False)].copy()
    
    #set the subtotal col for the opposite type to False snce it doesnt matter since its essentially 0 anyway (we're operating in tall dfs right now but if we made it wide they would be 0)
    total_emissions_historical['subtotal_results'] = False
    total_emissions_projected['subtotal_layout'] = False
    
    #also drop anything where _captured_emissions is in the subsectors3 or 2 columns since we are only interested in the total combustion emissions and not the net emissions
    captured_emissions_proj = total_emissions_projected.loc[(total_emissions_projected['sub3sectors'].str.contains('_captured_emissions', na=False)) | (total_emissions_projected['sub2sectors'].str.contains('_captured_emissions', na=False))].copy()
    captured_emissions_hist = total_emissions_historical.loc[(total_emissions_historical['sub3sectors'].str.contains('_captured_emissions', na=False)) | (total_emissions_historical['sub2sectors'].str.contains('_captured_emissions', na=False))].copy()  
    
    total_emissions_historical = total_emissions_historical.loc[(~total_emissions_historical['sub3sectors'].str.contains('_captured_emissions', na=False)) & (~total_emissions_historical['sub2sectors'].str.contains('_captured_emissions', na=False))].copy()
    total_emissions_projected = total_emissions_projected.loc[(~total_emissions_projected['sub2sectors'].str.contains('_captured_emissions', na=False) & (~total_emissions_projected['sub3sectors'].str.contains('_captured_emissions', na=False)))].copy()
    
    total_emissions_projected_copy = total_emissions_projected.copy()
    total_emissions_historical_copy = total_emissions_historical.copy()
    
    total_emissions_historical['sectors'] = '20_total_combustion_emissions'
    total_emissions_historical['sub1sectors'] = 'x'
    total_emissions_historical['sub2sectors'] = 'x'
    total_emissions_historical['sub3sectors'] = 'x'
    total_emissions_historical['sub4sectors'] = 'x'    
    
    total_emissions_projected['sectors'] = '20_total_combustion_emissions'
    total_emissions_projected['sub1sectors'] = 'x'
    total_emissions_projected['sub2sectors'] = 'x'
    total_emissions_projected['sub3sectors'] = 'x'
    total_emissions_projected['sub4sectors'] = 'x'    
    
    #set any negative CO2e emissions (Mt/PJ)s to positive since we habe that in the transfromation sector and own use sector
    total_emissions_historical['CO2e emissions (Mt/PJ)'] = abs(total_emissions_historical['CO2e emissions (Mt/PJ)'])
    total_emissions_projected['CO2e emissions (Mt/PJ)'] = abs(total_emissions_projected['CO2e emissions (Mt/PJ)'])
    
    #sum these up 
    total_emissions_historical = total_emissions_historical.groupby(non_year_and_value_cols + ['year', 'Gas']).sum().reset_index()
    total_emissions_projected = total_emissions_projected.groupby(non_year_and_value_cols + ['year', 'Gas']).sum().reset_index()
    
    #concat
    total_emissions = pd.concat([total_emissions_historical, total_emissions_projected], ignore_index=True)
    final_df_copy = pd.concat([final_df_copy, total_emissions], ignore_index=True)
    ###############
    #and also to help the user we will create a fuel called 22_total_combustion_emissions which will just show be lines where the row is part of the total_combustion_emissions (i.e. the sector is within the selected_categories)
    
    total_emissions_projected_copy['fuels'] = '22_total_combustion_emissions'
    total_emissions_projected_copy['subfuels'] = 'x'
    
    total_emissions_historical_copy['fuels'] = '22_total_combustion_emissions'
    total_emissions_historical_copy['subfuels'] = 'x'
    
    #set any negative CO2e emissions (Mt/PJ)s to positive since we have that in the transfromation sector and own use sector
    total_emissions_projected_copy['CO2e emissions (Mt/PJ)'] = abs(total_emissions_projected_copy['CO2e emissions (Mt/PJ)'])
    total_emissions_historical_copy['CO2e emissions (Mt/PJ)'] = abs(total_emissions_historical_copy['CO2e emissions (Mt/PJ)'])

    #sum these up 
    total_emissions_projected_copy = total_emissions_projected_copy.groupby(non_year_and_value_cols + ['year', 'Gas']).sum().reset_index()
    total_emissions_historical_copy = total_emissions_historical_copy.groupby(non_year_and_value_cols + ['year', 'Gas']).sum().reset_index()
    
    #concat
    total_emissions_copy = pd.concat([total_emissions_historical_copy, total_emissions_projected_copy], ignore_index=True)
    
    final_df_copy = pd.concat([final_df_copy, total_emissions_copy], ignore_index=True)
    ###############
    
    #and also calcualte total net emisisons. that is the emissions when you drop the effects of carbon capture. We will just calcualte it as a total in sectors and fuels so its not too complicated. To do this justconcat the captured_emissions_proj and captured_emissions_hist to total_emissions then rename the sectors to 21_total_combustion_emissions_net and fuels to 23_total_combustion_emissions_net. then sum it up!
    net_emissions = pd.concat([captured_emissions_proj, captured_emissions_hist, total_emissions], ignore_index=True)
    net_emissions['sectors'] = '21_total_combustion_emissions_net'
    net_emissions['fuels'] = '23_total_combustion_emissions_net'
    net_emissions['sub1sectors'] = 'x'
    net_emissions['sub2sectors'] = 'x'
    net_emissions['sub3sectors'] = 'x'
    net_emissions['sub4sectors'] = 'x'
    net_emissions['subfuels'] = 'x'
    
    net_emissions = net_emissions.groupby(non_year_and_value_cols + ['year', 'Gas']).sum().reset_index()
    final_df_copy = pd.concat([final_df_copy, net_emissions], ignore_index=True)
    
    return final_df_copy


def aggregate_co2e_emissions(final_df_copy, non_year_and_value_cols, SINGLE_ECONOMY_ID):
    # Calculate CO2e emissions
    # Aggregate emissions data to get co2e totals (not by gas type)
    final_df_aggregate = final_df_copy.groupby(non_year_and_value_cols + ['year']).sum().reset_index()
    final_df_aggregate['Gas'] = 'CO2e'
    ###
    #jsut because its often gonna be something we quetion, just calcaulte the difference in co2e and co2 emissions total using sector == 20_total_combustion_emissions
    total_co2e_combustion_emissions = final_df_aggregate.loc[final_df_aggregate['sectors'] == '20_total_combustion_emissions'].copy()
    total_co2_combustion_emissions = final_df_copy.loc[(final_df_copy['sectors'] == '20_total_combustion_emissions') & (final_df_copy['Gas'] == 'CARBON DIOXIDE')].copy()
    
    #merge, sum and then calc the difference. then save to a csv
    total_combustion_emissions = pd.merge(total_co2e_combustion_emissions, total_co2_combustion_emissions, how='outer', on=non_year_and_value_cols + ['year'], indicator=True, suffixes=('_co2e', '_co2'))
    total_combustion_emissions['CO2e emissions (Mt/PJ)_co2e'] = total_combustion_emissions['CO2e emissions (Mt/PJ)_co2e'].fillna(0)
    total_combustion_emissions['CO2e emissions (Mt/PJ)_co2'] = total_combustion_emissions['CO2e emissions (Mt/PJ)_co2'].fillna(0)

    #sum
    total_combustion_emissions = total_combustion_emissions.groupby(non_year_and_value_cols + ['year']).sum(numeric_only=True).reset_index()
    total_combustion_emissions['difference'] = total_combustion_emissions['CO2e emissions (Mt/PJ)_co2e'] - total_combustion_emissions['CO2e emissions (Mt/PJ)_co2']
    total_combustion_emissions = total_combustion_emissions.groupby(non_year_and_value_cols + ['year']).sum(numeric_only=True).reset_index()
    total_combustion_emissions['difference'] = total_combustion_emissions['CO2e emissions (Mt/PJ)_co2e'] - total_combustion_emissions['CO2e emissions (Mt/PJ)_co2']
    total_combustion_emissions.to_csv(f'./data/temp/error_checking/difference_in_co2e_and_co2_emissions_{SINGLE_ECONOMY_ID}.csv')  
    #save a plot to plotting_output
    # ax = total_combustion_emissions.groupby('year').sum(numeric_only=True).reset_index().plot(x='year', y='difference', kind='line', title=f'Difference in sum of all gas emissions (co2e) compared to CO2 emissions for {SINGLE_ECONOMY_ID}', xlabel='Year', ylabel='(Mt/PJ)')
    # fig = ax.get_figure()
    # fig.savefig(f'./plotting_output/difference_in_co2e_and_co2_emissions_{SINGLE_ECONOMY_ID}.png')
    
    # Concatenate aggregate and original emissions data
    final_df_copy = pd.concat([final_df_copy, final_df_aggregate], ignore_index=True)
    return final_df_copy
    

def create_dummy_emissions_factors_rows(final_df_copy, emissions_factors, ZERO_EMISSION_FUELS, ZERO_NET_EMISSION_FUELS, INCLUDE_ZERO_NET_EMISSION_FUELS, SINGLE_ECONOMY_ID):
    #TEST#
    #first, drop any of these rows which are 0's and tehn check if there are any left: (this make it so we can more easily add in new rows to the ebt data without having to create an emissions mapping for them)
    final_df_copy = final_df_copy.loc[~((final_df_copy['value'] == 0) & (final_df_copy['_merge'] == 'left_only'))].copy()
    if len(final_df_copy.loc[final_df_copy['_merge'] == 'left_only']) > 0:
        #chekc if the missing values are where subfuels contians 'unallocated'. that is where we recently made upadtes which may be creating new rows which dont have emisisons factors. we will jsut add these to a csv for now and do them all at once later. then we will just replace the emisisons factors with the average of the other rows with the same fuel . this is pretty inexact because there are different emissions factors for different sectors and especially different subfuels but for now it will do
        unallocated_rows = final_df_copy.loc[(final_df_copy['_merge'] == 'left_only') & (final_df_copy['subfuels'].str.contains('unallocated', na=False))].copy()
        # breakpoint()
        if len(unallocated_rows) > 0:
            unallocated_rows.to_csv(f'./data/temp/error_checking/emissions_missing_mappings_{SINGLE_ECONOMY_ID}_unallocated.csv')#wee can gather these up and do them all at once later
            
            #rejoin some emissions factors but just use the fuel/subfuel type so as to not create too much extra work:
            emissions_factors_no_na = emissions_factors.copy()
            emissions_factors_no_na = emissions_factors_no_na.dropna(subset=['CO2e emissions factor']).copy()
            emissions_factors_by_fuels = emissions_factors_no_na.groupby(['Unit', 'Gas', 'fuels'])[['CO2e emissions factor']].mean().reset_index()
            #drop the remanants from the merge and do it again:
            unallocated_rows = unallocated_rows.drop(columns=['Unit', 'Gas', 'CO2e emissions factor', 'Sector not applicable', 'Fuel not applicable', 'No expected energy use', '_merge']).copy()
            #do the merge again
            unallocated_rows = pd.merge(unallocated_rows, emissions_factors_by_fuels, how='outer', on=['fuels'], indicator=True)
            #drop right only rows as they are where we dont need the new emissions factors
            unallocated_rows = unallocated_rows[unallocated_rows['_merge'] != 'right_only'].copy()
            
            # Convert '_merge' column to string to avoid category issues
            unallocated_rows['_merge'] = unallocated_rows['_merge'].astype(str)
            
            #wherer teh merge is left only and the fuels or subfuels is in ZERO_NET_EMISSION_FUELS, set merge to ZERO_NET_EMISSION_FUELS so we doont have to worry about them (their emissions will be nan)
            # unallocated_rows.loc[(unallocated_rows['_merge'] == 'left_only') & (unallocated_rows['fuels'].isin(ZERO_EMISSION_FUELS)), 'CO2e emissions factor'] = np.nan
            unallocated_rows.loc[(unallocated_rows['_merge'] == 'left_only') & (unallocated_rows['fuels'].isin(ZERO_EMISSION_FUELS)), '_merge'] = 'ZERO_EMISSION_FUELS'
            
            # unallocated_rows.loc[(unallocated_rows['_merge'] == 'left_only') & (unallocated_rows['subfuels'].isin(ZERO_EMISSION_FUELS)), 'CO2e emissions factor'] =  np.nan
            unallocated_rows.loc[(unallocated_rows['_merge'] == 'left_only') & (unallocated_rows['subfuels'].isin(ZERO_EMISSION_FUELS)), '_merge'] = 'ZERO_EMISSION_FUELS'
            #now where the merge is ZERO_EMISSION_FUELS, set the emissions factor to 0 and set the gas to CARBON DIOXIDE', 'METHANE', 'NITROUS OXIDE', 'CO2e'],
            ZERO_EMISSION_FUELS_rows = unallocated_rows.loc[unallocated_rows['_merge'] == 'ZERO_EMISSION_FUELS']
            unallocated_rows = unallocated_rows[unallocated_rows['_merge'] != 'ZERO_EMISSION_FUELS'].copy()
            unallocated_rows['_merge'] = 'UNALLOCATED_NON_ZERO'
            for gas in ['CARBON DIOXIDE', 'METHANE', 'NITROUS OXIDE']:
                ZERO_EMISSION_FUELS_rows['Gas'] = gas
                ZERO_EMISSION_FUELS_rows['CO2e emissions factor'] = 0
                unallocated_rows = pd.concat([unallocated_rows, ZERO_EMISSION_FUELS_rows.copy()], ignore_index=True)
            # Identify any left_only and cause error if so
            if len(unallocated_rows.loc[unallocated_rows['_merge'] == 'left_only']) > 0:
                breakpoint()
                raise Exception(f'Some rows {unallocated_rows.loc[unallocated_rows["_merge"] == "left_only"]} did not merge with the emissions factors data. Please create more mappings for the missing values.')
            #otherwise, concat the two dfs back together
            final_df_copy = final_df_copy[~(final_df_copy['_merge'] == 'left_only')& ~(final_df_copy['subfuels'].str.contains('unallocated', na=False))].copy()
            #add the cols: Sector not applicable	Fuel not applicable	No expected energy use with FALSE	FALSE	FALSE #these dont matter for the ZERO_EMISSION_FUELS and for the others,  well its only temporary.
            unallocated_rows['Sector not applicable'] = False
            unallocated_rows['Fuel not applicable'] = False
            unallocated_rows['No expected energy use'] = False
            unallocated_rows['_merge'] = 'unallocated'
            final_df_copy = pd.concat([final_df_copy, unallocated_rows], ignore_index=True)   
        
        #then check for rows wehre the fuel or subfuel is in one of the ZERO_NET_EMISSION_FUELS (if INCLUDE_ZERO_NET_EMISSION_FUELS = False) or ZERO_EMISSION_FUELS
        zero_emissions_rows = final_df_copy.loc[(final_df_copy['_merge'] == 'left_only') & ((final_df_copy['fuels'].isin(ZERO_EMISSION_FUELS)) | (final_df_copy['subfuels'].isin(ZERO_EMISSION_FUELS)) | (final_df_copy['fuels'].isin(ZERO_NET_EMISSION_FUELS)) | (final_df_copy['subfuels'].isin(ZERO_NET_EMISSION_FUELS)))].copy()
        if len(zero_emissions_rows) > 0:            
            if INCLUDE_ZERO_NET_EMISSION_FUELS == True:#dont create dummy  rows where the fuel or subfuel is in ZERO_NET_EMISSION_FUELS sicne we want to include them with proper emissions factors
                zero_emissions_rows= zero_emissions_rows[~((zero_emissions_rows['fuels'].isin(ZERO_NET_EMISSION_FUELS)) | (zero_emissions_rows['subfuels'].isin(ZERO_NET_EMISSION_FUELS)))]
                final_df_copy = final_df_copy[~((final_df_copy['fuels'].isin(ZERO_EMISSION_FUELS)) | (final_df_copy['subfuels'].isin(ZERO_EMISSION_FUELS)))]
            else:
                final_df_copy = final_df_copy[~((final_df_copy['fuels'].isin(ZERO_EMISSION_FUELS)) | (final_df_copy['subfuels'].isin(ZERO_EMISSION_FUELS)) | ((final_df_copy['fuels'].isin(ZERO_NET_EMISSION_FUELS)) | (final_df_copy['subfuels'].isin(ZERO_NET_EMISSION_FUELS))))]
                
            #fill the emissions factors with 0 and create a row for each gas type and set sector not applicable, fuel not applicable and no expected energy use to False
            for gas in ['CARBON DIOXIDE', 'METHANE', 'NITROUS OXIDE']:
                zero_emissions_rows['Gas'] = gas
                zero_emissions_rows['CO2e emissions factor'] = 0
                zero_emissions_rows['Sector not applicable'] = False
                zero_emissions_rows['Fuel not applicable'] = False
                zero_emissions_rows['No expected energy use'] = False
                zero_emissions_rows['_merge'] = 'ZERO_EMISSION_FUELS'
                final_df_copy = pd.concat([final_df_copy, zero_emissions_rows.copy()], ignore_index=True)
        #if there are any left_only rows left, then we have a problem
        if len(final_df_copy.loc[final_df_copy['_merge'] == 'left_only']) > 0:
            breakpoint()
            a = final_df_copy.loc[final_df_copy['_merge'] == 'left_only']
            #add on the other rows where the merge was in zero_emissions_rows and unallocated_rows so the user can do them at same time
            a = pd.concat([a, zero_emissions_rows, unallocated_rows], ignore_index=True)
            a.to_csv(f'./data/temp/error_checking/emissions_missing_mappings_{SINGLE_ECONOMY_ID}.csv')        
            final_df_copy = final_df_copy[final_df_copy['_merge'] != 'left_only']
            raise Exception(f'Some rows {a} did not merge with the emissions factors data. Please create more mappings for the missing values.')
    return final_df_copy
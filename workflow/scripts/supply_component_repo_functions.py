# Pipeline energy consumption (even though it's within the demand block, it's only modelled after consumption of gas is determined within TFC and power)

# 07_petroleum_products
# 08_gas
# 17_electricity

# Set working directory to be the project folder 
import os
import re
import pandas as pd 
import numpy as np
import glob
from datetime import datetime
from utility_functions import *
import yaml 

timestamp = datetime.now().strftime('%Y_%m_%d')
# wanted_wd = 'Outlook9th_EBT'
# os.chdir(re.split(wanted_wd, os.getcwd())[0] + wanted_wd)

def pipeline_transport(economy, model_df_clean_wide):
    """This takes in data from the EBT system and separates historical data from projection data. It then calculates the ratio of gas consumption to total consumption in the pipeline sector and uses this ratio to calculate the energy consumption (of gas, petroleum prods and electricity) in the pipeline sector for the projection years.  This was done after demand was modelled, because the energy used for pipeline transport is a function of the demand of gas.
    The function saves the results in a CSV file in the Outlook9th_EBT\data\modelled_data folder for automatic use by the EBT process.
    """
    # 2022 and beyond
    historical_years = list(range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1, 1))
    historical_years_str = [str(i) for i in historical_years]
    proj_years = list(range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1, 1))
    proj_years_str = [str(i) for i in proj_years]

    latest_hist = OUTLOOK_BASE_YEAR
    ref_elec = 0.002
    tgt_elec = 0.004
    # switch_start_year = '2025'

    # Pipeline fuels
    relevant_fuels = ['07_petroleum_products', '08_gas', '17_electricity', '19_total']
    
    #########################
    
    #make all values after the base year 0 in historical data
    historical_data = model_df_clean_wide.copy()
    historical_data[proj_years] = 0
    
    EGEDA_df = historical_data.copy()
    results_df = model_df_clean_wide.copy()
    #ADDITION BY FINN
    #########################

    # latest EGEDA data
    # EGEDA_df = pd.read_csv(latest_EGEDA)
    #grab rows where subtotal_results is False
    # EGEDA_df = EGEDA_df[EGEDA_df['subtotal_layout'] == False].copy().reset_index(drop = True)
    
    EGEDA_df = EGEDA_df.drop(columns = ['subtotal_layout', 'subtotal_results']).copy().reset_index(drop = True)

    # EGEDA pipeline data
    EGEDA_pipe = EGEDA_df[(EGEDA_df['sub1sectors'] == '15_05_pipeline_transport') &
                        (EGEDA_df['fuels'].isin(relevant_fuels)) &
                        (EGEDA_df['subfuels'] == 'x')].copy().reset_index(drop = True)#an alternative to doing it this way is to find subtotal_layout==False and then find the relevant fuels (but not setting subfuels to x since that drops any rows with subfuels - e.g. current method grabs 08_gas which is a subtotal of 08_01_natural_gas)
    
    # for economy in APEC_economies:
    # Save location
    save_location = './results/supply_components/01_pipeline_transport/{}/'.format(economy)

    if not os.path.isdir(save_location):
        os.makedirs(save_location)

    results_ref = results_df[results_df['scenarios'] == 'reference'].copy().reset_index(drop = True)
    results_tgt = results_df[results_df['scenarios'] == 'target'].copy().reset_index(drop = True)
        
    EGEDA_pipe_ref = EGEDA_pipe[(EGEDA_pipe['economy'] == economy) & (EGEDA_pipe['scenarios'] == 'reference')].copy().reset_index(drop = True)
    EGEDA_pipe_tgt = EGEDA_pipe[(EGEDA_pipe['economy'] == economy) & (EGEDA_pipe['scenarios'] == 'target')].copy().reset_index(drop = True)

    # Scenario dictionary with relevant pieces to use later
    scenario_dict = {'ref': [results_ref, EGEDA_pipe_ref, ref_elec],
                    'tgt': [results_tgt, EGEDA_pipe_tgt, tgt_elec]}
    
    for scenario in scenario_dict.keys():
        ############################################################
        #largely finns addition to make things play nice at any stage of the EBT process#
        # Data frame with results from other sectors to use to build trajectories to fill the trans and own df's
        scenario_df = scenario_dict[scenario][0].copy()
        
        demand_sectors = ['15_transport_sector', '14_industry_sector', '16_other_sector'] + ['17_nonenergy_use']
        tfc_df = scenario_df[(scenario_df['sectors'].isin(demand_sectors)) & (scenario_df['fuels'].isin(['08_gas'])) & (scenario_df['subtotal_results'] == False)].copy().reset_index(drop = True)
        #sum to calculate total gas consumption
        tfc_df = tfc_df.groupby(['scenarios','economy','fuels']).sum().reset_index()
        #insert values into the df matching the structure required for the code below
        dummy_df = scenario_df[(scenario_df['sectors'].isin(['12_total_final_consumption'])) &
                        (scenario_df['sub1sectors'] == 'x') &
                        (scenario_df['fuels'].isin(['08_gas']))  &
                        (scenario_df['subfuels'] == 'x')].copy().reset_index(drop = True)
        dummy_df[proj_years] = tfc_df[proj_years]
        tfc_df = dummy_df.copy()
        #and finally minus the pipeline use from the gas consumption in case it is in the folder, so got merged in with the other dmand data
        pipeline_nonspecified_use = scenario_df[(scenario_df['sub1sectors'].isin(['15_05_pipeline_transport', '16_05_nonspecified_others'])) & (scenario_df['fuels'].isin(['08_gas'])) & (scenario_df['subtotal_results'] == False)].copy().reset_index(drop = True)
        if len(pipeline_nonspecified_use) > 0:
            #sum pipeline use and nonspecified others
            pipeline_nonspecified_use = pipeline_nonspecified_use.groupby(['scenarios','economy','fuels']).sum().reset_index()
            #double check the two dataframes are 1 row each and then subtract the pipeline use from the gas consumption
            if len(tfc_df) != 1 or len(pipeline_nonspecified_use) != 1:
                raise Exception('The consumption dataframes are not the expected length')
            tfc_df[proj_years] = tfc_df[proj_years] - pipeline_nonspecified_use[proj_years]
        ############################################################
        
        # Fill NA so they're zeroes instead
        tfc_df = tfc_df.fillna(0)

        # Sum consumption (TFC)
        tfc_df = tfc_df.groupby(['scenarios', 'economy', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'subfuels', 'fuels'])\
                .sum().reset_index().assign(sectors = 'tfc')

        # Dataframes to populate
        pipe_df = scenario_dict[scenario][1]
        pipe_df = pipe_df.fillna(0)
    
        # Define ratio dataframe
        ratio_df = pd.DataFrame(columns = ['fuels', latest_hist] + proj_years)
        ratio_df.loc[0, 'fuels'] = '07_petroleum_products'
        ratio_df.loc[1, 'fuels'] = '08_gas'
        ratio_df.loc[2, 'fuels'] = '17_electricity'

        #check pipe df isnt all zeros. if it is then just skip. 
        sum_vals_proj = tfc_df[proj_years].sum().sum()
        sum_vals_hist = pipe_df[historical_years].sum().sum()
        if sum_vals_proj == 0:
            if sum_vals_hist == 0:
                print(f'{economy} {scenario} has no pipeline transport data. skipping its calculation.')
                continue
            else:
                raise Exception(f'{economy} {scenario} has no projected pipeline transport data but has historical data. This is not expected. Note that it could be because an economy had pipeline demand in the past but no longer has it. If this is the case then add an exception to the code to skip this economy.')
        # Calculate initial ratios for the most recent historical year and projection years
        for year in [latest_hist] + proj_years:
            for fuel in relevant_fuels[:-1]:  # Exclude '19_total' from relevant fuels
                filtered_pipe_df = pipe_df.loc[pipe_df['fuels'] == '19_total', latest_hist]
                
                # Skip processing if the filtered DataFrame is empty
                if filtered_pipe_df.empty:
                    print(f"Skipping year {latest_hist} for '19_total' as there is no data.")
                    continue

                if filtered_pipe_df.values[0] == 0:
                    # If the total consumption for the latest historical year is zero, set ratio to zero
                    ratio_df.loc[ratio_df['fuels'] == fuel, year] = 0
                else:
                    # Calculate the ratio of fuel consumption to total consumption for the latest historical year
                    ratio_df.loc[ratio_df['fuels'] == fuel, year] = (
                        pipe_df.loc[pipe_df['fuels'] == fuel, latest_hist].values[0] / 
                        filtered_pipe_df.values[0]
                    )

        # Drop the '19_total' row as it's not needed for further calculations
        # Check if index 3 exists in the DataFrame
        if 3 in pipe_df.index:
            pipe_df = pipe_df.drop([3]).reset_index(drop=True)
        else:
            print("Index 3 not found in pipe_df. Skipping drop operation.")

        # Fuel switching parameters
        fs_full = scenario_dict[scenario][2]  # Full fuel switching ratio for electricity
        fs_half = fs_full / 2  # Half fuel switching ratio for petroleum products and gas

        # Apply fuel switching for each projection year
        for year in proj_years:
            # Increase electricity ratio until it reaches 1
            if ratio_df.loc[ratio_df['fuels'] == '17_electricity', year - 1].values[0] <= (1 - fs_full):
                ratio_df.loc[ratio_df['fuels'] == '17_electricity', year] = (
                    ratio_df.loc[ratio_df['fuels'] == '17_electricity', year - 1].values[0] + fs_full
                )
            else:
                ratio_df.loc[ratio_df['fuels'] == '17_electricity', year] = 1

            # Decrease petroleum products and gas ratios if both are above the half switching ratio
            if (ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', year - 1].values[0] >= fs_half) and \
            (ratio_df.loc[ratio_df['fuels'] == '08_gas', year - 1].values[0] >= fs_half):
                ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', year] = (
                    ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', year - 1].values[0] - fs_half
                )
                ratio_df.loc[ratio_df['fuels'] == '08_gas', year] = (
                    ratio_df.loc[ratio_df['fuels'] == '08_gas', year - 1].values[0] - fs_half
                )

            # Set ratio to zero if it falls below the half switching ratio
            if ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', year - 1].values[0] < fs_half:
                ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', year] = 0
            if ratio_df.loc[ratio_df['fuels'] == '08_gas', year - 1].values[0] < fs_half:
                ratio_df.loc[ratio_df['fuels'] == '08_gas', year] = 0

            # Further adjust ratios if one of them is zero
            if (ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', year - 1].values[0] >= fs_full) and \
            (ratio_df.loc[ratio_df['fuels'] == '08_gas', year - 1].values[0] == 0):
                ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', year] = (
                    ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', year - 1].values[0] - fs_full
                )
            if (ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', year - 1].values[0] == 0) and \
            (ratio_df.loc[ratio_df['fuels'] == '08_gas', year - 1].values[0] >= fs_full):
                ratio_df.loc[ratio_df['fuels'] == '08_gas', year] = (
                    ratio_df.loc[ratio_df['fuels'] == '08_gas', year - 1].values[0] - fs_full
                )

        # Populate the pipeline dataframe with projected data
        for year in proj_years:
            if tfc_df.loc[0, year - 1] == 0:
                # If previous year's total final consumption is zero, set ratio to zero
                ratio = 0
            else:
                # Calculate the ratio of current year to previous year's total final consumption
                ratio = tfc_df.loc[0, year] / tfc_df.loc[0, year - 1]

            for fuel in relevant_fuels[:-1]:  # Exclude '19_total' from relevant fuels
                pipe_df[year] = pipe_df[year].astype(float)
                # Update pipeline data for the current year using the calculated ratio and fuel-specific ratio
                pipe_df.loc[pipe_df['fuels'] == fuel, year] = (
                    pipe_df.loc[:, year - 1].sum() * ratio * ratio_df.loc[ratio_df['fuels'] == fuel, year].values[0]
                )
        #save to a folder to keep copies of the results
        pipe_df.to_csv(save_location + economy + '_pipeline_transport_' + scenario + '_' + timestamp + '.csv', index = False)
        
        #and save them to modelled_data folder too. but only after removing the latest version of the file
        for file in os.listdir(f'./data/modelled_data/{economy}/'):
            if re.search(economy + '_pipeline_transport_' + scenario, file):
                os.remove(f'./data/modelled_data/{economy}/' + file)
        pipe_df.to_csv(f'./data/modelled_data/{economy}/' + economy + '_pipeline_transport_' + scenario + '_' + timestamp + '.csv', index = False)


###############################################################################################################################################################################################################################################################################################################################################################################################################################


def trans_own_use_addon(economy, model_df_clean_wide):
    """Much like the pipeline_transport function, this function takes in demand data from the EBT system and separates historical data from projection data. It then calculates the energy consumption for other-transformation, own use and nonspecified for the projection years.  This is also only done after demand is modelled. 
    The function saves the results in a CSV file in the Outlook9th_EBT\data\modelled_data folder for automatic use by the EBT process.

    """
    relevant_fuels = ['01_coal', '02_coal_products', '06_crude_oil_and_ngl', '07_petroleum_products',
                    '08_gas', '15_solid_biomass', '16_others', '17_electricity', '18_heat']
    
    # 2022 and beyond
    proj_years = list(range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1, 1))
    proj_years_str = [str(i) for i in proj_years]

    #########################
    #make all values after the base year 0 in historical data
    historical_data = model_df_clean_wide.copy()
    historical_data[proj_years] = 0
    
    EGEDA_df = historical_data.copy()
    results_df = model_df_clean_wide.copy()
    #ADDITION BY FINN
    #########################

    # latest EGEDA data
    # EGEDA_df = pd.read_csv(latest_EGEDA)
    # EGEDA_df = EGEDA_df[EGEDA_df['subtotal_results'] == False].copy().reset_index(drop = True)
    
    EGEDA_df = EGEDA_df.drop(columns = ['subtotal_layout', 'subtotal_results']).copy().reset_index(drop = True)
    # 

    EGEDA_df[EGEDA_df['sub1sectors'].str.startswith('09')]['sub1sectors'].unique()
    EGEDA_df[EGEDA_df['sub1sectors'].str.startswith('10')]['sub2sectors'].unique()
    EGEDA_df[EGEDA_df['sub1sectors'].str.startswith('16')]['sub1sectors'].unique()

    # sub1sectors transformation categories that need to be modelled
    trans_df = pd.read_csv('./config/supply_components_data/transformation_orphans.csv', header = None)
    trans_cats = trans_df[0].values.tolist()

    # sub2sectors own-use categories that need to be modelled
    ownuse_df = pd.read_csv('./config/supply_components_data/ownuse_cats.csv', header = None)
    ownuse_cats = ownuse_df[0].values.tolist()

    # Non-specified end-use
    nonspec_df = pd.read_csv('./config/supply_components_data/other_nonspec.csv', header = None)
    nonspec_cats = nonspec_df[0].values.tolist()

    EGEDA_trans = EGEDA_df[EGEDA_df['sub1sectors'].isin(trans_cats)].copy().reset_index(drop = True)
    EGEDA_own = EGEDA_df[EGEDA_df['sub2sectors'].isin(ownuse_cats)].copy().reset_index(drop = True)
    EGEDA_nonspec = EGEDA_df[EGEDA_df['sub1sectors'].isin(nonspec_cats)].copy().reset_index(drop = True)

    # for economy in APEC_economies:
    # Save location
    save_location = './results/supply_components/02_trans_own_addon/{}/'.format(economy)

    if not os.path.isdir(save_location):
        os.makedirs(save_location)

    # results_df = pd.read_csv(latest_file)
    results_ref = results_df[results_df['scenarios'] == 'reference'].copy().reset_index(drop = True)
    results_tgt = results_df[results_df['scenarios'] == 'target'].copy().reset_index(drop = True)

    # Transformation results needed
    EGEDA_trans_ref = EGEDA_trans[(EGEDA_trans['economy'] == economy) & (EGEDA_trans['scenarios'] == 'reference')].copy().reset_index(drop = True)
    EGEDA_trans_tgt = EGEDA_trans[(EGEDA_trans['economy'] == economy) & (EGEDA_trans['scenarios'] == 'target')].copy().reset_index(drop = True)

    EGEDA_own_ref = EGEDA_own[(EGEDA_own['economy'] == economy) & (EGEDA_own['scenarios'] == 'reference')].copy().reset_index(drop = True)
    EGEDA_own_tgt = EGEDA_own[(EGEDA_own['economy'] == economy) & (EGEDA_own['scenarios'] == 'target')].copy().reset_index(drop = True)

    EGEDA_nonspec_ref = EGEDA_nonspec[(EGEDA_nonspec['economy'] == economy) & (EGEDA_nonspec['scenarios'] == 'reference')].copy().reset_index(drop = True)
    EGEDA_nonspec_tgt = EGEDA_nonspec[(EGEDA_nonspec['economy'] == economy) & (EGEDA_nonspec['scenarios'] == 'target')].copy().reset_index(drop = True)

    scenario_dict = {'ref': [results_ref, EGEDA_trans_ref, EGEDA_own_ref, EGEDA_nonspec_ref],
                    'tgt': [results_tgt, EGEDA_trans_tgt, EGEDA_own_tgt, EGEDA_nonspec_tgt]}
    
    for scenario in scenario_dict.keys():
        ############################################################
        #largely finns addition to make things play nice with having this calculate at any stage of the EBT process#
        # Data frame with results from other sectors to use to build trajectories to fill the trans and own df's
        scenario_df = scenario_dict[scenario][0].copy()
        #instead of grabbing total final consumption we will need to grab the gas consumption from the demands sectors and minus the pipeline use and nonspecified others which are clacualted in the stage after demand (so we dont want to double count them, even though the projection for nonspecified and pipeline would benefit from pipelin and non specified demand respectively - it would cause differences based on the stage of the EBT process the data is from)
        demand_sectors = ['15_transport_sector', '14_industry_sector', '16_other_sector'] + ['17_nonenergy_use']
        tfc_df = scenario_df[(scenario_df['sectors'].isin(demand_sectors)) & (scenario_df['fuels'].isin(relevant_fuels)) & (scenario_df['subtotal_results'] == False)].copy().reset_index(drop = True)
        #sum to calculate total gas consumption
        tfc_df = tfc_df.groupby(['scenarios','economy','fuels']).sum().reset_index()
        #insert values into the df matching the structure required for the code below
        dummy_df = scenario_df[(scenario_df['sectors'].isin(['12_total_final_consumption'])) &
                        (scenario_df['sub1sectors'] == 'x') &
                        (scenario_df['fuels'].isin(relevant_fuels))  &
                        (scenario_df['subfuels'] == 'x')].copy().reset_index(drop = True)
        dummy_df[proj_years] = tfc_df[proj_years]
        tfc_df = dummy_df.copy()
        #and finally minus the pipeline use from the gas consumption in case it is in the folder, so got merged in with the other dmand data
        pipeline_nonspecified_use = scenario_df[(scenario_df['sub1sectors'].isin(['15_05_pipeline_transport', '16_05_nonspecified_others'])) & (scenario_df['fuels'].isin(relevant_fuels)) & (scenario_df['subtotal_results'] == False)].copy().reset_index(drop = True)
        #sum pipeline use and nonspecified others
        pipeline_nonspecified_use = pipeline_nonspecified_use.groupby(['scenarios','economy','fuels']).sum().reset_index()
        #double check the two dataframes are 1 row each and then subtract the pipeline use from the gas consumption
        if len(tfc_df) != len(pipeline_nonspecified_use):
            raise Exception('The consumption dataframes are not the expected length')
        tfc_df[proj_years] = tfc_df[proj_years] - pipeline_nonspecified_use[proj_years]
        #largely finns addition to make things play nice with having this calculate at any stage of the EBT process#
        ############################################################
        
        tfc_df = tfc_df.fillna(0)
        
        # Dataframes to populate
        trans_df = scenario_dict[scenario][1]
        trans_df = trans_df.fillna(0)
        own_df = scenario_dict[scenario][2]
        own_df = own_df.fillna(0)
        nons_df = scenario_dict[scenario][3]
        nons_df = nons_df.fillna(0)

        # Define results dataframe
        trans_results_df = pd.DataFrame(columns = trans_df.columns)
        own_results_df = pd.DataFrame(columns = own_df.columns)
        nons_results_df = pd.DataFrame(columns = nons_df.columns)
        # Iterate over each unique fuel in the total final consumption DataFrame
        for fuel in tfc_df['fuels'].unique():
            # Extract rows for the current fuel
            fuel_agg_row = tfc_df[tfc_df['fuels'] == fuel].copy().reset_index(drop=True)

            # Extract rows for the current fuel from transformation, own use, and nonspecified use DataFrames
            trans_results_interim = trans_df[trans_df['fuels'] == fuel].copy().reset_index(drop=True)
            own_results_interim = own_df[own_df['fuels'] == fuel].copy().reset_index(drop=True)
            nons_results_interim = nons_df[nons_df['fuels'] == fuel].copy().reset_index(drop=True)

            # Iterate over each projection year to calculate ratios and populate DataFrames
            for year in proj_years:
                if fuel_agg_row.loc[0, year - 1] == 0:
                    # If previous year's total final consumption is zero, set ratio to zero
                    ratio = 0
                else:
                    # Calculate the ratio of current year's total final consumption to the previous year's
                    ratio = fuel_agg_row.loc[0, year] / fuel_agg_row.loc[0, year - 1]

                # Populate the transformation DataFrame based on the calculated ratio
                for row in trans_results_interim.index:
                    trans_results_interim[year] = trans_results_interim[year].astype(float)
                    trans_results_interim.loc[row, year] = trans_results_interim.loc[row, year - 1] * ratio

                # Populate the own use DataFrame based on the calculated ratio
                for row in own_results_interim.index:
                    own_results_interim[year] = own_results_interim[year].astype(float)
                    own_results_interim.loc[row, year] = own_results_interim.loc[row, year - 1] * ratio

                # Populate the nonspecified use DataFrame based on the calculated ratio
                for row in nons_results_interim.index:
                    nons_results_interim[year] = nons_results_interim[year].astype(float)
                    nons_results_interim.loc[row, year] = nons_results_interim.loc[row, year - 1] * ratio

            # Concatenate the interim results with the main results DataFrames
            if trans_results_df.empty:
                # If the main transformation results DataFrame is empty, initialize it with the interim results
                trans_results_df = trans_results_interim.copy()
            else:
                # Otherwise, concatenate the interim results with the main DataFrame and reset index
                trans_results_df = pd.concat([trans_results_df, trans_results_interim]).copy().reset_index(drop=True)

            if own_results_df.empty:
                # If the main own use results DataFrame is empty, initialize it with the interim results
                own_results_df = own_results_interim.copy()
            else:
                # Otherwise, concatenate the interim results with the main DataFrame and reset index
                own_results_df = pd.concat([own_results_df, own_results_interim]).copy().reset_index(drop=True)

            if nons_results_df.empty:
                # If the main nonspecified use results DataFrame is empty, initialize it with the interim results
                nons_results_df = nons_results_interim.copy()
            else:
                # Otherwise, concatenate the interim results with the main DataFrame and reset index
                nons_results_df = pd.concat([nons_results_df, nons_results_interim]).copy().reset_index(drop=True)

        #save to a folder to keep copies of the results
        trans_results_df.to_csv(save_location + economy + '_other_transformation_' + scenario + '_' + timestamp + '.csv', index = False)
        own_results_df.to_csv(save_location + economy + '_other_own_use_' + scenario + '_' + timestamp + '.csv', index = False)
        nons_results_df.to_csv(save_location + economy + '_non_specified_' + scenario + '_' + timestamp + '.csv', index = False)

        #and save them to modelled_data folder too. but only after removing the latest version of the file
        for file in os.listdir(f'./data/modelled_data/{economy}/'):
            if re.search(economy + '_other_transformation_' + scenario, file):
                os.remove(f'./data/modelled_data/{economy}/' + file)
            if re.search(economy + '_other_own_use_' + scenario, file):
                os.remove(f'./data/modelled_data/{economy}/' + file)
            if re.search(economy + '_non_specified_' + scenario, file):
                os.remove(f'./data/modelled_data/{economy}/' + file)
                
        trans_results_df.to_csv(f'./data/modelled_data/{economy}/' + economy + '_other_transformation_' + scenario + '_' + timestamp + '.csv', index = False)
        own_results_df.to_csv(f'./data/modelled_data/{economy}/' + economy + '_other_own_use_' + scenario + '_' + timestamp + '.csv', index = False)
        nons_results_df.to_csv(f'./data/modelled_data/{economy}/' + economy + '_non_specified_' + scenario + '_' + timestamp + '.csv', index = False)



###############################################################################################################################################################################################################################################################################################################################################################################################################################



def minor_supply_components(economy, model_df_clean_wide):
    """   
    this script takes in the transformation and demand data and calculates the energy used for some minor supply components (e.g. biofuel supply). This is done after all transformation is modelled, at the same time as the supply modelling is done. *this could cause minor confusion for supply or transformation modellers if they accidentally think they need to use any of the outputs from this in their modelling. Although this doesnt seem likely, it is something to be aware of.*
    """
    # wanted_wd = 'Outlook9th_EBT'
    # os.chdir(re.split(wanted_wd, os.getcwd())[0] + wanted_wd)
    
    # 2022 and beyond
    latest_hist = OUTLOOK_BASE_YEAR
    proj_years = list(range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1, 1))
    proj_years_str = [str(i) for i in proj_years]

    #########################
    #make all values after the base year 0 in historical data
    historical_data = model_df_clean_wide.copy()
    historical_data[proj_years] = 0
    
    EGEDA_df = historical_data.copy()
    results_df = model_df_clean_wide.copy()
    #ADDITION BY FINN
    #########################
    # latest EGEDA data
    # EGEDA_df = pd.read_csv(latest_EGEDA)
    
    #PLEASE NOTE THERE ARE PRETTY BIG ISSUES WITH THE ACCOUNTING OF SUBTOTALS HERE (IN PLACES WE ARE SUMMING UP SUBTOTALS AND NON-SUBTOTALS AND ITS NOT CLEAR HOW TO FIX THAT WITHOUT A FULL REWRITE). HOWEVER THE WAY IT WAS DESIGNED FROM THE BEGINNING WAS BY IGNORING THE SUBTOTAL ISSUE. SO FOR NOW WE WILL KEEP IT THAT WAY, KNOWING THAT THE FINAL VALUES ARE PRETTY INSIGNIFICANT IN THE GRAND SCHEME OF THINGS.
    
    # EGEDA_df = EGEDA_df[EGEDA_df['subtotal_results'] == False].copy().reset_index(drop = True)
    EGEDA_df = EGEDA_df.drop(columns = ['subtotal_layout', 'subtotal_results']).copy().reset_index(drop = True)
    
    # sub1sectors transformation categories that need to be modelled
    biomass_subfuel_df = pd.read_csv('./config/supply_components_data/biomass_subfuels.csv', header = None)
    others_subfuel_df = pd.read_csv('./config/supply_components_data/others_subfuels.csv', header = None)
    lignite_subfuel_df = pd.read_csv('./config/supply_components_data/lignite_subfuels.csv', header = None)

    subfuels_list = lignite_subfuel_df[0].values.tolist() + biomass_subfuel_df[0].values.tolist() + others_subfuel_df[0].values.tolist()
    
    ##########################
    #drop biofuels from the list of subfuels to model here, if its being modelled in the biofuel model!
    # subfuels_list = [fuel for fuel in subfuels_list if fuel not in ['16_05_biogasoline','16_06_biodiesel','16_07_bio_jet_kerosene','16_01_biogas','15_01_fuelwood_and_woodwaste','15_02_bagasse','15_03_charcoal','15_04_black_liquor','15_05_other_biomass']]
    subfuels_list = [fuel for fuel in subfuels_list if fuel not in ['16_02_industrial_waste', '16_03_municipal_solid_waste_renewable', '16_04_municipal_solid_waste_nonrenewable', '16_05_biogasoline','16_06_biodiesel','16_07_bio_jet_kerosene','16_01_biogas','15_01_fuelwood_and_woodwaste','15_02_bagasse','15_03_charcoal','15_04_black_liquor','15_05_other_biomass', '16_09_other_sources']]
    ##########################
    
    relevant_supply = ['01_production', '02_imports', '03_exports']
    all_supply = ['01_production', '02_imports', '03_exports', '04_international_marine_bunkers', '05_international_aviation_bunkers',
                '06_stock_changes']

    # Define columns for use
    df_columns = ['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels']
    all_years = list(range(1980, 2071, 1))
    # years_str = [str(i) for i in all_years]

    df_columns = df_columns + all_years

    # for economy in APEC_economies:
    # Save location
    save_location = './results/supply_components/03_supply_results/{}/'.format(economy)

    if not os.path.isdir(save_location):
        os.makedirs(save_location)

    # Remove unnecessary columns
    results_df = results_df.loc[:, df_columns]

    results_ref = results_df[results_df['scenarios'] == 'reference'].copy().reset_index(drop = True)
    results_tgt = results_df[results_df['scenarios'] == 'target'].copy().reset_index(drop = True)

    scenario_dict = {'ref': [results_ref],
                    'tgt': [results_tgt]}
    
    for scenario in scenario_dict.keys():
        scenario_results_df = scenario_dict[scenario][0]
        # Start with biomass and 16_others fuels that are not modelled by biorefining model
        # Creat empty dataframe to save results
        supply_df = pd.DataFrame()
        for fuel in subfuels_list + ['x']:
            if fuel != 'x': 
                # Consumption results are: TFC, transformation and own-use
                # Transformation
                trans_ref = scenario_results_df[(scenario_results_df['subfuels'] == fuel) &
                                                (scenario_results_df['sectors'] == '09_total_transformation_sector') &
                                                (scenario_results_df['sub1sectors'] == 'x')].copy().reset_index(drop = True).fillna(0)
                
                # Transformation is negative so need to be made positive to calculate total consumption
                numeric_trans = trans_ref.iloc[:, 9:] * -1
                trans_ref.iloc[:, 9:] = numeric_trans
                
                # Own-use
                own_ref = scenario_results_df[(scenario_results_df['subfuels'] == fuel) &
                                            (scenario_results_df['sectors'] == '10_losses_and_own_use') &
                                            (scenario_results_df['sub1sectors'] == 'x')].copy().reset_index(drop = True).fillna(0)
                
                # Own-use is negative so need to be made positive to calculate total consumption
                numeric_own = own_ref.iloc[:, 9:] * -1
                own_ref.iloc[:, 9:] = numeric_own
                
                # TFC
                tfc_ref = scenario_results_df[(scenario_results_df['subfuels'] == fuel) &
                                            (scenario_results_df['sectors'] == '12_total_final_consumption') &
                                            (scenario_results_df['sub1sectors'] == 'x')].copy().reset_index(drop = True).fillna(0)
                
            elif fuel == 'x':
                # Same as above but for coal products
                # Consumption results are: TFC, transformation and own-use
                # Transformation
                trans_ref = scenario_results_df[(scenario_results_df['fuels'] == '02_coal_products') &
                                                (scenario_results_df['subfuels'] == fuel) &
                                                (scenario_results_df['sectors'] == '09_total_transformation_sector') &
                                                (scenario_results_df['sub1sectors'] == 'x')].copy().reset_index(drop = True).fillna(0)
                
                # Transformation is negative so need to be made positive to calculate total consumption
                numeric_trans = trans_ref.iloc[:, 9:] * -1
                trans_ref.iloc[:, 9:] = numeric_trans
                
                # Own-use
                own_ref = scenario_results_df[(scenario_results_df['fuels'] == '02_coal_products') &
                                            (scenario_results_df['subfuels'] == fuel) &
                                            (scenario_results_df['sectors'] == '10_losses_and_own_use') &
                                            (scenario_results_df['sub1sectors'] == 'x')].copy().reset_index(drop = True).fillna(0)
                
                # Own-use is negative so need to be made positive to calculate total consumption
                numeric_own = own_ref.iloc[:, 9:] * -1
                own_ref.iloc[:, 9:] = numeric_own
                
                # TFC
                tfc_ref = scenario_results_df[(scenario_results_df['fuels'] == '02_coal_products') &
                                            (scenario_results_df['subfuels'] == fuel) &
                                            (scenario_results_df['sectors'] == '12_total_final_consumption') &
                                            (scenario_results_df['sub1sectors'] == 'x')].copy().reset_index(drop = True).fillna(0)
            
            # Combine
            all_cons = pd.concat([trans_ref, own_ref, tfc_ref]).copy().reset_index(drop = True)

            # Generate total row
            total_row = all_cons.groupby(['scenarios', 'economy', 'sub1sectors', 'sub2sectors', 
                                            'sub3sectors', 'sub4sectors', 'fuels', 'subfuels'])\
                                                .sum().assign(sectors = 'Total consumption').reset_index()

            # Add the row to the other consumption rows
            all_cons = pd.concat([all_cons, total_row]).copy().reset_index(drop = True)

            # Now grab TPES, but just for 2021 in order to get a ratio and apply it for projected results
            if fuel != 'x':             
                current_supply = scenario_results_df[(scenario_results_df['subfuels'] == fuel) &
                                                    (scenario_results_df['sectors'].isin(relevant_supply)) &
                                                    (scenario_results_df['sub1sectors'] == 'x')].copy().reset_index(drop = True)\
                                                        .fillna(0).loc[:, ['sectors', latest_hist]]
            
            elif fuel == 'x':             
                current_supply = scenario_results_df[(scenario_results_df['fuels'] == '02_coal_products') &
                                                    (scenario_results_df['subfuels'] == fuel) &
                                                    (scenario_results_df['sectors'].isin(relevant_supply)) &
                                                    (scenario_results_df['sub1sectors'] == 'x')].copy().reset_index(drop = True)\
                                                        .fillna(0).loc[:, ['sectors', latest_hist]]

            # Create new column for ratio results
                current_supply['ratio'] = np.nan

            # Calculate ratio
            for row in current_supply.index:
                if current_supply[latest_hist].sum() == 0:
                    current_supply.loc[row, 'ratio'] = 0

                else:
                    current_supply.loc[row, 'ratio'] = current_supply.loc[row, latest_hist] / current_supply[latest_hist].sum()

            # Supply results df to fill in
            if fuel != 'x':
                subfuels_supply_df = scenario_results_df[(scenario_results_df['subfuels'] == fuel) &
                                                        (scenario_results_df['sectors'].isin(all_supply)) &
                                                        (scenario_results_df['sub1sectors'] == 'x')].copy().reset_index(drop = True)
                
            elif fuel == 'x':
                subfuels_supply_df = scenario_results_df[(scenario_results_df['fuels'] == '02_coal_products') &
                                                        (scenario_results_df['subfuels'] == fuel) &
                                                        (scenario_results_df['sectors'].isin(all_supply)) &
                                                        (scenario_results_df['sub1sectors'] == 'x')].copy().reset_index(drop = True)
            
            # Calculate production, imports and exports for each projection year for every subfuel defined in the subfuels list
            for year in proj_years:
                for component in relevant_supply:
                    subfuels_supply_df.loc[subfuels_supply_df['sectors'] == component, year] = all_cons.loc[all_cons['sectors'] == 'Total consumption', year].values[0]\
                        * current_supply.loc[current_supply['sectors'] == component, 'ratio'].values[0]
                        
            #make sure that all exports are negative and imports are positive:
            if subfuels_supply_df.loc[subfuels_supply_df['sectors'] == '03_exports', proj_years].sum().sum() > 0:
                subfuels_supply_df.loc[subfuels_supply_df['sectors'] == '03_exports', proj_years] = subfuels_supply_df.loc[subfuels_supply_df['sectors'] == '03_exports', proj_years] * -1
            if subfuels_supply_df.loc[subfuels_supply_df['sectors'] == '02_imports', proj_years].sum().sum() < 0:
                subfuels_supply_df.loc[subfuels_supply_df['sectors'] == '02_imports', proj_years] = subfuels_supply_df.loc[subfuels_supply_df['sectors'] == '02_imports', proj_years] * -1
            # if subfuels_supply_df.fuels.unique()[0] == '02_coal_products':
            #     breakpoint()#check for 02_coal_products 
            supply_df = pd.concat([supply_df, subfuels_supply_df]).copy().reset_index(drop = True)
        #double check we arenbt gettin model_df_clean_wide['subfuels'] == 'x') & (model_df_clean_wide['fuels'] == '16_others') or model_df_clean_wide['subfuels'] == 'x') & (model_df_clean_wide['fuels'] == '15_solid_biomass' in the supply_df:
        if supply_df.loc[(supply_df['subfuels'] == 'x') & (supply_df['fuels'] == '16_others') | (supply_df['subfuels'] == 'x') & (supply_df['fuels'] == '15_solid_biomass')].shape[0] > 0:
            breakpoint()
            raise Exception('There are subfuels that Im pretty sure shouldnt be here')
        # ['subfuels'] == 'x') & (supply_df['fuels'] == '16_others') or supply_df['subfuels'] == 'x') & (supply_df['fuels'] == '15_solid_biomass' in supply_df:
        #save to a folder to keep copies of the results
        supply_df.to_csv(save_location + economy + '_biomass_others_supply_' + scenario + '_' + timestamp + '.csv', index = False)                    
        #and save them to modelled_data folder too. but only after removing the latest version of the file
        for file in os.listdir(f'./data/modelled_data/{economy}/'):
            if re.search(economy + '_biomass_others_supply_' + scenario, file):
                os.remove(f'./data/modelled_data/{economy}/' + file)
                
        supply_df.to_csv(f'./data/modelled_data/{economy}/' + economy + '_biomass_others_supply_' + scenario + '_' + timestamp + '.csv', index = False)

    # What do we need to do here
    # 1. Grab fuel consumption for each of these non-major (sub)fuels for all projection years
    # 2. Grab split of supply components: production, imports, exports, bunkers and stock changes to arrive at TPES
    # --> production, imports, exports have not been calculated yet
    # --> Assume stock changes are zero
    # --> Grab bunkers results that have already been generated to 2070
    # --> From the above: Production + imports - exports - bunker result = TPES
    # For most recent historical year (2020 or 2021), look at the ratio of 
    # --> Production to TPES
    # --> Imports to TPES
    # --> Exports to TPES 

    # Coal products
    # No production

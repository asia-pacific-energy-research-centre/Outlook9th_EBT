
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

def biorefining_excel_to_dict(biofuel_refining_capacity_parameters_file_path):
    
    if not os.path.exists(biofuel_refining_capacity_parameters_file_path):
        breakpoint()
        raise FileNotFoundError(f"Biorefining inputs Excel file not found: {biofuel_refining_capacity_parameters_file_path}")
    
    workbook = pd.ExcelFile(biofuel_refining_capacity_parameters_file_path)
    data = {
        'biorefinery_avg_utlisation_rates': [],
        'biofuels_refining_capacity_additions': [],
        'EBT_biofuels_list': []
    }

    # Read data from each sheet and reconstruct the YAML structure
    for sheet_name in workbook.sheet_names:
        df = workbook.parse(sheet_name)
        if '_capacity' in sheet_name:
            economy = sheet_name.replace('_capacity', '')
            capacity_additions = df.to_dict(orient='records')
            data['biofuels_refining_capacity_additions'].append({
                'economy': economy,
                'capacity_additions': capacity_additions
            })
            
        elif sheet_name == 'config':
            # Extract the EBT_biofuels_list and other config data
            data['EBT_biofuels_list'] = df['EBT_biofuels_list'].dropna().tolist()
            
        else:
            economy = sheet_name
            initial_rate = df['initial_avg_utlisation_rate'].iloc[0]
            utlisation_rate_changes = df.drop(columns=['economy', 'initial_avg_utlisation_rate']).to_dict(orient='records')
            data['biorefinery_avg_utlisation_rates'].append({
                'economy': economy,
                'initial_avg_utlisation_rate': initial_rate,
                'utlisation_rate_changes': utlisation_rate_changes
            })
    return data
            
            
def biofuels_refining(economy, model_df_clean_wide, PLOT = True, biofuel_refining_capacity_parameters_file_path = 'config/biofuel_refining_capacity_parameters.xlsx'):
    """to save workload we are just going to do biofuels refining within this model. it will take in demand from the power and demand sectors and then depending on a few paramerters, it will output production, imports and exports of liquid biofuels. 
    The parameters will be: 
    capacity additions (of each biofuel)
    
    This will also have the capcity to output the total amount of feedstock required and detailed information on the type of capacity (which will have to be detailed within the input params)
    
    It will also need to calcualte capacity based on current production. 
    
    The parameter format will be:
    
    ```yml
    biofuels_refining:
        - economy: 01_AUS
            capacity_additions:
            - year: 2030
                additional_energy_pj: 0
                specific_fuel: 'ethanol'
                EBT_fuel: '16_05_biogasoline'
    ```
    Also there will be this in the yaml for biorefinery utilisation rates:
    ```yml
    biorefinery_avg_utlisation_rates:
        - economy: 01_AUS
            initial_avg_utlisation_rate: 1
            utlisation_rate_changes:
            - year: 2030
                new_avg_utlisation_rate: 1
    ```                
    So yeah. 
    """
        
    config = biorefining_excel_to_dict(biofuel_refining_capacity_parameters_file_path)
    # #open up the yaml with the parameters we need
    # with open('config/modelling_parameters.yml', 'r') as file:
    #     config = yaml.safe_load(file)
        
    capacity_df, detailed_capcaity_df, production_df = prepare_biorefining_capacity_data(economy, model_df_clean_wide, config)
    consumption_df = prepare_biorefining_consumption_data(economy, model_df_clean_wide, config)
    final_refining_df = calculate_biofuels_refining_exports_imports(model_df_clean_wide, consumption_df, production_df)
    if PLOT:
        print('Plotting biofuels data')
        plot_biofuels_data(final_refining_df, economy)
    save_results(final_refining_df, detailed_capcaity_df, capacity_df, economy)
    ###########################################################
    ###########################################################

def prepare_biorefining_capacity_data(economy, model_df_clean_wide, config):   
    biofuel_params = config['biofuels_refining_capacity_additions']
    
    #find the economy in the biofuel_params
    for economy_params in biofuel_params:
        if economy_params['economy'] == economy:
            # economy_params = economy_params
            break
    if economy_params['economy'] != economy:
        raise Exception(f'{economy} not found in biofuels_refining parameters. Need to add it to the yaml file')
    
    #create a dataframe to store the capacity values for each biofuel type
    EBT_biofuels_list = config['EBT_biofuels_list']
    capacity_df = pd.DataFrame(columns = ['economy', 'year', 'scenarios']+EBT_biofuels_list)
    #fill with all the years:
    latest_hist = OUTLOOK_BASE_YEAR
    proj_years = list(range(OUTLOOK_BASE_YEAR+1, OUTLOOK_LAST_YEAR+1, 1))
    historical_years = list(range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1, 1))
    # Convert year columns to integers if they are strs
    
    # model_df_clean_wide.columns = [int(col) if re.search(r'\d{4}', col) else col for col in model_df_clean_wide.columns]
    model_df_clean_wide.columns = [int(col) if re.search(r'\d{4}', str(col)) else col for col in model_df_clean_wide.columns]
    
    capacity_df['year'] = [latest_hist] + proj_years
    capacity_df['economy'] = economy
    capacity_df[EBT_biofuels_list] = 0
    #make equal sized capcity df for each scnerio in model_df_clean_wide:
    capacity_df_new = pd.DataFrame(columns = ['economy', 'year', 'scenarios']+EBT_biofuels_list)
    for scenario in model_df_clean_wide['scenarios'].unique():
        capacity_df['scenarios'] = scenario
        capacity_df_new = pd.concat([capacity_df_new, capacity_df])
    capacity_df = capacity_df_new.copy().reset_index(drop = True)
    
    #now for each capacity addition, add the capacity to the capacity_df:
    detailed_capcaity_df = pd.DataFrame(columns = ['economy', 'year','scenarios', 'specific_fuel', 'EBT_fuel', 'capacity_type', 'capacity', 'avg_utilisation_rate'])
    for capacity_addition in economy_params['capacity_additions']:
        year = capacity_addition['year']
        EBT_fuel = capacity_addition['EBT_fuel']
        scenario = capacity_addition['scenario']
        if EBT_fuel not in EBT_biofuels_list:
            raise Exception(f'{EBT_fuel} not in EBT_biofuels_list. Need to add it to the yaml file')
        capacity_df.loc[(capacity_df['year'] == year) & (capacity_df['scenarios'] == scenario), EBT_fuel] += capacity_addition['additional_energy_pj']
        
        #add the detailed capacity to the detailed_capacity_df
        specific_fuel = capacity_addition['specific_fuel']
        capacity_type = 'new_capacity'
        capacity = capacity_addition['additional_energy_pj']
        if capacity != 0:#this allows for negative capacity too! (and theres checks later to make sure production is never negative)
            detailed_capcaity_df = pd.concat([detailed_capcaity_df, pd.DataFrame([{'economy': economy, 'year': year, 'specific_fuel': specific_fuel, 'EBT_fuel': EBT_fuel, 'capacity_type': capacity_type, 'capacity': capacity, 'scenarios': scenario, 'avg_utilisation_rate': np.nan}])], ignore_index=True)
    #dso the same for the utiisation rates
    biorefinery_avg_utlisation_rates = config['biorefinery_avg_utlisation_rates']
    initial_avg_utlisation_rates = {}
    for economy_params in biorefinery_avg_utlisation_rates:
        if economy_params['economy'] == economy:
            # economy_params = economy_params
            break
    if economy_params['economy'] != economy:
        raise Exception(f'{economy} not found in biorefinery_avg_utlisation_rates parameters. Need to add it to the yaml file')
    
    initial_avg_utlisation_rate = economy_params['initial_avg_utlisation_rate']
    initial_avg_utlisation_rates[economy] = initial_avg_utlisation_rate
    #set base year in detailed_capacity_dfa capacity df to the initial_avg_utlisation_rate
    detailed_capcaity_df.loc[(detailed_capcaity_df['economy'] == economy) & (detailed_capcaity_df['year'] == latest_hist), 'avg_utilisation_rate'] = initial_avg_utlisation_rate
    capacity_df.loc[(capacity_df['economy'] == economy) & (capacity_df['year'] == latest_hist), 'avg_utilisation_rate'] = initial_avg_utlisation_rate 
    
    for utlisation_rate_change in economy_params['utlisation_rate_changes']:
        scenario = utlisation_rate_change['scenario']
        year = utlisation_rate_change['year']
        new_avg_utlisation_rate = utlisation_rate_change['new_avg_utlisation_rate']
        capacity_df.loc[(capacity_df['economy'] == economy) & (capacity_df['year'] == year) & (capacity_df['scenarios'] == scenario), 'avg_utilisation_rate'] = new_avg_utlisation_rate

    #check for no duplicates
    if capacity_df.duplicated(subset = ['economy', 'year', 'scenarios']).sum() > 0:
        raise Exception('There are duplicates in the capacity_df')
    
    #calcaulte the capcity for the historical years. This will require calcaulting total production of each biofuel type and then working out the capacity required to produce that amount (convert it at a 100% utilisation rate so that 1 PJ of production requires 1 PJ of capacity)
    #grab the production data from the model_df_clean_wide
    # scenarios	economy	sectors	sub1sectors	sub2sectors	sub3sectors	sub4sectors	fuels	subfuels	subtotal_layout	subtotal_results

    biofuel_production_historical = model_df_clean_wide[(model_df_clean_wide['sectors'] == '01_production') & (model_df_clean_wide['subfuels'].isin(EBT_biofuels_list) & (model_df_clean_wide['subtotal_layout'] == False))].copy().reset_index(drop = True)
    #keep only the columns for historical years
    biofuel_production_historical = biofuel_production_historical[['scenarios','economy','subfuels']+historical_years]
    #sum
    biofuel_production_historical = biofuel_production_historical.groupby(['scenarios','economy','subfuels']).sum().reset_index()
    #melt and then pivot so the years are in one column and fuels are the columns
    biofuel_production_historical = biofuel_production_historical.melt(id_vars = ['scenarios','economy','subfuels'], value_vars = historical_years, var_name = 'year', value_name = 'production')
    biofuel_production_historical = biofuel_production_historical.pivot(index = ['scenarios','economy','year'], columns = 'subfuels', values = 'production').reset_index()
    #double check all the values in EBT_biofuels_list are in the columns
    for fuel in EBT_biofuels_list:
        if fuel not in biofuel_production_historical.columns:
            raise Exception(f'{fuel} not in biofuel_production_historical columns')
        
    #now we need to calculate the capacity for each year using the utilisation rates:
    #first fill utilisation rates for the historical years with the initial values
    biofuel_production_historical['avg_utilisation_rate'] = initial_avg_utlisation_rates[economy] 
    #now calc capacity using the production from each fuel column and the utilisation rate
     
    biofuel_production_historical[EBT_biofuels_list] = biofuel_production_historical[EBT_biofuels_list].div(biofuel_production_historical['avg_utilisation_rate'], axis=0)
    
    #concatenate the historical and future capacity dfs and recalcaulta the total capacity for each year
    #first check cols are the same
    if set(capacity_df.columns) != set(biofuel_production_historical.columns):
        breakpoint()
        raise Exception('The columns in capacity_df and biofuel_production_historical are not the same')
    
    #drop BASE_YEAR from capacity_df
    capacity_df = capacity_df[capacity_df['year'] != latest_hist].copy().reset_index(drop = True)
    #concatenate
    capacity_df = pd.concat([capacity_df, biofuel_production_historical]).copy().reset_index(drop = True)
    #now calculate the total capacity for each year in the future years, using the newly calcualted base year capacity (from the historical data) and all subsequent capacity additions
    #asnd also set the utilisation rates for the future years 
    for scenario in model_df_clean_wide['scenarios'].unique():
        for year in proj_years:
            for EBT_fuel in EBT_biofuels_list:
                capacity_df.loc[(capacity_df['year'] == year) & (capacity_df['scenarios'] == scenario), EBT_fuel] += capacity_df.loc[(capacity_df['year'] == year-1) & (capacity_df['scenarios'] == scenario), EBT_fuel].values[0]
        
            #if the current years utilisation rate is not set, set it to the previous years
            if capacity_df.loc[(capacity_df['year'] == year) & (capacity_df['scenarios'] == scenario), 'avg_utilisation_rate'].isnull().values.any():
                capacity_df.loc[(capacity_df['year'] == year) & (capacity_df['scenarios'] == scenario), 'avg_utilisation_rate'] = capacity_df.loc[(capacity_df['year'] == year-1) & (capacity_df['scenarios'] == scenario), 'avg_utilisation_rate'].values[0]
                
            #set the utilisation rate in detailed_capacity_df, if this year, scenario combo is in the detailed_capacity_df
            if detailed_capcaity_df.loc[(detailed_capcaity_df['year'] == year) & (detailed_capcaity_df['scenarios'] == scenario), 'avg_utilisation_rate'].isnull().values.any():
                detailed_capcaity_df.loc[(detailed_capcaity_df['year'] == year) & (detailed_capcaity_df['scenarios'] == scenario), 'avg_utilisation_rate'] = capacity_df.loc[(capacity_df['year'] == year) & (capacity_df['scenarios'] == scenario), 'avg_utilisation_rate'].values[0]
                
    #now we have capcity and utilisation rates for each year we can calculate the production, imports and exports
    production_df = capacity_df.copy()
    for fuel in EBT_biofuels_list:
        production_df[fuel] = production_df[fuel] * production_df['avg_utilisation_rate']
    #drop avg_utilisation_rate and then melt so fuel is a column
    production_df = production_df.drop(columns = 'avg_utilisation_rate')
    production_df = production_df.melt(id_vars = ['scenarios', 'economy','year'], value_vars = EBT_biofuels_list, var_name = 'subfuels', value_name = 'production')
    
    #set any prodcuton less than 0 to 0
    production_df['production'] = np.where(production_df['production'] < 0, 0, production_df['production'])
        
    return capacity_df, detailed_capcaity_df, production_df


def prepare_biorefining_consumption_data(economy, model_df_clean_wide, config):
    EBT_biofuels_list = config['EBT_biofuels_list']
    proj_years = list(range(OUTLOOK_BASE_YEAR, OUTLOOK_LAST_YEAR+1, 1))
    historical_years = list(range(EBT_EARLIEST_YEAR, OUTLOOK_BASE_YEAR+1, 1))
    all_years = historical_years + proj_years
    all_years = [year for year in all_years if year in model_df_clean_wide.columns]
    #now we need to calculate imports and exports. This will be defined by the amount of consumption of each fuel in the demand and power sectors:
    #grab the consumption data from the model_df_clean_wide for future years
    sectors= ['04_international_marine_bunkers', '05_international_aviation_bunkers', '09_total_transformation_sector', '10_losses_and_own_use', '14_industry_sector', '15_transport_sector', '16_other_sector', '17_nonenergy_use']
    removed_column_value_dict = {'sub1sectors':'09_10_biofuels_processing'}
    consumption_df = model_df_clean_wide[(model_df_clean_wide['sectors'].isin(sectors)) & (model_df_clean_wide['subfuels'].isin(EBT_biofuels_list)) & (model_df_clean_wide['subtotal_results'] == False)].copy().reset_index(drop = True)
    print('Consumption data for biofuels refining is based on the sectors {}'.format(sectors))
    for column, value in removed_column_value_dict.items():
        consumption_df = consumption_df[consumption_df[column] != value].copy().reset_index(drop = True)
        print('Removing {} from {} in the biofuels consumption calculation for biofuels refining'.format(value, column))
    #keep only the columns for future years
    consumption_df = consumption_df[['scenarios','economy','subfuels']+all_years]
    #make everything absolute (bunkers and transformation are negative)
    all_years = [year for year in all_years if year in consumption_df.columns]
    for year in all_years:
        consumption_df[year] = consumption_df[year].abs()
    #sum
    consumption_df = consumption_df.groupby(['scenarios','economy','subfuels']).sum().reset_index()
    #melt and then pivot so the years are in one column and fuels are the columns. this makes it compatible with the production data so we can calculate imports and exports
    consumption_df = consumption_df.melt(id_vars = ['scenarios','economy','subfuels'], value_vars =all_years, var_name = 'year', value_name = 'consumption')
    
    return consumption_df


def calculate_biofuels_refining_exports_imports(model_df_clean_wide, consumption_df, production_df):
    #merge consumption and production data
    final_df = consumption_df.merge(production_df, on = ['scenarios', 'economy', 'subfuels', 'year'], how = 'outer')
    #find any nas in either production or consumption and let user know, we should at least have 0's for these
    if final_df['consumption'].isnull().sum() > 0:
        print('There are {} NaNs in the consumption data'.format(final_df['consumption'].isnull().sum()))
        breakpoint()
        raise Exception('There are NaNs in the consumption data')
    if final_df['production'].isnull().sum() > 0:
        print('There are {} NaNs in the production data'.format(final_df['production'].isnull().sum()))
        breakpoint()
        raise Exception('There are NaNs in the production data')
    #calculate imports and exports. this is jsut the difference between consumption and production, and if negative, it is an export if positive it is an import
    final_df['02_imports'] = np.where(final_df['consumption'] - final_df['production'] > 0, (final_df['consumption'] - final_df['production']), 0)
    final_df['03_exports'] = np.where(final_df['consumption'] - final_df['production'] < 0, (final_df['consumption'] - final_df['production']), 0)
    
    #rename 01_production:
    final_df.rename(columns = {'production':'01_production'}, inplace = True)
    
    # #we now have no need for the consumption column
    # final_df.drop(columns = ['consumption'], inplace = True)
    
    #not sure if we need to fill in biofuels_processing in transformation so leave it for now.
    
    #melt the sectors (01_production, 02_imports, 03_exports) so they are in one column and then pivot the years so they are in columns:
    final_df = final_df.melt(id_vars = ['scenarios', 'economy', 'subfuels', 'year'], value_vars = ['consumption', '01_production', '02_imports', '03_exports'], var_name = 'sectors', value_name = 'energy_pj')
    #pivot years
    final_df = final_df.pivot_table(index = ['scenarios', 'economy', 'subfuels', 'sectors'], columns = 'year', values = 'energy_pj').reset_index()
    
    #fill in missing cols from model_df_clean_wide:
    model_df_clean_wide_index_cols = model_df_clean_wide[['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels']].drop_duplicates().copy().reset_index(drop = True)
    final_df = model_df_clean_wide_index_cols.merge(final_df, on = ['scenarios', 'economy', 'sectors', 'subfuels'], how = 'right')
    #check for any missing values, this is where we may have created a category that wasnt in the original model_df_clean_wide
    #exceopt ignore where sectors==consumption as this will be missing
    if final_df[final_df['sectors'] != 'consumption'].isnull().sum().sum() > 0:
        print('There are {} NaNs in the final_df'.format(final_df[final_df['sectors'] != 'consumption'].isnull().sum().sum()))
        breakpoint()
        raise Exception('There are NaNs in the final_df')
        
    return final_df

def plot_biofuels_data(final_refining_df, economy):
    """
    Create and display Plotly Express charts based on the final_refining_df data.
    """
    import plotly.express as px
    # Filter data for the specific economy and melt for visualization.
    df = final_refining_df[final_refining_df['economy'] == economy].copy()
    df_melted = df.melt(id_vars=['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels'], var_name='year', value_name='energy_pj')

    # Line chart to show the production, imports, and exports over the years by fuel type.
    fig_production = px.line(
        df_melted,
        x='year',
        y='energy_pj',
        color='sectors',
        line_dash='sectors',
        facet_row='scenarios',
        facet_col='subfuels',
        title=f'Biofuels Production, Imports, and Exports Over Time for {economy}',
        labels={'year': 'Year', 'energy_pj': 'Energy (PJ)', 'sectors': 'Supply Components', 'subfuels': 'Fuel Type'}
    )
    #write html
    fig_production.write_html(f'./plotting_output/biofuels_refining/{economy}_biofuels_refining_line.html')
    
    # Bar chart to compare the production, imports, and exports by sector and year
    fig_bar = px.bar(
        df_melted.loc[df_melted['sectors'] != 'consumption'],
        x='year',
        y='energy_pj',
        color='sectors',
        barmode='relative',
        facet_col='scenarios',
        facet_row='subfuels',
        title=f'Biofuels Production, Imports, and Exports Comparison for {economy}',
        labels={'year': 'Year', 'energy_pj': 'Energy (PJ)', 'sectors': 'Supply Components'}
    )
    #write html
    fig_bar.write_html(f'./plotting_output/biofuels_refining/{economy}_biofuels_refining_stacked_bars.html')
    
    #and now create one which has net imports as a line, with prodcuton and conshunption in the chart too
    df_net_imports = df_melted.copy()
    #pivot the sectors so they are in columns
    df_net_imports = df_net_imports[['scenarios', 'economy', 'subfuels', 'year', 'sectors', 'energy_pj']].pivot(index = ['scenarios', 'economy', 'subfuels', 'year'], columns = 'sectors', values = 'energy_pj').reset_index()
    
    df_net_imports['net_imports'] = df_net_imports['02_imports'] + df_net_imports['03_exports']
    df_net_imports.drop(columns = ['02_imports', '03_exports'], inplace = True)
    df_net_imports = df_net_imports.melt(id_vars=['scenarios', 'economy', 'subfuels', 'year'], var_name='sectors', value_name='energy_pj')
    fig_net_imports = px.line(
        df_net_imports,
        x='year',
        y='energy_pj',
        color='sectors',
        line_dash='sectors',
        facet_row='scenarios',
        facet_col='subfuels',
        title=f'Biofuels Production, Consumption and Net Imports Over Time for {economy} - negative imports are exports',
        labels={'year': 'Year', 'energy_pj': 'Energy (PJ)', 'sectors': 'Supply Components', 'subfuels': 'Fuel Type'}
    )
    #write html
    fig_net_imports.write_html(f'./plotting_output/biofuels_refining/{economy}_biofuels_refining_net_imports_line.html')
    

def save_results(final_refining_df, detailed_capcaity_df, capacity_df, economy):
    # Save location
    save_location = './results/supply_components/04_biofuels_refining/{}/'.format(economy)
    #drop 'consumption' from final_refining_df's sectors column
    final_refining_df = final_refining_df.loc[final_refining_df['sectors'] != 'consumption']
    
    if not os.path.isdir(save_location):
        os.makedirs(save_location)
        
    for scenario in final_refining_df['scenarios'].unique():
        supply_df = final_refining_df[final_refining_df['scenarios'] == scenario].copy().reset_index(drop = True)
        #save to a folder to keep copies of the results
        supply_df.to_csv(save_location + economy + '_biofuels_refining_' + scenario + '_' + timestamp + '.csv', index = False)     
        #and save them to modelled_data folder too. but only after removing the latest version of the file
        for file in os.listdir(f'./data/modelled_data/{economy}/'):
            if re.search(economy + '_biofuels_refining_' + scenario, file):
                os.remove(f'./data/modelled_data/{economy}/' + file)
                
        supply_df.to_csv(f'./data/modelled_data/{economy}/' + economy + '_biofuels_refining_' + scenario + '_' + timestamp + '.csv', index = False)
    #and save detailed_capcaity_df to the results folder but only after removing the latest version of the file
    for file in os.listdir('./results/supply_components/04_biofuels_refining/{}/'.format(economy)):
        if re.search(economy + '_biofuels_refining_capacity_additions_detailed', file):
            if os.path.exists('./results/supply_components/04_biofuels_refining/{}/'.format(economy) + file):
                os.remove('./results/supply_components/04_biofuels_refining/{}/'.format(economy) + file)
        if re.search(economy + '_biofuels_refining_capacity', file):
            if os.path.exists('./results/supply_components/04_biofuels_refining/{}/'.format(economy) + file):
                os.remove('./results/supply_components/04_biofuels_refining/{}/'.format(economy) + file)
            
    detailed_capcaity_df.to_csv(save_location + economy + '_biofuels_refining_capacity_additions_detailed_' + timestamp + '.csv', index = False)
    capacity_df.to_csv(save_location + economy + '_biofuels_refining_capacity_' + timestamp + '.csv', index = False)
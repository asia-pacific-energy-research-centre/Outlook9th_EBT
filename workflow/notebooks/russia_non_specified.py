#value to estimate:
# 'nonspecified_transformation': {
#     'sub1sectors': ['09_12_nonspecified_transformation']
# },
# 'nonspecified_own_uses': {
#     'sub2sectors': ['10_01_17_nonspecified_own_uses']
# },
# 'nonspecified_others': {
#     'sectors': ['16_05_nonspecified_others']
# },
# # 'pipeline_transport': {
# #     'sub1sectors': ['15_05_pipeline_transport']
# # },
# 'nonspecified_transport': {
#     'sectors': ['15_06_nonspecified_transport']
# }
# 
# proxy
# 'nonspecified_transformation': {'sectors': ['09_total_transformation_sector']},
# 'nonspecified_own_uses': {'sub1sectors': ['10_01_own_use']},
# 'nonspecified_others': {'sectors': ['16_other_sector']},
# 'nonspecified_transport': {'sectors': ['15_transport_sector']}

#take in projected data and calcualte the ratio  in OUTLOOK_BASE_YEAR between the proxy and value we want to esitmate, then for all projected years times the proxy by the ratio to get the new data.

#note the sturckture of the dataframe is [scenarios, economy, sectors, sub1sectors, sub2sectors, sub3sectors, sub4sectors, fuels, subfuels, subtotal_layout, subtotal_results, 2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030, 2031, 2032, 2033, 2034, 2035, 2036, 2037, 2038...
# C:\Users\finbar.maunsell\github\Outlook9th_EBT\results\16_RUS\merged\merged_file_energy_16_RUS_20250403.csv
#%%
import pandas as pd
import os
data = pd.read_csv('../../results/16_RUS/merged/merged_file_energy_16_RUS_20250403.csv')
OUTLOOK_BASE_YEAR = 2022
sector_to_proxy = {
    '09_12_nonspecified_transformation': ['09_total_transformation_sector'],
    '10_01_own_use': ['10_01_own_use'],
    '16_05_nonspecified_others': ['16_other_sector'],
    '15_06_nonspecified_transport': ['15_transport_sector'],
}#note the keys are all in sub1sectors

for sector, proxy in sector_to_proxy.items():
    # Get the proxy data
    #loop through the proxy sectors and get the data for each by looping through the sector columns to extract the data:
    for proxy_ in proxy:
        proxy_data= pd.DataFrame()
        for sector_col in ['sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors']:
            proxy_data = pd.concat([proxy_data, data[data[sector_col]==proxy_]], ignore_index=True)
        proxy_data['new_sector'] = sector  
    
    base_year_sector_data = proxy_data[proxy_data['sub1sectors'] == sector][str(OUTLOOK_BASE_YEAR)].sum()
    base_year_proxy = proxy_data[str(OUTLOOK_BASE_YEAR)].sum()
    breakpoint()
    
    
            
#%%

# # Calculate the ratio for the base year (2022)
# base_year = 2022
# for year in range(2023, 2039):
#     # Calculate the ratio for each year
#     ratio = proxy_data[str(year)].sum() / proxy_data[str(base_year)].sum()
    
#     # Apply the ratio to the proxy data for the projected years
#     data.loc[data['sub1sectors'] == sector, str(year)] = proxy_data[str(year)] * ratio
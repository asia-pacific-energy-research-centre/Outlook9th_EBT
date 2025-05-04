#%%
import pandas as pd
import matplotlib.pyplot as plt
import os

# Load the data if available; otherwise, simulate sample data
old_df = pd.read_excel("../../data/raw_data/russia_pipelines_projection.xlsx", sheet_name="old")
new_df = pd.read_excel("../../data/raw_data/russia_pipelines_projection.xlsx", sheet_name="new")
    
# Merge the dataframes on 'scenarios' and 'fuels'
# (Assumes that in your real data these columns match exactly.)
merged_df = pd.merge(old_df, new_df, on=["scenarios", "fuels"], suffixes=("_old", "_new"))

# Compute the adjustment ratio using the 2022 values.
# For each merged row, the ratio is new base year 2022 divided by the old projection 2022.
#rename 2021 columns to 2021_new1 and 2022 columns to 2022_new2 for clarity
merged_df.rename(columns={ "2021_new": "2021_new1", "2022_new": "2022_new2"}, inplace=True)
merged_df["ratio"] = merged_df["2021_new1"] / merged_df["2021_old"]
merged_df['ratio_2022'] = merged_df['2022_new2'] / merged_df['2022_old']
merged_df['2021_adj'] = merged_df['2021_old'] * merged_df["ratio"]
merged_df['2022_adj'] = merged_df['2022_old'] * merged_df["ratio_2022"]
# Define the range of years present in the old pipelines projection.
years = list(range(2022, 2071))

# For each year column from 2021 to 2070, multiply the old projection value by the ratio.
# The merged dataframe now contains the old columns with a suffix '_old'.
for year in years:
    if year ==2022:
        col_old = '2022_old'
    else:
        col_old = year
    if year != 2022:
        col_adj = str(year) + "_new2"  # new column for the adjusted value
        merged_df[col_adj] = merged_df[col_old] * merged_df["ratio_2022"]
    col_adj = str(year) + "_new1"  # new column for the adjusted value
    merged_df[col_adj] = merged_df[col_old] * merged_df["ratio"]
    #make te projection for _old values clear by changing anything that is kist the year itself to _old
    merged_df.rename(columns={col_old: str(col_old) + "_old"}, inplace=True)

#drop ratio col
merged_df.drop(columns=["ratio", 'ratio_2022'], inplace=True)
# # For plotting, we want the df melted so that each row is a year and each column is a scenario.
# # This will make it easier to plot the data.
melted_df = merged_df.melt(id_vars=["scenarios", "fuels"], var_name="Year", value_name="Value")
#then where there is _old or _adj we wnat to label that in a new column
melted_df['Type'] = melted_df['Year'].str.extract(r'_(old|adj|new1|new2)')[0]
melted_df['Year'] = melted_df['Year'].str.replace(r'_(old|adj|new1|new2)', '', regex=True)
# Convert the Year column to int
melted_df['Year'] = melted_df['Year'].astype(int)
# Convert the Value column to numeric
melted_df['Value'] = pd.to_numeric(melted_df['Value'], errors='coerce')
#%%

#plot using plotly express
import plotly.express as px

fig = px.line(melted_df, x='Year', y='Value', facet_col='scenarios', line_dash='Type', color ='fuels',
              title='Comparison of Old and New Projections',
              labels={'Value': 'Projected Value', 'Year': 'Year', 'scenarios': 'Scenarios'})
#save to plotting_ouptut
fig.write_html('../../plotting_output/russia_pipelines_proj.html')
# %%

#ok we'll use new1 for the projection and assume new2 2022 doesnt happen
new1 = melted_df[melted_df['Type'] == 'new1']

#pivot
new1_pivot = new1.pivot(index= ['scenarios', 'fuels'], columns='Year', values='Value').reset_index()

#fix columns and stuff:
# scenarios	economy	sectors	sub1sectors	sub2sectors	sub3sectors	sub4sectors	fuels	subfuels
new1_pivot['economy'] = '16_RUS'
# sectors	sub1sectors
# 15_transport_sector	15_05_pipeline_transport
# sub2sectors	sub3sectors	sub4sectors
# x	x	x

new1_pivot['sectors'] = '15_transport_sector'
new1_pivot['sub1sectors'] = '15_05_pipeline_transport'
new1_pivot['sub2sectors'] = 'x'
new1_pivot['sub3sectors'] = 'x'
new1_pivot['sub4sectors'] = 'x'

#where fuels is 17_electricity then subfuels is x, else set suvfuels to fuels. then set the fuels column based on the first number of subufuels:
new1_pivot['subfuels'] = new1_pivot['fuels']
new1_pivot.loc[new1_pivot['fuels'] == '17_electricity', 'subfuels'] = 'x'
new1_pivot.loc[new1_pivot['fuels'] != '17_electricity', 'fuels'] = new1_pivot['fuels'].str.split('_').str[0]
#%%
#map the fuels to its official name:
#07_petroleum_products 08_gas 
#if any nas raise them
MAP = {
    '07': '07_petroleum_products',
    '08': '08_gas',
    '17_electricity': '17_electricity'}
new1_pivot['fuels'] = new1_pivot['fuels'].map(MAP)
#check for any nans in the fuels column
if new1_pivot['fuels'].isna().any():
    raise ValueError('There are NaN values in the fuels column after mapping.')

#make these cols all at teh start and years cols at the end:
year_cols = [col for col in new1_pivot.columns if str(col).isdigit()]
# Reorder the columns to have the year columns at the end
col_order = [col for col in new1_pivot.columns if col not in year_cols] + year_cols
new1_pivot = new1_pivot[col_order]
#%%
new1_pivot.to_csv('../../results/modelled_within_repo/russia_pipelines_proj.csv', index=False)
# 
# Save it to the russia modelled data folder to include it in the projections
# # %%

# %%

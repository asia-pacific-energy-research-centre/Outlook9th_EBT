#%%
# Restructure ESTO EBT for Sankey Diagram
import pandas as pd
import utility_functions as utils

utils.set_working_directory()

# Load esto ebt data
esto_ebt = pd.read_excel('data/00APEC_2024.xlsx', sheet_name='09ROK', header=0)

# Rename unnamed columns to 'source' and 'target'
esto_ebt.rename(columns={'Unnamed: 0': 'source', 'Unnamed: 1': 'target'}, inplace=True)

# Convert all column names to strings to avoid integer column names
esto_ebt.columns = esto_ebt.columns.map(str)

# Ensure 2022 is treated as a column
if '2022' in esto_ebt.columns:
    # Select the required columns
    esto_ebt_filtered = esto_ebt[['source', 'target', '2022']].copy()
    # Rename '2022' to 'value' for clarity
    esto_ebt_filtered.rename(columns={'2022': 'value'}, inplace=True)
else:
    raise ValueError("The year 2022 is not found in the worksheet columns.")

# Load mapping file
mapping_workbook = 'config/sankey_diagram_mapping.xlsx'

# Load mappings from different sheets
source_mapping = pd.read_excel(mapping_workbook, sheet_name='source_mapping').set_index('original_source')['mapped_source'].to_dict()
target_mapping = pd.read_excel(mapping_workbook, sheet_name='target_mapping').set_index('original_target')['mapped_target'].to_dict()
type_mapping = pd.read_excel(mapping_workbook, sheet_name='type_mapping').set_index('original_type')['mapped_type'].to_dict()

# Load processes from the 'processes' sheet and make first column the index
processes = pd.read_excel(mapping_workbook, sheet_name='processes', header=0, index_col=0)
# Save the processes worksheet to a new csv file
processes.to_csv('data/sankey_diagram_processes.csv')

# Apply the mappings to the source and target columns
esto_ebt_filtered['source'] = esto_ebt_filtered['source'].map(source_mapping)
esto_ebt_filtered['target'] = esto_ebt_filtered['target'].map(target_mapping)

# Create a new column 'type' based on the source column
esto_ebt_filtered['type'] = esto_ebt_filtered['source'].map(type_mapping)

# Create a new column 'sector' based on the target column
# esto_ebt_filtered['sector'] = esto_ebt_filtered['target'].map(sector_mapping)

# Reorder and clean up the columns
esto_ebt_filtered = esto_ebt_filtered[['source', 'target', 'type', 'value']]

# Filter out rows where either 'source' or 'target' is empty
esto_ebt_filtered.dropna(subset=['source', 'target'], inplace=True)

# Combine rows with the same source, target, and type by summing the values and also preserving the order of the rows
esto_ebt_filtered = esto_ebt_filtered.groupby(['source', 'target', 'type'], as_index=False, sort=False).sum()

# Remove rows with zero value
esto_ebt_filtered = esto_ebt_filtered[esto_ebt_filtered['value'] != 0]

# Change all to absolute value
esto_ebt_filtered['value'] = esto_ebt_filtered['value'].abs()

# Remove rows where 'source' is 'Electricity generation' or 'Heat generation' and 'target' is 'Electricity generation'
esto_ebt_filtered = esto_ebt_filtered[~((esto_ebt_filtered['source'] == 'Electricity generation') & (esto_ebt_filtered['target'] == 'Electricity generation'))]
esto_ebt_filtered = esto_ebt_filtered[~((esto_ebt_filtered['source'] == 'Heat generation') & (esto_ebt_filtered['target'] == 'Electricity generation'))]

# Change 'Heat generation' in 'source' to 'Electricity generation'
# esto_ebt_filtered['source'] = esto_ebt_filtered['source'].replace('Heat generation', 'Electricity generation')

# Copy rows where 'target' is 'Services' and 'Residential' and sum both values for each 'source' and 'type' and add the result as new rows with 'target' as 'Buildings'and keep 'source' and 'type' the same
services_residential_sum = esto_ebt_filtered[esto_ebt_filtered['target'].isin(['Services', 'Residential'])].groupby(['source', 'type'], as_index=False, sort=False)['value'].sum()
services_residential_sum['target'] = 'Buildings'
esto_ebt_filtered = pd.concat([esto_ebt_filtered, services_residential_sum], ignore_index=True)

sector_mapping = pd.read_excel(mapping_workbook, sheet_name='sector_mapping').set_index('original_sector')['mapped_sector'].to_dict()
# Change 'source' in esto_ebt_filtered to the sectors in 'mapped_sector' in sector_mapping if the sector in 'original_sector' matches the 'target' in esto_ebt_filtered and keep 'target' and 'type' the same
esto_ebt_filtered['source'] = esto_ebt_filtered.apply(lambda x: sector_mapping[x['target']] if x['target'] in sector_mapping else x['source'], axis=1)

# Calculate the sum of values for all 'Electricity generation' under 'target' and substract the sum of values for 'Electricity generation' under 'source' and add the result as a new row with 'source' as 'Electricity generation' and 'target' as 'Losses' and 'type' as 'Transformation losses'
electricity_generation_target_sum = esto_ebt_filtered[esto_ebt_filtered['target'] == 'Electricity generation']['value'].sum()
electricity_generation_source_sum = esto_ebt_filtered[esto_ebt_filtered['source'] == 'Electricity generation']['value'].sum()
electricity_generation_losses = electricity_generation_target_sum - electricity_generation_source_sum
transformation_losses_row = pd.DataFrame([{'source': 'Electricity generation', 'target': 'Losses', 'type': 'Transformation losses', 'value': electricity_generation_losses}])
esto_ebt_filtered = pd.concat([esto_ebt_filtered, transformation_losses_row], ignore_index=True)

# Temp fix: Change 'Other transformation' in 'target' to 'Industry'
# esto_ebt_filtered['target'] = esto_ebt_filtered['target'].replace('Other transformation', 'Industry')

# Temp fix: Change 'Losses' in 'target' to 'Losses1' only if 'source' is not 'Electricity generation'
# esto_ebt_filtered['target'] = esto_ebt_filtered.apply(lambda x: 'Losses1' if (x['target'] == 'Losses') & (x['source'] != 'Electricity generation') else x['target'], axis=1)

# Save the transformed DataFrame to a new CSV file
esto_ebt_filtered.to_csv('data/sankey_diagram_data.csv', index=False)

print("Transformation complete. Transposed data saved to: data/sankey_diagram_data.csv")
#%%
# Create Sankey Diagram
import pandas as pd
from floweaver import *
import utility_functions as utils

utils.set_working_directory()

flows = pd.read_csv('data/sankey_diagram_data.csv')
processes = pd.read_csv('data/sankey_diagram_processes.csv', index_col=0)

dataset = Dataset(flows, dim_process=processes)

fuels = ['Coal', 'Oil', 'Gas', 'Nuclear', 'Renewables']
# uses = ['Own use', 'Industry', 'Transport', 'Buildings', 'Agriculture', 'Non-specified', 'Non-energy', 'Losses']
# uses = ['Own use', 'Iron and steel', 'Chemical', 'Machinery', 'Other industry', 'Road', 'Other transport', 'Services', 'Residential', 'Agriculture', 'Non-specified', 'Non-energy', 'Losses']
sectors = ['Own use', 'Industry', 'Transport', 'Buildings', 'Agriculture', 'Non-specified', 'Non-energy', 'Losses']
subsectors = ['Iron and steel', 'Chemical', 'Machinery', 'Other industry', 'Road', 'Other transport', 'Services', 'Residential']

nodes = {
    'supply': ProcessGroup('type == "fuel"', Partition.Simple('process', fuels), title='Primary energy supply'),
    'electricity': ProcessGroup(['Electricity generation'], title='Electricity generation'),
    'heat': ProcessGroup(['Heat generation'], title='Heat generation'),
    'sectors': ProcessGroup('type == "sector"', Partition.Simple('process', sectors)),
    'subsectors': ProcessGroup('type == "subsector"', partition=Partition.Simple('process', subsectors)),
    # 'sector': Waypoint(Partition.Simple('target', [('Industry', ['Iron and steel', 'Chemical', 'Machinery', 'Other industry']), ('Transport', ['Road', 'Other transport']), ('Buildings', ['Residential', 'Services']), ('Agriculture', ['Agriculture']), ('Non-specified', ['Non-specified']), ('Non-energy', ['Non-energy']), ('Losses', ['Losses'])])),
    'direct_use': Waypoint(Partition.Simple('source', [
        # This is a hack to hide the labels of the partition, there should be a better way...
        (' '*i, [k]) for i, k in enumerate(fuels)])),
}

ordering = [
    [['supply']],
    [['direct_use', 'electricity', 'heat']],
    [['sectors']],
    [['subsectors']],
]

bundles = [
    Bundle('supply', 'sectors', waypoints=['direct_use']),
    Bundle('supply', 'electricity'),
    Bundle('supply', 'heat'),
    Bundle('electricity', 'sectors'),
    Bundle('heat', 'sectors'),
    Bundle('sectors', 'subsectors'),
]

palette = {
    'Coal': 'black',
    'Oil': 'darkorange',
    'Gas': 'steelblue',
    'Nuclear': 'red',
    'Renewables': 'lightgreen',
    'Electricity': 'skyblue',
    'Heat': 'dimgrey',
    'Transformation losses': 'lightgrey',
}

sdd = SankeyDefinition(nodes, bundles, ordering, flow_partition=dataset.partition('type'))

weave(sdd, dataset, palette=palette).to_widget()
#%%
# Create Legend for Sankey Diagram
import ipywidgets as widgets
from IPython.display import display

legend_items = []

# Create legend items
for label, color in palette.items():
    legend_items.append(
        widgets.HBox(
            [
                widgets.HTML(f"<div style='width:20px;height:20px;background-color:{color};border:1px solid #000'></div>"),
                widgets.Label(label, style={'font_size': '12px'}),
            ],
            layout=widgets.Layout(display='flex', align_items='center', gap='5px')
        )
    )

# Arrange legend items in a horizontal layout
legend = widgets.HBox(
    legend_items,
    layout=widgets.Layout(
        display='flex',
        flex_wrap='wrap',       # Allow wrapping if it gets too long
        justify_content='center',  # Center-align items
        gap='15px',             # Space between items
        padding='10px',         # Add padding around the legend
        border='1px solid black',  # Optional border for clarity
    )
)

display(legend)
#%%
# Save Sankey Diagram to SVG
size_for_svg = dict(width=1400, height=900, margins=dict(left=100, right=100))

sankey_diagram = weave(sdd, dataset, palette=palette).to_widget(**size_for_svg)
sankey_diagram.auto_save_svg('data/sankey_diagram.svg')
#%%
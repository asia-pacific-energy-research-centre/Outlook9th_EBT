# Intro

This python script explanation is based on ```EGEDA_2020_create_csv_2_fuel_sector_layout_revised_cleanversion.ipynb```.  
In the ipynb file, there are also markdown to explain the idea.  
  
Assume that we come up with a new layout for the energy balance table, we would like the EGEDA historical data presented in your self-defined layout.  
  
This python script is aim to create a new layout with EGEDA historical data and blanks for projected data.
(You should have new layout files before you start this script. See excel files in ./data/self_defined_layout.)  
  
The differences between EGEDA and self-defined layouts include 
- Different level of fuel (or sector) in single column → Different levels in multiple columns.
- Unconstrained item numbers (1, 2, ..., 10, 11...) → Two-digit itme numbers (01, 02, ..., 10, 11...)
- New, aggregated, and disaggregate fuels.
	- fuel aggregation
		- 01_02_thermal_coal: It includes '01_02_other_bituminous_coal', '01_03_subbituminous_coal', '01_04_anthracite'. ('03_peat' and '04_peat_products' do not belong to thermal coal.)
		- 04_03_other_hydrocarbons: It includes '06_03_refinery_feedstocks', '06_04_additives_oxygenates', '06_05_other_hydrocarbons'.
		- 05_04_jet_fuel: It includes '07_04_gasoline_type_jet_fuel', '07_05_kerosene_type_jet_fuel'.
		- 05_11_other_petroleum_products: It includes '07_12_white_spirit_sbp', '07_13_lubricants', '07_14_bitumen', '07_15_paraffin_waxes', '07_16_petroleum_coke', '07_17_other_products'.
	- fuel disaggregation	
		- 10_02_other_solar: '10_solar' comprises '10_01_of_which_photovoltaics' and '10_02_other_solar'.
	- New fuels: These new items are not in the current EGEDA EBT (as of March 2023) but added in the new EBT layout for modellers.
		- 14_10_hydrogen
		- 14_11_ammonia
	- sector aggregation
		- Sum the main_activiti_producer and autoproducers for '09_01_01_electricity_plants', '09_02_01_electricity_plants', '09_01_02_chp_plants', '09_02_02_chp_plants', '09_01_03_heat_plants', '09_02_03_heat_plants', '18_01_map_electricity_plants', '18_03_ap_electricity_plants', '18_02_map_chp_plants', '18_04_ap_chp_plants', '19_01_map_chp_plants', '19_03_ap_chp_plants', '19_02_map_heat_plants', and '19_04_ap_heat_plants'. We also rearrange the levels here.
		- Rearrange the levels for '16_01_commercial_and_public_services', '16_02_residential', '16_03_agriculture', '16_04_fishing'

The idea is to connect (replace) the EGEDA item name with your self-defined item name (1-1 relationship).
With that,we can merge the EGEDA historical data and the reference name on the "EGEDA item name."
After that, we only have to deal with the new, aggregated, and disaggregated item and drop the unnecessary columns.  

The final result is the historical data presented in your self-define layout (probably some blanks in projected periods for modellers to fill in).



# Data preparation
create a folder called "data", and prepare the following files
- 00APEC.xlsx: Original EGEDA data.
- economy_dict.csv: a reference table that includes economy name used by original EGEDA and self-defined layout(format you want).
- scenario_list.xlsx: scenario that we consider.

# EGEDA data preprocessing
Transfer the format of item name that we want, then we can get EGEDA raw data with processed layout. Notice that we only change EGEDA's item format here.
- remove blanks
- use lowercase
- use underscore
- replace "&" with "and"
- use two digits item_number 
...

# Export the fuels and sectors
Export these files to a folder called temp (the destination is not crucial).
- clean_egeda_fuel_name.xlsx
- clean_egeda_sector_name.xlsx

# (In excel) Create reference tables for fuels and sectors
- Create the reference tables in folder called "manuel_adjust" in "data" folder.
	- .\data\manuel_adjust\reference_table_fuel.xlsx
	- .\data\manuel_adjust\reference_table_sector.xlsx
- Paste the clean EGEDA item name and your self-defined columns in the files.
	- Notice that our self-defined layout separate different levels into different columns.
	- To align with the clean EGEDA item name, we use the lowest level in each row.
- Compare the differences, align the item name, and make some remarks in this file.

# Import the reference table, and compare the difference in item name (item number does not matter here)
- Check if there is any difference in item name. 
- Ideally, if you create your self-defined layout by the format you use to clean the original EGEDA layout, we do not have to revised the reference table again. Somehow, it's safer to do the comparison and revision again.


# (In excel) create revised reference tables for fule and sectors
- Duplicate the refernce excel files and rename it as xxx_revised.
	- .\data\manuel_adjust\reference_table_fuel_revised.xlsx
	- .\data\manuel_adjust\reference_table_sector_revised.xlsx
- Revised the difference you found.
- Mark the revised part (optional).

# Import the revised reference tables, and replace the columns of EGEDA dataframe
Here, we start connecting the self-defined layout with the EGEDA historical data
- Import the xxx_revised excel files and name it as fuel_mapping/ sector_mapping.
- Merge the mapping dataframe with the egeda dataframe.
- Create a new column called 'replace_materials' and replace the fuels series by the following rules.
| fules    | replace_materials | unique_the_end_of_fuels    |
| -------- | :-----:		   | :---     					|
| Alice→   |  Alice            | nan    					|
| nan      |  Bob              | ←Bob   					|
| Charlie  |  David            | ←David 					|
- Do the same thing to sectors.

# Import the multi-column self-defined layout, and create an extra column: lowest level item name
(Notice that the self-define item name before this step is the "lowest level item name")
- It's highly recommended to clean it in excel when you revise the reference table. With that, this stpe will be not so important.
- Clean the multi-column self-defined layout again, just in case. 
- Create an extra column (lowest level item name) for the multi-column self-define layout you just imported.
	- sector_key_col
	- fuel_key_col
		- the key series is create to connect the multi-column dataframe with the egeda historical dataframe.
- The final dataframe here
	- fuel_layout
	- sector_layout

# Deal with the aggregation, disaggregation, and new rows
- See reference_table_fuel_revised.xlsx and reference_table_sector_revised.xlsx for details.
- We excluded the row used for aggregation in principle. However, we may keep some rows as subsectors.

# Merge layout with the EGEDA historical data
- Cross merge the sector_layout, fuel_layout, econ, year
- Merge with the egeda historical dataframe on ```'economy', 'year', 'fuels', 'sectors'```, ```'economy', 'year', 'fuel_key_col', 'sector_key_col'```.
- Drop the unnecessary columns.

# Pivot the dataframe
Set "year" as columns to save time and space.

# Merge scenario
Recall that you prepared the data we mentioned at the beginning.

# Expend the projected years (2021 to 2070)
We create the extra columns (years) for the projected periods.

# Export the final result


# Potential Improvement
- Rows with x should be list first.


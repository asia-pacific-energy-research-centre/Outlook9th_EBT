#%%
# Set working directory to be the project folder 
import os
import re
import pandas as pd 
import numpy as np
import glob
from datetime import datetime
from utility_functions import *
import yaml 
#handle worning fore behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operatio
pd.options.mode.chained_assignment = None  # default='warn'
########################################################################################################################################################################

# =============================================================================
# Activity-to-Missing Sectors Dictionary
# =============================================================================
# This dictionary defines, for each activity, the associated energy sectors (and their
# sub-levels) that represent missing or indirect energy use. For example, the “own use”
# of energy for LNG transformation might not be directly available; instead, we rely on 
# proxy sectors (like LNG imports/exports) to represent the transformation activity.
#
# In this dictionary:
#   - Each key is an activity (e.g., 'gas_works', 'lng_imports', etc.).
#   - The values are dictionaries that list the relevant sector identifiers under keys such 
#     as 'sub1sectors', 'sub2sectors', or even 'sectors'. These identifiers are used to extract
#     data corresponding to the activity for later adjustments and projection.
#
# This mapping ensures that energy demand totals for each activity (e.g., from transformation
# or own use) are correctly paired with the corresponding energy data. It also helps ensure that
# the model does not inadvertently double count energy when merging datasets.
acitvity_to_missing_sectors_dict = {
    'gas_works': {
        'sub2sectors': ['09_06_01_gas_works_plants', '10_01_02_gas_works_plants']
    },
    # 'lng_imports': {#
    #     'sub2sectors': ['09_06_02_liquefaction_regasification_plants', '10_01_03_liquefaction_regasification_plants']
    # },
    # 'losses_from_lng_liquefaction_gasification': {
    #     'sub2sectors': ['10_01_03_liquefaction_regasification_plants']
    # },
    'oil_and_gas_extraction': {
        'sub2sectors': ['10_01_12_oil_and_gas_extraction']
    },
    'petrochemical_industry': {
        'sub1sectors': ['09_09_petrochemical_industry']#17_nonenergy_use e.g for 07_03_naphtha - this could be an alterantive proxy
    },

    # 'biofuels_processing': {
    #     'sub1sectors': ['09_10_biofuels_processing']
    # },
    'charcoal_processing_fuel_wood_input': {#FIXED TO JUST BEFORE INPUT FUELS AFTER DIFFICULTIES WITH MONOR FUEL SUPPLY MODELLING. OUTPUT of cahrcoal IS NOW HANDLED IN make_manual_changes_to_rows()
    'sub1sectors': ['09_11_charcoal_processing'],#after a bit of deliberation i decded to include this, even though we estiamte charcoal production in the minor fuel supply modelling system based on demand. The problem is that we still need that, but also having this is more accurate. I expect that doubel ups will get rmoved by the supply balancing system anyway. one issue is that its proxy is just total charcoal demand. but i thinkt hat should be ok given modellers continue to project that.
    'subfuels': ['15_01_fuelwood_and_woodwaste', '15_05_other_biomass'] 
    },
    # 'charcoal_processing_losses': {#there are none of these anyway
    #     'sub2sectors': ['10_01_15_charcoal_production_plants']#to avoid losing losses from demand we will contineu to estiamte thse, but since transfomration supply is just a bit difficult and satisfied by production, we wont do that.
    # },
    'electric_boilers': {
        'sub1sectors': ['09_04_electric_boilers']  # Note: only applicable in Russia; data is shifted to heat output and energy input for heat.
    },
    'coal_transformation_own_use': {#'10_01_05_coke_ovens',#coke ovens are being handled by the industry sector model for 02_coal_prodcuts which is 99.99% of the coke ovens energy use so we'll just stick wuth that.
        'sub2sectors': [
            '10_01_08_patent_fuel_plants',
            '10_01_09_bkb_pb_plants'
        ]
    },
    'coal_transformation_specific_fuels': {#we seem to estimate only up to 09_08_coal_transformation for these sectors. this is meant to disaggregte that out into the sub2sectors. Im actually not 100% this is even working. e.g. 04_CHL_coal_transformation_ref_2025_04_21.csv contains 09_08_coal_transformation but no sub2sectors.
        'sub2sectors': ['09_08_05_liquefaction_coal_to_oil', '09_08_04_bkb_pb_plants', '09_08_03_patent_fuel_plants',
                        '09_08_02_blast_furnaces', '09_08_01_coke_ovens'],
        'fuels': ['01_coal', '02_coal_products']                
    },
    'chp_and_heat_plants': {
        'sub2sectors': ['10_01_01_electricity_chp_and_heat_plants']  # These are already modeled in the power sector.
    },
    'pumped_hydro': {
        'sub2sectors': ['10_01_13_pump_storage_plants']  # These are already modeled in the power sector.
    },
    'nuclear_industry': {
        'sub2sectors': ['10_01_14_nuclear_industry']  # Data available only up to 1984; can be ignored.
    },
    'gasification_biogases': {
        'sub2sectors': ['10_01_16_gasification_plants_for_biogases']
    },
    'coal_mines': {
        'sub2sectors': ['10_01_06_coal_mines']
    },
    'chemical_heat': {
        'sub1sectors': ['09_05_chemical_heat_for_electricity_production']  # Assumed to come from the electricity sector as an 'other' category.
    },
    'nonspecified_transformation': {
        'sub1sectors': ['09_12_nonspecified_transformation']
    },
    'nonspecified_own_uses': {
        'sub2sectors': ['10_01_17_nonspecified_own_uses']
    },
    'nonspecified_others': {
        'sub1sectors': ['16_05_nonspecified_others']
    },
    # 'pipeline_transport': {
    #     'sub1sectors': ['15_05_pipeline_transport']
    # },
    'nonspecified_transport': {
        'sub1sectors': ['15_06_nonspecified_transport']
    }
}

# =============================================================================
# Activity-to-Proxies Dictionary
# =============================================================================
# This dictionary maps each activity to one or more proxy data sources. In many cases, the
# activity of interest does not have directly modeled data, so a proxy is used instead. For
# example, the energy use proxy for LNG transformation might be the sum of LNG import and export
# values.
#
# In this dictionary:
#   - Each key is an activity (same as those in acitvity_to_missing_sectors_dict).
#   - The corresponding value is a dictionary that specifies which sectors, sub-sectors,
#     fuels, or subfuels should be summed (and then possibly subtracted from the activity-specific
#     total) to serve as a proxy for the activity.
#
# The proxy is calculated by summing the data for the given identifiers, subtracting out the data
# that is being estimated (to avoid double-counting), and then using the resulting ratio as the
# basis for projecting energy use into the future.
activity_to_proxies_dict = {
    'gas_works': {'subfuels': ['08_03_gas_works_gas']},
    # 'losses_from_lng_liquefaction_gasification': {'fuels': ['08_gas'], 'sub2sectors': ['09_06_02_liquefaction_regasification_plants']},#note that for oecd coutnries we dont have historical splits between lng and nautral gas for imports/exports as well as any data on 09_06_02_liquefaction_regasification_plants. hwoever since this script will default to an avg of all economies if one economy's data is missing we can still use this script to project the future energy use for this sector.the issue is though, that these economies DO have dataon losses in this sector.. but im sure the difference wont be too major and it shoudlget caught in the error checking if ti is
    
    'oil_and_gas_extraction': {'sectors': ['01_production'], 'fuels': ['08_gas', '06_crude_oil_and_ngl']},
    'petrochemical_industry': {'sub2sectors': ['14_03_02_chemical_incl_petrochemical']},
    # 'biofuels_processing': {'subfuels': ['16_05_biogasoline', '16_06_biodiesel', '16_07_bio_jet_kerosene',
    #                                       '16_08_other_liquid_biofuels', '15_01_fuelwood_and_woodwaste',
    #                                       '15_02_bagasse', '15_04_black_liquor', '15_05_other_biomass']},
    'charcoal_processing_fuel_wood_input': {'subfuels': ['15_03_charcoal'], 'sectors': ['12_total_final_consumption']},
    # 'charcoal_processing_losses': {'subfuels': ['15_03_charcoal']},#there are none of these anyway
    'electric_boilers': {None},  # No proxy needed; assumed to be projected elsewhere.
    'coal_transformation_own_use': {'sub1sectors': ['09_08_coal_transformation'], 'fuels': ['01_coal', '02_coal_products']},
    'coal_transformation_specific_fuels': {'sectors': ['09_08_coal_transformation'], 'fuels': ['01_coal', '02_coal_products']},
    'chp_and_heat_plants': {None},
    'pumped_hydro': {None},
    'nuclear_industry': {None},
    'gasification_biogases': {'subfuels': ['16_01_biogas']},
    'coal_mines': {'sectors': ['01_production'], 'fuels': ['01_coal']},
    'chemical_heat': {None},
    'nonspecified_transformation': {'sectors': ['09_total_transformation_sector']},
    'nonspecified_own_uses': {'sub1sectors': ['10_01_own_use']},
    'nonspecified_others': {'sectors': ['16_other_sector']},
    'nonspecified_transport': {'sectors': ['15_transport_sector']}
    # 'pipeline_transport': {'sectors': ['01_production'], 'subfuels': ['08_01_natural_gas']},
}


###################
#since russia is a bit special we will do some specifics separatey here:
russia_changes_to_activity_to_proxies_dict = { 
    'nonspecified_transformation': {'sectors': ['12_total_final_consumption'], 'subfuels': ['08_01_natural_gas']},
    'nonspecified_own_uses': {'sectors': ['12_total_final_consumption'], 'subfuels':['07_09_lpg', '07_08_fuel_oil']},#all data is for these combinations so we just want to base it off total demand for these fuels in the sub1sectors.
}
canada_heat_activity = {
    'canada_heat': {'sectors': ['19_heat_output_in_pj']}
}
canada_heat_activity_proxy = {
    'canada_heat': {'fuels': ['18_heat']}
}

#CHINA GAS WORKS - we will remove previous gas works estimation entries and replace with more specific ones.
china_removed_activity = {
    'gas_works': {
        'sub2sectors': ['09_06_01_gas_works_plants', '10_01_02_gas_works_plants']
}}
china_removed_activity_proxy = {
    'gas_works': {'subfuels': ['08_03_gas_works_gas']}}
#then add in esimtaions for losses by fuel.. so the amount of petroleum losses is based on use of petroleum in gas works, same for gas and coal.
china_gas_works_activity = {
    'gas_works_natural_gas': {
        'sub2sectors': ['10_01_02_gas_works_plants'],
        'subfuels': ['08_01_natural_gas']},
    'gas_works_petroleum': {
        'sub2sectors': ['10_01_02_gas_works_plants'],
        'fuels': ['07_petroleum_products']},
    'gas_works_coal': {
        'sub2sectors': ['10_01_02_gas_works_plants'],
        'fuels': ['01_coal']},
    'gas_works_electricity': {
        'sub2sectors': ['10_01_02_gas_works_plants'],
        'fuels': ['18_electricity']},
    'gas_works_heat': {
        'sub2sectors': ['10_01_02_gas_works_plants'],
        'fuels': ['18_heat']}
}
china_gas_works_activity_proxy = {
    'gas_works_natural_gas': {'sub2sectors': ['09_06_01_gas_works_plants'], 'subfuels': ['08_01_natural_gas']},
    'gas_works_petroleum': {'sub2sectors': ['09_06_01_gas_works_plants'], 'fuels': ['07_petroleum_products']},
    'gas_works_coal': {'sub2sectors': ['09_06_01_gas_works_plants'], 'fuels': ['01_coal']},
    'gas_works_electricity': {'sub2sectors': ['09_06_01_gas_works_plants'], 'fuels': ['17_electricity']},
    'gas_works_heat': {'sub2sectors': ['09_06_01_gas_works_plants'], 'fuels': ['18_heat']}
}
#################################
#korea 06_x_other_hydrocarbons - 08_transfers # we need a projection of transfers in this sector to handle the supply mistmatch and allow us to increase use of 06_x_other_hydrocarbons in the refinery sector to repalce curde oil. this will also lead to a decrease in crude oil imports which is a substantial amount of crude oil and a great change to make  
# # once this is done i will need to go through and adjsut the crude use downwards by this amount as well as the crude oil imports, and use of these hydrocarbons in the refinery sector.
korea_other_hydrocarbons_activity = {
    'korea_other_hydrocarbons': {
        'sectors': ['08_transfers', '11_statistical_discrepancy'],#the statistical discrepancy is needed to balance the data unfortunately. at least in 2022, it is -150pj and transfers is 380pj, so it kind of cancels out some of that transfers which is kind of appearing out of nowhere...
        'subfuels': ['06_x_other_hydrocarbons']
    },
}
korea_other_hydrocarbons_activity_proxy = {
    'korea_other_hydrocarbons': {
        'sectors': ['09_total_transformation_sector'],
        'sub1sectors': ['09_09_petrochemical_industry'],
        'subfuels': ['06_x_other_hydrocarbons']
    }
}
#################################
#australia 06_x_other_hydrocarbons and lpg - 08_transfers # we need a projection of transfers in this sector to handle the supply mistmatch and allow us to export that amount of these fuels. 
australia_other_hydrocarbons_activity = {
    'australia_other_hydrocarbons': {
        'sectors': ['08_transfers'],
        'subfuels': ['06_x_other_hydrocarbons']
    },
    'australia_lpg': {
        'sectors': ['08_transfers'],
        'subfuels': ['07_09_lpg']
    }
}
australia_other_hydrocarbons_activity_proxy = {
    'australia_other_hydrocarbons': {
        'sectors': ['09_total_transformation_sector'],
        'sub1sectors': ['09_07_oil_refineries'],
        'subfuels': ['06_x_other_hydrocarbons']
    },
    'australia_lpg': {
        'sectors': ['03_exports'],#since we are doing this to ensure exports remain as is, it seems reasonable to use this as a proxy.
        'subfuels': ['07_09_lpg']
    }
}
###################################

thailand_nonspecified_transformation_output = {
    'nonspecified_transformation': {
        'sub1sectors': ['09_12_nonspecified_transformation'],
        'subfuels': ['07_09_lpg', '08_01_natural_gas', '06_02_natural_gas_liquids']#we will use this to estimate the output of ethane and lpg from the nonspecified transformation sector.
    }
}
#we will base this off a manuallly createed row which specifies to hold constant the value, since we just want it to have a consistent value in the future years.
thailand_nonspecified_transformation_output_proxy = {
    'nonspecified_transformation': {
        'sectors': ['CONSTANT']
    }
}
#and make a specific one for ehtane absed on the non energy use of ethane

thailand_ethane_nonspecified_transformation_output = {
    'nonspecified_transformation_ethane': {
        'sectors': ['09_12_nonspecified_transformation'],
        'subfuels': ['07_11_ethane']
    }
}
thailand_ethane_nonspecified_transformation_output_proxy = {
    'nonspecified_transformation_ethane': {
        'sectors': ['17_nonenergy_use'],
        'subfuels': ['07_11_ethane']
    }
}
#################################
#BRUNEI
# hey found an issue in coal for the autoproducer (electricity plant used exclusively by a plant) from the oil refinery (hengyi). Bascially we were accidentally recategorising coal use in 10_01_11_oil_refineries (own use) as electricity generation inputs and tehn not projecting the smaller actual electricity gneration inputs. 
 
# see cahrt below for example (TGT)
# intended (i did a quick projection of power inputs based on ratio in base year): 
# actual:
 
# This wil look weird in the charts so we will need to do soemthing about it. It will also mean tehre is extra ~20pj of coal imports in/around 2030 and onwards. 
# We will also need to fix the fuel consumption in power sector (but not generation since that is correct) so there is no increase in 2023 due to miscategorisation as well as setting the increase in ~2025 to be the amount used by pwoer sector (blue) in chart 1 above rather than own use (black). 
 
# I've built a system into the EBT code for post hoc changes like tehse which i think is suited to this problem, so hopefully wont cause extra work for anyone else, and only a mniimal amount for me. 
 
# may pay to double check the ratio between coal demand for coal generation and coal genration stays the same too.. 
 
# Anyway just a quick note for later!
# bd_coal_own_use_for_oil_refineries_activity = {
#     'bd_coal_own_use_for_oil_refineries': {'fuels': ['01_coal'], 'sub2sectors': ['10_01_11_oil_refineries']}
# }
# bd_coal_own_use_for_oil_refineries_proxy = {
#     'bd_coal_own_use_for_oil_refineries': {'fuels': ['01_coal'], 'sectors': ['18_electricity_output_in_gwh']}
# }#todo take away the extra coal use from this fro the bd power coal use. Also note that this depends on the assumption that the only coal power producer is associated with the refinery where a portion of that heat form that power is used for own use in the refinery. without this assumption we would have to think hard about what it could be based on, as it isnt clear there is any other proxy, i think.
#################################
#=============================================================================
# Function: def estimate_missing_sectors_using_activity_estimates(df, 

# =============================================================================
def estimate_missing_sectors_using_activity_estimates(df, economy,acitvity_to_missing_sectors_dict=acitvity_to_missing_sectors_dict, activity_to_proxies_dict=activity_to_proxies_dict,MERGE_ONTO_INPUT_DATA=False,SAVE_OUTPUT_TO_MODELLED_DATA_FOLDER=True,PLOT=True):
    """
    IMPORTANT: MAKE SURE THAT ANY ESTIAMTES MADE IN THIS FUNCTION ARE ABSOLUTELY REQUIRED BECAUSE THERE IS NO WAY TO AUTOMATICALLY CHECK THAT THERE ARE NO DOUBLE ESTIAMTES BEING DONE. THIS IS BECAUSE THE OUTPUT IS SAVED TO THE MODELLED DATA FOLDER AND THEN MERGED BACK INTO THE INPUT DATA - SO THE INPUT DATA WILL CONTAIN THESE ESTIMATES FROM PREVIOUS RUNS AND WILL BE REPALCED WITH THE ESTIMATES MADE IN THIS FUNCTION - AND BECAUSE OF THAT WE CANT KNOW WEHTEHR PREXISTING ESTIAMTES ARE FORM THIS FUNCTION OR NOT.
    
    Estimate own use, transformation and other missing energy uses for sectors we know are missing based on activity proxies. This is mostly for where a modeller has not been assigned to these sectors and they arent suited to being modelled in a separate model (normally because they are minor sectors and the time spent on properly mdoelling them isnt worht it comapred to this solution - as an example the lng trasnformation, losses and gas pipeline sectors were orignally modelled here but because they are quite important and can be eaily grouped we moved them to create_transformation_losses_pipeline_rows_for_gas_based_on_supply()in this repo.   
     
    This function projects future energy use for certain activities by:
      1. Extracting energy demand data for specific sectors (defined in acitvity_to_missing_sectors_dict).
      2. Computing a proxy for each activity from available energy data (using activity_to_proxies_dict).
      3. Calculating the ratio between the observed energy use in the base year and the proxy value.
      4. Using this ratio to project future energy use, based on the projected growth of the proxy.
      5. Plotting the projected trends to visually verify the results.
      
    Additionally, the function performs several data consistency checks:
      - It verifies that no non-zero values exist in the merged data that might indicate double counting.
      - It checks that there are no values at sub-levels that should be subtotals only.
      - It raises errors (and triggers breakpoints) if data inconsistencies are detected.
    
    Parameters:
      df (pd.DataFrame): DataFrame containing the energy use data, with year columns as 4-digit strings.
      acitvity_to_missing_sectors_dict (dict): Dictionary mapping each activity to its associated energy sectors,
                                               which represent missing or indirect data.
      activity_to_proxies_dict (dict): Dictionary mapping each activity to the proxy identifiers used for estimating
                                       its energy use.
    
    Returns:
      pd.DataFrame: A modified DataFrame with projected energy use for the activities, where the projections
                    replace the original data in the future years.
    """
    #######temp. if we get too many of these just make a function
    if economy == '16_RUS':
        #use the dicts for russia
        for key, value in russia_changes_to_activity_to_proxies_dict.items():
            activity_to_proxies_dict[key] = value
    if economy == '03_CDA':
        for key, value in canada_heat_activity.items():
            acitvity_to_missing_sectors_dict[key] = value
        for key, value in canada_heat_activity_proxy.items():
            activity_to_proxies_dict[key] = value
    if economy == '05_PRC':
        #use the dicts for china
        for key, value in china_removed_activity.items():
            acitvity_to_missing_sectors_dict.pop(key, None)
        for key, value in china_removed_activity_proxy.items():
            activity_to_proxies_dict.pop(key, None)
        for key, value in china_gas_works_activity.items():
            acitvity_to_missing_sectors_dict[key] = value
        for key, value in china_gas_works_activity_proxy.items():
            activity_to_proxies_dict[key] = value
    if economy == '09_ROK':
        for key, value in korea_other_hydrocarbons_activity.items():
            acitvity_to_missing_sectors_dict[key] = value
        for key, value in korea_other_hydrocarbons_activity_proxy.items():
            activity_to_proxies_dict[key] = value
    if economy == '01_AUS':
        for key, value in australia_other_hydrocarbons_activity.items():
            acitvity_to_missing_sectors_dict[key] = value
        for key, value in australia_other_hydrocarbons_activity_proxy.items():
            activity_to_proxies_dict[key] = value
    if economy == '19_THA':
        for key, value in thailand_ethane_nonspecified_transformation_output.items():
            acitvity_to_missing_sectors_dict[key] = value
        for key, value in thailand_ethane_nonspecified_transformation_output_proxy.items():
            activity_to_proxies_dict[key] = value
        for key, value in thailand_nonspecified_transformation_output.items():
            acitvity_to_missing_sectors_dict[key] = value
        for key, value in thailand_nonspecified_transformation_output_proxy.items():
            activity_to_proxies_dict[key] = value
            
    # if economy == '02_BD':
    #     for key, value in bd_coal_own_use_for_oil_refineries_activity.items():
    #         acitvity_to_missing_sectors_dict[key] = value
    #     for key, value in bd_coal_own_use_for_oil_refineries_proxy.items():
    #         activity_to_proxies_dict[key] = value
    # breakpoint()
    #######temp. if we get too many of these just make a function
    #      if the ratio is not able to be clacualted then base it off an average of all economies data for this activity.
    # all_economies_df, date_id = find_most_recent_file_date_id('results/', RETURN_DATE_ID=True, filename_part = 'model_df_wide_tgt')
    # all_economies_df = pd.read_csv(os.path.join('results', f'model_df_wide_tgt_{date_id}.csv'))
    df = df.loc[df['economy'] == economy]
    df_copy = df.copy()
    str_OUTLOOK_BASE_YEAR = str(OUTLOOK_BASE_YEAR)
    if not all(isinstance(col, str) for col in df.columns):
        #make them strs
        df.columns = df.columns.astype(str)
        
    # Identify year columns (assumed to be 4-digit numbers represented as strings)
    year_columns = [col for col in df.columns if re.match(r'^\d{4}$', str(col))]
    
    # Find years before the base year and drop them
    pre_base_year_columns = [col for col in year_columns if int(col) < OUTLOOK_BASE_YEAR]
    future_year_columns = [col for col in year_columns if col not in pre_base_year_columns and col != str_OUTLOOK_BASE_YEAR]
    

    # Exclude aggregate fuel rows (e.g., totals) to avoid skewing the projections.
    df = df.loc[~df['fuels'].isin(['19_total', '20_total_renewables', '21_modern_renewables'])]
    # all_economies_df = all_economies_df.loc[~all_economies_df['fuels'].isin(['19_total', '20_total_renewables', '21_modern_renewables'])]
    #create 'CONSTANT' row which has the same value (1) for the current and all future years.
    # breakpoint()
    def create_constant_row(df, future_year_columns, str_OUTLOOK_BASE_YEAR):
        """
        Create a constant row for the base year and future years.
        This row will have a value of 1 for the base year and all future years.
        """
        #     scenarios	economy	sectors	sub1sectors	sub2sectors	sub3sectors	sub4sectors	fuels	subfuels	subtotal_layout	subtotal_results
        # reference	19_THA	01_production	x	x	x	x	01_coal	01_01_coking_coal	FALSE	FALSE
        #grab a row form df by dropping duplciaes after grouping by the cols we need:
        
        constant_row = df[['scenarios', 'economy']].drop_duplicates()
        # Add the necessary columns for future years.
        constant_row[str_OUTLOOK_BASE_YEAR] = 1  # Base year valu
        constant_row['sectors'] = 'CONSTANT'
        constant_row['sub1sectors'] = 'x'
        constant_row['sub2sectors'] = 'x'
        constant_row['sub3sectors'] = 'x'
        constant_row['sub4sectors'] = 'x'
        constant_row['fuels'] = 'CONSTANT'
        constant_row['subfuels'] = 'x'
        constant_row['subtotal_layout'] = False
        constant_row['subtotal_results'] = False
        # Add the constant row for future years.
        for year in future_year_columns:
            constant_row[year] = 1 
        df = pd.concat([df, constant_row], ignore_index=True)
        return df
    df = create_constant_row(df, future_year_columns, str_OUTLOOK_BASE_YEAR)
    
    # Separate base year and future years data.
    base_year_df_all_economies = df.drop(columns=future_year_columns + pre_base_year_columns)
    future_years_df = df.drop(columns=[str_OUTLOOK_BASE_YEAR] + pre_base_year_columns)
    historical_data = df.drop(columns=future_year_columns)
    
    # Drop subtotal rows.
    base_year_df_all_economies = base_year_df_all_economies[base_year_df_all_economies['subtotal_layout'] == False]
    historical_data = historical_data[historical_data['subtotal_layout'] == False]
    future_years_df = future_years_df[future_years_df['subtotal_results'] == False]
    try:
        # Drop rows where the base year value is zero.
        base_year_df_all_economies = base_year_df_all_economies.loc[~(base_year_df_all_economies[str_OUTLOOK_BASE_YEAR] == 0)]
    except:
        breakpoint()
    future_years_df = future_years_df.loc[~(future_years_df[future_year_columns] == 0).all(axis=1)]
    
    # -----------------------------------------------------------------------------
    # Extract data for sectors corresponding to missing activities.
    # -----------------------------------------------------------------------------
    extracted_sectors_df_base_year_all_economies = pd.DataFrame(columns=base_year_df_all_economies.columns)
    extracted_sectors_df_future = pd.DataFrame(columns=future_years_df.columns)
    
    for activity, sectors_dict in acitvity_to_missing_sectors_dict.items():
        
        matched_rows = base_year_df_all_economies.copy()
        matched_rows['activity'] = activity
        matched_rows_future = future_years_df.copy()
        matched_rows_future['activity'] = activity
        
        for key in ['sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'sectors', 'subfuels', 'fuels']:
            if key in sectors_dict:
                # Extract matching rows for the base year and label with the activity.
                matched_rows = matched_rows[matched_rows[key].isin(sectors_dict[key])].copy()
                
                # Do the same for future years.
                matched_rows_future = matched_rows_future[matched_rows_future[key].isin(sectors_dict[key])].copy()
        
        extracted_sectors_df_base_year_all_economies = pd.concat([extracted_sectors_df_base_year_all_economies, matched_rows], ignore_index=True)
        
        extracted_sectors_df_future = pd.concat([extracted_sectors_df_future, matched_rows_future], ignore_index=True)
                
    
    # -----------------------------------------------------------------------------
    # Compute proxies for each activity.
    # -----------------------------------------------------------------------------
    activity_proxy_df_base_year_all_economies = pd.DataFrame(columns=['economy', 'scenarios', 'activity', str_OUTLOOK_BASE_YEAR])
    activity_proxy_df_future = pd.DataFrame(columns=['economy', 'scenarios', 'activity'] + future_year_columns)
    
    for activity, proxy in activity_to_proxies_dict.items():
        
        if proxy == {None}:
            continue  # Skip activities that are assumed to be projected elsewhere.
        proxy_rows = base_year_df_all_economies.copy()
        proxy_rows_future = future_years_df.copy()
        if 'sectors' in proxy:
            proxy_rows = proxy_rows[proxy_rows['sectors'].isin(proxy['sectors'])]
            proxy_rows_future = proxy_rows_future[proxy_rows_future['sectors'].isin(proxy['sectors'])]
        if 'sub1sectors' in proxy:
            proxy_rows = proxy_rows[proxy_rows['sub1sectors'].isin(proxy['sub1sectors'])]
            proxy_rows_future = proxy_rows_future[proxy_rows_future['sub1sectors'].isin(proxy['sub1sectors'])]
        if 'sub2sectors' in proxy:
            proxy_rows = proxy_rows[proxy_rows['sub2sectors'].isin(proxy['sub2sectors'])]
            proxy_rows_future = proxy_rows_future[proxy_rows_future['sub2sectors'].isin(proxy['sub2sectors'])]
        if 'fuels' in proxy:
            proxy_rows = proxy_rows[proxy_rows['fuels'].isin(proxy['fuels'])]
            proxy_rows_future = proxy_rows_future[proxy_rows_future['fuels'].isin(proxy['fuels'])]
        if 'subfuels' in proxy:
            proxy_rows = proxy_rows[proxy_rows['subfuels'].isin(proxy['subfuels'])]
            proxy_rows_future = proxy_rows_future[proxy_rows_future['subfuels'].isin(proxy['subfuels'])]
        if 'sub3sectors' in proxy or 'sub4sectors' in proxy:
            breakpoint()
            raise ValueError("Sub3sectors and sub4sectors not yet implemented.")
        
        # Ensure values are absolute.
        proxy_rows[str_OUTLOOK_BASE_YEAR] = proxy_rows[str_OUTLOOK_BASE_YEAR].abs()
        proxy_rows_future[future_year_columns] = proxy_rows_future[future_year_columns].abs()
        proxy_rows = proxy_rows.groupby(['economy', 'scenarios']).sum().reset_index()
        proxy_rows_future = proxy_rows_future.groupby(['economy', 'scenarios']).sum().reset_index()
        proxy_rows['activity'] = activity
        proxy_rows_future['activity'] = activity
        
        activity_proxy_df_base_year_all_economies = pd.concat(
            [activity_proxy_df_base_year_all_economies, proxy_rows[['economy', 'scenarios', 'activity', str_OUTLOOK_BASE_YEAR]]],
            ignore_index=True
        )
        activity_proxy_df_future = pd.concat(
            [activity_proxy_df_future, proxy_rows_future],
            ignore_index=True
        )
        
    #calcualte an 00_APEC sum of all_economies data in activity_proxy_df_base_year_all_economies and extracted_sectors_df_base_year_all_economies which will allow us to create a ratio based on the average of all other economies to use when we cant calculate a ratio for a specific economy because they dont have that activity in the base year.
    apec_sum_extracted_sectors_df_base_year_all_economies = extracted_sectors_df_base_year_all_economies.groupby(['scenarios', 'activity']).sum().reset_index()
    apec_sum_activity_proxy_df_base_year_all_economies = activity_proxy_df_base_year_all_economies.groupby(['scenarios', 'activity']).sum().reset_index()
    #set econmoy to 00_APEC
    apec_sum_extracted_sectors_df_base_year_all_economies['economy'] = '00_APEC'
    apec_sum_activity_proxy_df_base_year_all_economies['economy'] = '00_APEC'
    #concatenate to the dfs
    extracted_sectors_df_base_year_all_economies = pd.concat([extracted_sectors_df_base_year_all_economies, apec_sum_extracted_sectors_df_base_year_all_economies], ignore_index=True)
    activity_proxy_df_base_year_all_economies = pd.concat([activity_proxy_df_base_year_all_economies, apec_sum_activity_proxy_df_base_year_all_economies], ignore_index=True)
    
    # breakpoint()
    # -----------------------------------------------------------------------------
    # Calculate the ratio between actual energy use and proxy in the base year.
    # -----------------------------------------------------------------------------
    ratio_df = extracted_sectors_df_base_year_all_economies.merge(
        activity_proxy_df_base_year_all_economies, on=['economy', 'scenarios', 'activity'],
        suffixes=('_energy', '_proxy')
    )
    ratio_df['ratio'] = np.where(
        ratio_df[str_OUTLOOK_BASE_YEAR + '_proxy'] == 0,
        np.where(ratio_df[str_OUTLOOK_BASE_YEAR + '_energy'] == 0, 0, np.nan),
        ratio_df[str_OUTLOOK_BASE_YEAR + '_energy'] / ratio_df[str_OUTLOOK_BASE_YEAR + '_proxy']
    )
    
    if ratio_df['ratio'].isnull().any():
        breakpoint()
        raise ValueError("Proxy for activity is 0 but energy use is not. Cannot base projection. May need to handle this case by using a different proxy for this economy.")
    
    #then where a ratio is 0, replace it with the average of all economies data for that activity
    apec_ratios = ratio_df.loc[ratio_df.economy == '00_APEC']
    ratio_df = ratio_df.loc[ratio_df.economy != '00_APEC']
    ratio_df = ratio_df.merge(apec_ratios, on=['scenarios', 'activity'], suffixes=('', '_apec'))
    ratio_df['ratio'] = np.where(ratio_df['ratio'] == 0, ratio_df['ratio_apec'], ratio_df['ratio'])
    ratio_df = ratio_df.drop(columns=[col for col in ratio_df.columns if col.endswith('_apec')])
    #check for 0s 
    if (ratio_df['ratio'] == 0).any():
        breakpoint()
        raise ValueError("0s detected in ratio. Check for missing data. May not be an issue but i think it would mean that no data was at all available for this activity in the base year in all economies.")   
    # breakpoint()
    # -----------------------------------------------------------------------------
    # Project future energy use using the computed ratio.
    # -----------------------------------------------------------------------------
    activity_proxy_df_future = activity_proxy_df_future[['economy', 'scenarios', 'activity'] + future_year_columns]
    future_projection_df = activity_proxy_df_future.merge(
        ratio_df[['economy', 'scenarios', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels', 'activity', 'ratio', 
                  str_OUTLOOK_BASE_YEAR + '_energy', str_OUTLOOK_BASE_YEAR + '_proxy']],
        on=['economy', 'scenarios', 'activity'],
        how='left', indicator=True
    )
    
    if (future_projection_df['_merge'] == 'left_only').any():
        future_projection_df = future_projection_df[future_projection_df['_merge'] != 'left_only']
    if (future_projection_df['_merge'] == 'right_only').any():
        breakpoint()
        raise ValueError("Right only merge detected in DataFrame.")
    
    for year in future_year_columns:
        #identfy any cases where the ratio is 0 and the energy use is not. this would be where the activity has appeared in the future years but not in the base year (e.g. country started importing lng later on in the projection so losses from that apear later on but have nothing to be estiamted based on, unless we use ratios calcualted from data from other economies). we will raise an error since for this economy we mustnt be able to project this missing sector usign the calcilated proxy to energy use ratio from the base year. 
        if any((future_projection_df['ratio'] == 0) & (future_projection_df[year] != 0)):
            breakpoint()
            raise ValueError(f"Proxy for activity is 0 but energy use is not in year {year}. Cannot base projection. May need to handle this case by using all economies data to calculate a ratio of activity to energy use.")
        future_projection_df[year] = future_projection_df['ratio'] * future_projection_df[year]
    
    if future_projection_df.isnull().any().any():
        breakpoint()
        raise ValueError("Null values detected in projection. Check missing proxies or data inconsistencies.")
    
    future_projection_df = future_projection_df.drop(columns=['_merge', str_OUTLOOK_BASE_YEAR + '_energy', str_OUTLOOK_BASE_YEAR + '_proxy'])
    
    # breakpoint()#check that CONSTANT row is not in the future_projection_df
    # -----------------------------------------------------------------------------
    # Plot the projected energy use.
    # -----------------------------------------------------------------------------
    import plotly.express as px
    if PLOT:
        future_projection_df_plot = future_projection_df.merge(
            historical_data, on=['economy', 'scenarios', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels'],
            suffixes=('_future', '_past')
        )
        if future_projection_df_plot.columns.str.contains('_future').any() or future_projection_df_plot.columns.str.contains('_past').any():
            breakpoint()
        future_projection_df_plot = future_projection_df_plot.melt(
            id_vars=['economy', 'scenarios', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels', 'subtotal_layout', 'subtotal_results', 'ratio', 'activity'],
            var_name='year', value_name='value'
        )
        future_projection_df_plot['dash'] = future_projection_df_plot['fuels'] + ' ' + future_projection_df_plot['subfuels']
        future_projection_df_plot['color'] = (
            future_projection_df_plot['activity'] + ' ' + future_projection_df_plot['sectors'] + ' ' +
            future_projection_df_plot['sub1sectors'] + ' ' + future_projection_df_plot['sub2sectors'] + ' ' +
            future_projection_df_plot['sub3sectors'] + ' ' + future_projection_df_plot['sub4sectors']
        )
        future_projection_df_plot['year'] = future_projection_df_plot['year'].astype(int)
        future_projection_df_plot = future_projection_df_plot.sort_values(by=['year', 'color'])
        if len(future_projection_df_plot[future_projection_df_plot['economy'] == economy])>0:
            fig = px.line(
                future_projection_df_plot[future_projection_df_plot['economy'] == economy],
                x='year', y='value', color='color', title=f'Energy use projections for {economy}',
                facet_col='scenarios', hover_data=['ratio', 'economy', 'scenarios', 'color'], line_dash='dash'
            )
            fig.add_vline(x=OUTLOOK_BASE_YEAR, line_dash='dash', line_color='black')
            fig.write_html(f'plotting_output/missing_sectors_projections/{economy}_energy_projections.html')
        
    future_projection_df = future_projection_df.drop(columns=['activity', 'ratio'])
            
    dateid=datetime.now().strftime('%Y%m%d')  
    #make the order of columns so the year cols are at end
    future_projection_df = future_projection_df[[col for col in future_projection_df.columns if col not in future_year_columns] + future_year_columns]
    if SAVE_OUTPUT_TO_MODELLED_DATA_FOLDER:
        #check for previous files and delete them
        files = glob.glob(f'data/modelled_data/{economy}/missing_sectors_projections_{economy}_*.csv')
        for f in files:
            os.remove(f)
        future_projection_df.to_csv(f'data/modelled_data/{economy}/missing_sectors_projections_{economy}_{dateid}.csv', index=False) 
             
    #also save to results\modelled_within_repo\missing_sectors
    if os.path.exists('results/modelled_within_repo/missing_sectors') == False:
        os.makedirs('results/modelled_within_repo/missing_sectors')
        
    future_projection_df.to_csv(f'results/modelled_within_repo/missing_sectors/missing_sectors_projections_{economy}_{dateid}.csv', index=False)
    
    if MERGE_ONTO_INPUT_DATA:
        # -----------------------------------------------------------------------------
        # Merge projections back into the original data.
        # -----------------------------------------------------------------------------
        df_new = df_copy.merge(
            future_projection_df, on=['economy', 'scenarios', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels'],
            suffixes=('_old', '_new'), how='left'
        )
        for year in future_year_columns:
            df_new[year] = np.where(df_new[year + '_new'].notnull(), df_new[year + '_new'], df_new[year + '_old'])
        df_new = df_new.drop(columns=[col for col in df_new.columns if col.endswith('_new') or col.endswith('_old')])
        
        if df_new.shape != df_copy.shape:
            breakpoint()
            raise ValueError("Shape of new DataFrame does not match original DataFrame.")
    
        return df_new
    else:
        return None




# def create_pipeline_losses_and_transformation_rows_for_supply_input(results_df):
#     #supply modelling of gas/coal and oil is done right at the end of all modelling using a pretty simple spreadsheet system. but this doesnt consider the transfomration, losses and even pipelines (pipelines for gas and oil). So we will add these in here when the supply data is abailable.
#     #we can identify the supply data by looking for a df where 01_coal, 06_crude_oil_and_ngl and 08_gas are in the fuels column and the sectors 01_production are available.. 
#     #we will estimate the following rows:
#     acitvity_to_missing_sectors_dict = {
#         'lng_imports': {
#             'sub2sectors': ['09_06_02_liquefaction_regasification_plants', '10_01_03_liquefaction_regasification_plants']
#         },
#         'lng_exports': {
#             'sub2sectors': ['09_06_04_gastoliquids_plants', '10_01_04_gastoliquids_plants']
#         },
#         'oil_and_gas_extraction': {
#             'sub2sectors': ['10_01_12_oil_and_gas_extraction']
#         },
#         'coal_mines': {
#             'sub2sectors': ['10_01_06_coal_mines']
#         },
#         'pipeline_transport': {
#             'sub1sectors': ['15_05_pipeline_transport']
#         }
#     }
#     activity_to_proxies_dict = {
#         'pipeline_transport': {'sectors': ['01_production'], 'subfuels': ['08_01_natural_gas']},
#         'coal_mines': {'sectors': ['01_production'], 'fuels': ['01_coal']},
#         'lng_imports': {'fuels': ['08_gas'], 'sectors': ['02_imports']},
#         'lng_exports': {'fuels': ['08_gas'], 'sectors': ['03_exports']},
#         'oil_and_gas_extraction': {'sectors': ['01_production'], 'fuels': ['08_gas', '06_crude_oil_and_ngl']}
#     }
    
#     #extract the rows we need to change
#     df = results_df.copy()
    
#     #we will run through each of the activities and find the historical ratio between the proxy and the actual energy use. we will then use this ratio to project the future energy use. if the ratio is not able to be clacualted then base it off an average of all economies data for this activity.
#     all_economies_df, date_id = find_most_recent_file_date_id('results/', RETURN_DATE_ID=True, filename_part = 'model_df_wide_tgt')
    
#     for activity, sectors_dict in acitvity_to_missing_sectors_dict.items():
#         for key in ['sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'sectors']:
#             if key in sectors_dict:
#                 # Extract matching rows for the base year and label with the activity.
#                 matched_rows = df[df[key].isin(sectors_dict[key])].copy()
#                 matched_rows['activity'] = activity
#                 extracted_sectors_df_base_year = pd.concat([extracted_sectors_df_base_year, matched_rows], ignore_index=True)
                
#                 # Do the same for future years.
#                 matched_rows = future_years_df[future_years_df[key].isin(sectors_dict[key])].copy()
#                 matched_rows['activity'] = activity
#                 extracted_sectors_df_future = pd.concat([extracted_sectors_df_future, matched_rows], ignore_index=True)













# #%%
# #import a df so we can test it out:
# df = pd.read_csv('../../merged_file_energy_09_ROK_20250212.csv')

# str_OUTLOOK_BASE_YEAR = str(OUTLOOK_BASE_YEAR)
# #year columns are 4 digit numbers
# year_columns = [col for col in df.columns if re.match(r'^\d{4}$', str(col))]
# #if the year cols in df are not strings in the data throw an error
# if not all(isinstance(col, str) for col in df.columns):
#     breakpoint()
#     raise ValueError("Year columns must be strings in the data.")
# #find the years before base year and drop them
# pre_base_year_columns = [col for col in year_columns if int(col) < OUTLOOK_BASE_YEAR]
# future_year_columns = [col for col in year_columns if col not in pre_base_year_columns and col != str_OUTLOOK_BASE_YEAR]

# #set imports of 02_coal_products to abs
# df.loc[(df['sectors'] == '02_imports') & (df['fuels'] == '02_coal_products'), future_year_columns] = df.loc[(df['sectors'] == '02_imports') & (df['fuels'] == '02_coal_products'), future_year_columns].abs()
# df.loc[(df['sectors'] == '07_total_primary_energy_supply') & (df['fuels'] == '02_coal_products'), future_year_columns] = df.loc[(df['sectors'] == '07_total_primary_energy_supply') & (df['fuels'] == '02_coal_products'), future_year_columns].abs()
#%%
#%%
#todo:
#clean up stuff to do with file paths in this folder. aswell as the datetime one

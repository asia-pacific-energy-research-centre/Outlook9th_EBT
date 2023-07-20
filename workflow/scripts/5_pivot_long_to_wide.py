"""Pivoting long to wide"""

import pandas as pd
from datetime import datetime

long_df = pd.read_csv('../../demand_results_data/19_THA_non_energy_ref.csv')

# Step 1: Pivot the long dataframe
wide_df = long_df.pivot(index=['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels'], columns='year', values='energy')

# Step 2: Reset the index
wide_df.reset_index(inplace=True)

# Step 3: Rename the columns
wide_df.columns.name = None

date_today = datetime.now().strftime('%Y%m%d')
wide_df.to_csv('../../demand_results_data/19_THA_demand_nonenergy_df_wide_'+date_today+'.csv', index=False)
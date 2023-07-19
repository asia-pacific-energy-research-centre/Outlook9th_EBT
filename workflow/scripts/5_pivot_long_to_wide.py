"""Pivoting long to wide"""

pd.read_csv

# Step 1: Pivot the long dataframe
wide_df = long_df.pivot(index=['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels'], columns='year', values='energy')

# Step 2: Reset the index
wide_df.reset_index(inplace=True)

# Step 3: Rename the columns
wide_df.columns.name = None

# Step 4: Display the wide dataframe
print(wide_df)
"""Comparing two csv files"""

import pandas as pd
import numpy as np

file1_path = '../../past_results/merged_file_19_THA20230720.csv'

file2_path = '../../past_results/merged_file_19_THA20230721.csv'

def compare_csv_files(file1_path, file2_path, tolerance=1e-4):
    # Step 1: Read the CSV files into DataFrames
    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)

    # Step 2: Compare the shape of the DataFrames
    if df1.shape != df2.shape:
        print("The CSV files have different shapes.")
        return

    # Step 3: Compare the values in each cell of the DataFrames (for numeric columns only)
    try:
        # Get only the numeric columns for comparison
        numeric_columns = df1.select_dtypes(include=[float, int]).columns

        # Compare the numeric columns using the specified tolerance
        diff_cells = abs(df1[numeric_columns] - df2[numeric_columns]) >= tolerance

        # Handle NaN values in the comparison
        nan_diff_cells = df1[numeric_columns].isna() != df2[numeric_columns].isna()

        # Combine the differences and NaN differences
        all_diff_cells = diff_cells | nan_diff_cells

        if not all_diff_cells.any().any():
            print("The CSV files are exactly the same.")
        else:
            print("The CSV files are different. Differences:")
            for index, row in all_diff_cells.iterrows():
                for col in all_diff_cells.columns:
                    if all_diff_cells.loc[index, col]:
                        if pd.isna(df1.loc[index, col]):
                            print(f"Difference at row {index}, column '{col}': NaN != {df2.loc[index, col]}")
                        elif pd.isna(df2.loc[index, col]):
                            print(f"Difference at row {index}, column '{col}': {df1.loc[index, col]} != NaN")
                        else:
                            print(f"Difference at row {index}, column '{col}': {df1.loc[index, col]} != {df2.loc[index, col]}")
    except AssertionError as e:
        print("Error occurred while comparing CSV files.")
        print(e)

compare_csv_files(file1_path, file2_path)
import pandas as pd

# Load both CSV files
file1 = "large_dataset.csv"
file2 = "large_dataset005.csv"

df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

# Ensure both CSV files have the same columns
common_columns = list(set(df1.columns) & set(df2.columns))  # Common columns in both files
df1_extra_cols = list(set(df1.columns) - set(df2.columns))  # Columns only in File1
df2_extra_cols = list(set(df2.columns) - set(df1.columns))  # Columns only in File2

# Print extra columns if mismatch found
if df1_extra_cols or df2_extra_cols:
    print("⚠️ Column Mismatch Detected!")
    print("Extra Columns in File1:", df1_extra_cols)
    print("Extra Columns in File2:", df2_extra_cols)

# Merge DataFrames on 'ID' if available; otherwise, perform a full outer join
if "ID" in df1.columns and "ID" in df2.columns:
    merged_df = pd.merge(df1, df2, on="ID", suffixes=('_src', '_tgt'), how="outer", indicator=True)
else:
    df1['Source'] = 'File1'
    df2['Source'] = 'File2'
    merged_df = pd.concat([df1, df2], ignore_index=True)
    merged_df["_merge"] = merged_df.duplicated(keep=False)

# Identify missing rows
missing_in_file2 = merged_df[merged_df["_merge"] == "left_only"]  # Present in File1, missing in File2
missing_in_file1 = merged_df[merged_df["_merge"] == "right_only"]  # Present in File2, missing in File1

# Identify row-wise differences
row_differences = merged_df[merged_df["_merge"] != "both"]

# Identify column-wise differences
column_differences = []
for col in common_columns:
    if col != "ID":
        mismatches = merged_df[merged_df[f"{col}_src"] != merged_df[f"{col}_tgt"]]
        if not mismatches.empty:
            column_differences.append(col)

# Create a detailed mismatch DataFrame
comparison_results = []
for col in column_differences:
    df_diff = merged_df[merged_df[f"{col}_src"] != merged_df[f"{col}_tgt"]]
    if not df_diff.empty:
        df_diff = df_diff[["ID", f"{col}_src", f"{col}_tgt"]]
        df_diff["Column"] = col
        comparison_results.append(df_diff)

if comparison_results:
    column_diff_df = pd.concat(comparison_results, ignore_index=True)
else:
    column_diff_df = pd.DataFrame(columns=["ID", "Column", "Source_Value", "Target_Value"])

# Save results to CSV
missing_in_file2.to_csv("missing_in_target.csv", index=False)
missing_in_file1.to_csv("missing_in_source.csv", index=False)
row_differences.to_csv("row_differences.csv", index=False)
column_diff_df.to_csv("column_differences.csv", index=False)

# Print summary
print("🔍 Comparison Completed!")
print(f"Missing Rows in Target saved to 'missing_in_target.csv'")
print(f"Missing Rows in Source saved to 'missing_in_source.csv'")
print(f"Row Differences saved to 'row_differences.csv'")
print(f"Column Differences saved to 'column_differences.csv'")

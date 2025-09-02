# Import necessary libraries
import pandas as pd
import os
import csv
import openpyxl
import numpy as np

# Define the data directory
data_dir = os.path.join(os.getcwd(), "D:/University of Cambridge/ARCH_MAHSA - General/MAHSA_Database/Thesauri/Thesauri_Audit/Spreadsheets/")

# Load the Excel workbook using openpyxl
workbook_path = os.path.join(data_dir, 'MAHSA_Thesauri_v4_JT_copy.xlsx')
print(workbook_path)
workbook = openpyxl.load_workbook(workbook_path)

#workbook = openpyxl.load_workbook(os.path.join(data_dir, 'MAHSA_Thesauri_v4_JT_copy.xlsx'))

# Print all sheet names for reference
print("Original sheets:", workbook.sheetnames)

# Delete unnecessary sheets
sheets_to_delete = [
    'Temp Concept Sheet', 'Relationships', 'ODK Only', 'Guidelines',
    'TempWorkSheet', 'PalaeolithicChronology (in prg)'
]
for sheet in sheets_to_delete:
    del workbook[sheet]

# Save the cleaned workbook
processed_path = os.path.join(data_dir, '1_Processing/excel_thesauri_processed.xlsx')
workbook.save(processed_path)

# Read the processed workbook
xls = pd.ExcelFile(processed_path)
print("Sheets to process:", xls.sheet_names)

# Collect all sheets into a list of DataFrames
df_list = [pd.read_excel(processed_path, sheet_name=sheet, header=None) for sheet in xls.sheet_names]

# Concatenate all sheets into a single DataFrame
df = pd.concat(df_list, ignore_index=True)

# Create column 8: if column 0 is 'Resource Model Node', copy column 1; else empty string
df[8] = np.where(df[0] == 'Resource Model Node', df[1], '')

# Forward-fill missing values in column 8
df[8] = df[8].replace('', pd.NA).ffill()

# Copy column 1 to column 9
df[9] = df[1]

# Replace blank strings in column 0 with NaN
df[0] = df[0].replace('', pd.NA)

# Drop rows where column 0 is in specific labels or both column 0 and 1 are NaN
labels_to_drop = ['Resource Model Node', 'ODK List Name', 'Legacy Data Column', 'ODK Value']
drop_condition = df[0].isin(labels_to_drop) | (df[0].isna() & df[1].isna())
df = df.loc[~drop_condition].copy()

# Transform column 8: replace spaces with underscores and lowercase
df[8] = df[8].str.replace(' ', '_').str.lower()

# Drop unnecessary columns (3 through 7)
df.drop(df.columns[4:8], axis=1, inplace=True)

# Rename columns to meaningful names
df.columns = ["odk_value", "concept_key", "definition", "list_order","list_name", "concept_value"]

# Print the final DataFrame
print(df)

# Save the final DataFrame to CSV, quoting all values
output_csv = os.path.join(data_dir, '1_Processing/excel_thesauri_processed.csv')
df.to_csv(output_csv, index=False, quoting=csv.QUOTE_ALL)

print('Processing Completed')

# =======================
# Step 1: Sort thesauri CSV by 'list_name' then 'concept_value'
# =======================
df.sort_values(by=['list_name', 'concept_value'], inplace=True)
df.reset_index(drop=True, inplace=True)

# =======================
# Step 2: Read arches thesauri export
# =======================
arches_path = os.path.join(data_dir, 'arches_thesauri_export.xlsx')
arches_df = pd.read_excel(arches_path)

# =======================
# Step 3: Make a copy of the arches spreadsheet
# =======================
arches_processed_path = os.path.join(data_dir, '1_Processing/arches_thesauri_processed.xlsx')
arches_df.to_excel(arches_processed_path, index=False)

# =======================
# Step 4: Sort the copied arches spreadsheet by 'list_name' then 'concept_value'
# =======================
arches_df.sort_values(by=['list_name', 'concept_value'], inplace=True)
arches_df.reset_index(drop=True, inplace=True)

# =======================
# Step 5: Unique list_name values from thesauri CSV
# =======================
thesauri_unique = pd.DataFrame(df['list_name'].dropna().unique(), columns=['thesauri_list_name'])

# =======================
# Step 6: Unique list_name values from arches processed CSV
# =======================
arches_unique = pd.DataFrame(arches_df['list_name'].dropna().unique(), columns=['arches_list_name'])

# =======================
# Step 7: Exact matches between the two unique lists
# =======================
exact_matches = pd.merge(
    thesauri_unique,
    arches_unique,
    left_on='thesauri_list_name',
    right_on='arches_list_name',
    how='inner'
)
exact_matches['exact_match'] = 'yes'

# =======================
# Step 8: Close matches for thesauri unique values not in exact matches
# =======================
from difflib import get_close_matches

# Identify unmatched values
thesauri_unmatched = thesauri_unique[~thesauri_unique['thesauri_list_name'].isin(exact_matches['thesauri_list_name'])]
arches_unmatched = arches_unique[~arches_unique['arches_list_name'].isin(exact_matches['arches_list_name'])]

# Function to find close match
def find_close(value, choices):
    matches = get_close_matches(value, choices, n=1, cutoff=0.8)  # cutoff=0.8 for similarity
    return matches[0] if matches else pd.NA

# Build DataFrame 4 for thesauri unmatched
list_name_t_nm = thesauri_unmatched.copy()
list_name_t_nm['arches_list_name'] = list_name_t_nm['thesauri_list_name'].apply(lambda x: find_close(x, arches_unmatched['arches_list_name'].tolist()))
list_name_t_nm['close_match'] = list_name_t_nm['arches_list_name'].apply(lambda x: 'yes' if pd.notna(x) else 'no')

# =======================
# Step 9: Close matches for arches unmatched values
# =======================
list_name_a_nm = arches_unmatched.copy()
list_name_a_nm['thesauri_list_name'] = list_name_a_nm['arches_list_name'].apply(lambda x: find_close(x, thesauri_unmatched['thesauri_list_name'].tolist()))
list_name_a_nm['close_match'] = list_name_a_nm['thesauri_list_name'].apply(lambda x: 'yes' if pd.notna(x) else 'no')

# =======================
# Step 10: Create new Excel file with three tabs
# =======================
output_excel_path = os.path.join(data_dir, '2_Comparison/thesauri_arches_list_name_comparison.xlsx')
#with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
#    exact_matches.to_excel(writer, sheet_name='list_name_matches', index=False)
#    list_name_t_nm.to_excel(writer, sheet_name='list_name_t_nm', index=False)
#    list_name_a_nm.to_excel(writer, sheet_name='list_name_a_nm', index=False)

# Combine thesauri-only and arches-only non-matches into one DataFrame
df_list_name_nm = pd.concat([list_name_t_nm, list_name_a_nm], ignore_index=True)

# Sort so that close matches ("yes") appear first
if "close_match" in df_list_name_nm.columns:
    df_list_name_nm = df_list_name_nm.sort_values(by="close_match", ascending=False)

# Save two sheets instead of three
with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
    exact_matches.to_excel(writer, sheet_name='list_name_matches', index=False)
    df_list_name_nm.to_excel(writer, sheet_name="list_name_nm", index=False)



print('Comparison completed. Output saved to', output_excel_path)
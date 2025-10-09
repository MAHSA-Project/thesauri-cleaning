import pandas as pd
import os

# === 1. Define file paths ===
input_path = r"D:\University of Cambridge\ARCH_MAHSA - General\MAHSA_Database\Thesauri\Thesauri_Audit\Spreadsheets\Common_BulkImportSheet.xlsx"
output_folder = r"D:\University of Cambridge\ARCH_MAHSA - General\MAHSA_Database\Thesauri\Thesauri_Audit\Spreadsheets\1_Processing"
output_file = os.path.join(output_folder, "choices_names_test.xlsx")

# === 2. Load the 'Person-Organization RM' sheet, skipping the first 3 rows ===
sheet_name = "Person-Organization RM"
df_raw = pd.read_excel(input_path, sheet_name=sheet_name, skiprows=3)

# === 3. Filter rows with 'Yes' in the 'KOBO Account' column (Z) ===
df_filtered = df_raw[df_raw['KOBO Account'].astype(str).str.strip().str.lower() == 'yes']

# === 4. Create the first dataframe (df) ===
df = pd.DataFrame({
    'name': df_filtered['MAHSA_ID'],               # column D
    'label': df_filtered['Name'],                  # column F
    'po_institution': df_filtered['Related Organization']  # column M
})

# === 5. Map institution names and ODK names ===
po_to_name = pd.Series(df_raw['Name'].values, index=df_raw['MAHSA_ID']).to_dict()
po_to_odk = pd.Series(df_raw['ODK Institute Name'].values, index=df_raw['MAHSA_ID']).to_dict()

df['institutename'] = df['po_institution'].map(po_to_name)
df['institute_name'] = df['po_institution'].map(po_to_odk)  # renamed from ODKinstitutename

# === 6. Sort alphabetically by institutename, then label ===
df.sort_values(by=['institutename', 'label'], inplace=True, ignore_index=True)

# === 7. Add 'Not listed' row after each institutename group ===
new_rows = []
for name, group in df.groupby('institutename', sort=False):
    new_rows.append(group)
    not_listed_row = {
        'name': 'not_listed',
        'label': 'Not listed here',
        'po_institution': None,
        'institutename': name,
        'institute_name': group['institute_name'].iloc[0] if not group['institute_name'].isna().all() else None
    }
    new_rows.append(pd.DataFrame([not_listed_row]))

df = pd.concat(new_rows, ignore_index=True)

# === 8. Insert new first column 'list_name' and fill with 'recorder_list' ===
df.insert(0, 'list_name', 'recorder_list')

# === 9. Create second dataframe of unique institute names ===
df_unique = df[['institutename', 'institute_name']].drop_duplicates().rename(
    columns={'institutename': 'name', 'institute_name': 'label'}
)
df_unique.insert(0, 'list_name', 'institute_name')

# === 10. Keep only the required columns in the first dataframe ===
df = df[['list_name', 'name', 'label', 'institute_name']]

# === 11. Combine both dataframes ===
df_combined = pd.concat([df, df_unique], ignore_index=True, sort=False)

# === 12. Add empty fields for future use ===
df_combined['media::image'] = ""
df_combined['transect_method_list'] = ""
df_combined['heritage_resource_classification'] = ""

# === 13. Reorder columns as requested ===
df_combined = df_combined[
    ['list_name', 'name', 'label', 'media::image', 'transect_method_list', 'institute_name', 'heritage_resource_classification']
]

# === 14. Save to Excel ===
os.makedirs(output_folder, exist_ok=True)
df_combined.to_excel(output_file, index=False)

# === 15. Print summary ===
num_people = len(df_filtered)
num_institutes = df['institute_name'].dropna().nunique()

print("✅ Process complete!")
print(f"📊 People with KOBO accounts processed: {num_people}")
print(f"🏛️ Unique institutions found: {num_institutes}")
print(f"💾 File saved to:\n{output_file}")
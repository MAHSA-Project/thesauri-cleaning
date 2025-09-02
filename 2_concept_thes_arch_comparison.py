# =======================
# Standalone script for comparing concepts inside matching list_names
# =======================

import pandas as pd
import os
from difflib import get_close_matches
import datetime

# =======================
# Define data directory (adjust path if needed)
# =======================
data_dir = os.path.join(os.getcwd(), "D:/University of Cambridge/ARCH_MAHSA - General/MAHSA_Database/Thesauri/Thesauri_Audit/Spreadsheets/")

# =======================
# Input file paths
# =======================
thesauri_path = os.path.join(data_dir, '1_Processing/excel_thesauri_processed.csv')
arches_processed_path = os.path.join(data_dir, '1_Processing/arches_thesauri_processed.xlsx')
list_name_matches_path = os.path.join(data_dir, '2_Comparison/thesauri_arches_list_name_comparison.xlsx')

# =======================
# Load thesauri CSV (produced earlier)
# =======================
thesauri_df = pd.read_csv(thesauri_path)

# =======================
# Load arches processed Excel
# =======================
arches_df = pd.read_excel(arches_processed_path)

# =======================
# Load exact list_name matches (tab 'list_name_matches')
# =======================
exact_matches = pd.read_excel(list_name_matches_path, sheet_name='list_name_matches')

# =======================
# DataFrames to collect results
# =======================
concept_exact_matches = []
concept_non_matches = []

# =======================
# Loop through each list_name that matched exactly
# =======================
for _, row in exact_matches.iterrows():
    list_name = row['thesauri_list_name']  # same as arches_list_name

    # Extract all concepts for this list_name
    thesauri_sub = thesauri_df.loc[thesauri_df['list_name'] == list_name]
    arches_sub = arches_df.loc[arches_df['list_name'] == list_name]

    thesauri_concepts = thesauri_sub['concept_value'].dropna().unique()
    arches_concepts = arches_sub['concept_key'].dropna().unique()

    thesauri_set = set(thesauri_concepts)
    arches_set = set(arches_concepts)

    # --- Exact matches ---
    exact_concepts = thesauri_set.intersection(arches_set)
    for concept in exact_concepts:
        t_row = thesauri_sub.loc[thesauri_sub['concept_value'] == concept].head(1)
        a_row = arches_sub.loc[arches_sub['concept_key'] == concept].head(1)

        concept_exact_matches.append({
            'list_name': list_name,
            'thesauri_concept_name': concept,
            'arches_concept_name': concept,
            'definition': t_row['definition'].values[0] if 'definition' in t_row else pd.NA,
            'list_order': t_row['list_order'].values[0] if 'list_order' in t_row else pd.NA,
            'concept_value': a_row['concept_value'].values[0] if 'concept_value' in a_row else pd.NA,
            'sortorder': a_row['sortorder'].values[0] if 'sortorder' in a_row else pd.NA
        })

    # --- Remaining concepts not matched exactly ---
    thesauri_unmatched = thesauri_set - exact_concepts
    arches_unmatched = arches_set - exact_concepts

    # Try to find close matches for thesauri_unmatched concepts
    for concept in thesauri_unmatched:
        close = get_close_matches(concept, list(arches_unmatched), n=1, cutoff=0.8)
        if close:
            concept_non_matches.append({
                'list_name': list_name,
                'thesauri_concept_name': concept,
                'arches_concept_name': close[0],
                'close_match': 'yes'
            })
            arches_unmatched.discard(close[0])
        else:
            concept_non_matches.append({
                'list_name': list_name,
                'thesauri_concept_name': concept,
                'arches_concept_name': pd.NA,
                'close_match': 'no'
            })

    # Handle arches concepts still unmatched
    for concept in arches_unmatched:
        close = get_close_matches(concept, list(thesauri_unmatched), n=1, cutoff=0.8)
        if close:
            concept_non_matches.append({
                'list_name': list_name,
                'thesauri_concept_name': close[0],
                'arches_concept_name': concept,
                'close_match': 'yes'
            })
            thesauri_unmatched.discard(close[0])
        else:
            concept_non_matches.append({
                'list_name': list_name,
                'thesauri_concept_name': pd.NA,
                'arches_concept_name': concept,
                'close_match': 'no'
            })

# =======================
# Convert results to DataFrames (one with definition to be used later and on without)
# =======================
concept_exact_df_def = pd.DataFrame(
    concept_exact_matches,
    columns=['list_name', 'thesauri_concept_name', 'arches_concept_name', 'definition', 'list_order', 'concept_value', 'sortorder']
)
concept_exact_df = pd.DataFrame(
    concept_exact_matches,
    columns=['list_name', 'thesauri_concept_name', 'arches_concept_name', 'list_order', 'concept_value', 'sortorder']
)
concept_nm_df = pd.DataFrame(
    concept_non_matches,
    columns=['list_name', 'thesauri_concept_name', 'arches_concept_name', 'close_match']
)

# =======================
# Save to Excel file
# =======================
concepts_output_path = os.path.join(data_dir, '2_Comparison/thesauri_arches_concepts_comparison.xlsx')
with pd.ExcelWriter(concepts_output_path, engine='openpyxl') as writer:
    concept_exact_df.to_excel(writer, sheet_name='concept_name_matches', index=False)
    concept_nm_df.to_excel(writer, sheet_name='concept_name_nm', index=False)

print('Concept comparison completed. Output saved to', concepts_output_path)

# =======================
# Save additional CSV (complete thesauri concepts)
# =======================
today = datetime.datetime.today().strftime("%Y%m%d")
csv_output_dir = r"D:\University of Cambridge\ARCH_MAHSA - General\MAHSA_Database\Thesauri\Thesauri_Audit\Spreadsheets\3_Complete_concepts"
os.makedirs(csv_output_dir, exist_ok=True)

csv_output_path = os.path.join(csv_output_dir, f"complete_thesauri_concepts_{today}.csv")

# Reformat exact matches dataframe
csv_export_df = concept_exact_df_def.rename(columns={'arches_concept_name': 'concept_key'})
csv_export_df = csv_export_df[['list_name', 'concept_value', 'concept_key', 'sortorder', 'list_order', 'definition']]

# Ensure list_order is numeric where possible (blanks stay NaN)
csv_export_df['list_order'] = pd.to_numeric(csv_export_df['list_order'], errors='coerce')

# Sort priority:
# 1. list_name
# 2. list_order (put non-nulls first, then nulls)
# 3. concept_value (for rows where list_order is missing)
csv_export_df = csv_export_df.sort_values(
    by=['list_name', 'list_order', 'concept_value'],
    na_position='last'
)

# Add ascending id column starting at 1
csv_export_df['id'] = range(1, len(csv_export_df) + 1)

# Save CSV
csv_export_df.to_csv(csv_output_path, index=False, encoding="utf-8-sig")
print('Complete thesauri concepts CSV saved to', csv_output_path)
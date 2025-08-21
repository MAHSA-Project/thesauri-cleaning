# =======================
# Standalone script for comparing concepts inside matching list_names
# =======================

import pandas as pd
import os
from difflib import get_close_matches

# =======================
# Define data directory (adjust path if needed)
# =======================
data_dir = os.path.join(os.getcwd(), "D:/University of Cambridge/ARCH_MAHSA - General/MAHSA_Database/Thesauri/Thesauri_Audit/Spreadsheets/")

# =======================
# Input file paths
# =======================
thesauri_path = os.path.join(data_dir, 'Processing/MAHSA_Thesauri_v4_processed_jack.csv')
arches_processed_path = os.path.join(data_dir, 'Processing/arches_thesauri_export_processed.xlsx')
list_name_matches_path = os.path.join(data_dir, 'Comparison/thesauri_arches_list_name_comparison.xlsx')

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
    thesauri_concepts = thesauri_df.loc[thesauri_df['list_name'] == list_name, 'concept_value'].dropna().unique()
    arches_concepts = arches_df.loc[arches_df['list_name'] == list_name, 'concept_key'].dropna().unique()

    # Convert to sets for easy comparison
    thesauri_set = set(thesauri_concepts)
    arches_set = set(arches_concepts)

    # --- Exact matches ---
    exact_concepts = thesauri_set.intersection(arches_set)
    for concept in exact_concepts:
        concept_exact_matches.append({
            'list_name': list_name,
            'thesauri_concept_name': concept,
            'arches_concept_name': concept
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
            # Remove the matched arches concept so it doesn't get reused
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
# Convert results to DataFrames
# =======================
concept_exact_df = pd.DataFrame(
    concept_exact_matches,
    columns=['list_name', 'thesauri_concept_name', 'arches_concept_name']
)
concept_nm_df = pd.DataFrame(
    concept_non_matches,
    columns=['list_name', 'thesauri_concept_name', 'arches_concept_name', 'close_match']
)

# =======================
# Save to Excel file
# =======================
concepts_output_path = os.path.join(data_dir, 'Comparison/concepts_comparison.xlsx')
with pd.ExcelWriter(concepts_output_path, engine='openpyxl') as writer:
    concept_exact_df.to_excel(writer, sheet_name='concept_name_matches', index=False)
    concept_nm_df.to_excel(writer, sheet_name='concept_name_nm', index=False)

print('Concept comparison completed. Output saved to', concepts_output_path)
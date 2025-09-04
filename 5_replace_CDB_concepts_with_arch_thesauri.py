import os, re
from dotenv import load_dotenv, find_dotenv
import psycopg2
import pandas as pd

# Load .env
load_dotenv(find_dotenv())

# Load concepts directory
complete_concepts_dir = r"D:\University of Cambridge\ARCH_MAHSA - General\MAHSA_Database\Thesauri\Thesauri_Audit\Spreadsheets\3_Complete_concepts"

# Read credentials
dbname = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")

# Connect to Postgres
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)
cur = conn.cursor()

# Find latest complete_thesauri_concepts_YYYYMMDD.csv
csv_pattern = re.compile(r"complete_thesauri_concepts_(\d{8})\.csv$")
csv_candidates = []
for f in os.listdir(complete_concepts_dir):
    m = csv_pattern.match(f)
    if m:
        csv_candidates.append((f, m.group(1)))
if not csv_candidates:
    raise FileNotFoundError("No complete_thesauri_concepts_*.csv found.")
csv_candidates.sort(key=lambda x: x[1])
csv_name, csv_date = csv_candidates[-1]
csv_path = os.path.join(complete_concepts_dir, csv_name)
print("Using CSV:", csv_name)

print(csv_path)

# Copy current mahsa_thesauri on the CDB to the mahsa_thesauri_backup in case something goes wrong.
# Delete current backup
cur.execute("DELETE FROM public.mahsa_thesauri_test_backup;")
conn.commit()
print("All rows deleted from mahsa_thesauri_test_backup.")

# Copy over current mahsa_thesauri_test values to backup
cur.execute("""
    INSERT INTO public.mahsa_thesauri_test_backup
    (id, concept_key, concept_value, definition, list_name, bulk_import)
    SELECT id, concept_key, concept_value, definition, list_name, bulk_import
    FROM public.mahsa_thesauri_test;
""")
conn.commit()
print("All rows copied from mahsa_thesauri_test to mahsa_thesauri_test_backup.")

# Add new concepts to mahsa_thesauri
# Step 1: Delete all existing rows from test table
cur.execute("DELETE FROM public.mahsa_thesauri_test;")
conn.commit()
print("All existing rows deleted from mahsa_thesauri_test.")

# Step 2: Load CSV
df_csv = pd.read_csv(csv_path, dtype=str)  # read all as str to avoid formatting surprises

# Step 3: Keep only the columns that match the Postgres table
df_csv = df_csv[["id", "concept_key", "concept_value", "definition", "list_name", "bulk_import"]]

# Replace NaN/empty strings with None so psycopg2 inserts NULL
df_csv = df_csv.where(pd.notnull(df_csv), None)
df_csv = df_csv.replace('', None)

# Step 4: Insert rows into test table
insert_query = """
    INSERT INTO public.mahsa_thesauri_test (id, concept_key, concept_value, definition, list_name, bulk_import)
    VALUES (%s, %s, %s, %s, %s, %s);
"""

for row in df_csv.itertuples(index=False, name=None):
    cur.execute(insert_query, row)

conn.commit()
print(f"Inserted {len(df_csv)} rows into mahsa_thesauri_test.")

# Close connection
cur.close()
conn.close()
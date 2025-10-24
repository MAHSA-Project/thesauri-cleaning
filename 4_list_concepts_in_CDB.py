import os
from dotenv import load_dotenv, find_dotenv
import psycopg2
import pandas as pd

# Load .env from the project root (find_dotenv is robust across run locations)
load_dotenv(find_dotenv())

# Read credentials
dbname = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")

# Optional: fail fast if anything is missing
missing = [k for k in ["DB_NAME","DB_USER","DB_PASSWORD","DB_HOST","DB_PORT"] if not os.getenv(k)]
if missing:
    raise RuntimeError(f"Missing required env vars: {', '.join(missing)}. "
                       f"Did you create your .env or set your Run/Debug working directory?")

# Connect
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)

# Pull the entire table public.mahsa_thesauri into a DataFrame
dfcdb = pd.read_sql("SELECT * FROM public.mahsa_thesauri;", conn)
with pd.option_context('display.max_columns', None):
    print(dfcdb.head())

# Close the connection
conn.close()

# Save the DataFrame as a CSV
output_path = r"D:\University of Cambridge\ARCH_MAHSA - General\MAHSA_Database\Thesauri\Thesauri_Audit\Spreadsheets\1_Processing\CDB_thesauri_processed.csv"
dfcdb.to_csv(output_path, index=False)

print(f"CSV saved successfully to: {output_path}")


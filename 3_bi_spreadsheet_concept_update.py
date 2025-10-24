import os, re, shutil, datetime
import pandas as pd
import xlwings as xw

bulkimport_dir = r"D:\University of Cambridge\ARCH_MAHSA - General\MAHSA_Database\Thesauri\Thesauri_Audit\Spreadsheets\4_Updated_MAHSA_BulkImport"
complete_concepts_dir = r"D:\University of Cambridge\ARCH_MAHSA - General\MAHSA_Database\Thesauri\Thesauri_Audit\Spreadsheets\3_Complete_concepts"

# 1) find latest MASTER_MAHSA_BulkImport_Template_V12_YYYYMMDD_N.xlsm
pattern = re.compile(r"MASTER_MAHSA_BulkImport_Template_V12_(\d{8})_(\d+)\.xlsm$")
candidates = []
for f in os.listdir(bulkimport_dir):
    m = pattern.match(f)
    if m:
        candidates.append((f, m.group(1), int(m.group(2))))
if not candidates:
    raise FileNotFoundError("No MASTER_MAHSA_BulkImport_Template_V12_*.xlsm files found in folder.")
candidates.sort(key=lambda x: (x[1], x[2]))
latest_file, latest_date, latest_num = candidates[-1]
latest_path = os.path.join(bulkimport_dir, latest_file)

# 2) make new filename with today's date and incremented number
today_str = datetime.datetime.today().strftime("%Y%m%d")
new_num = latest_num + 1
new_file = f"MASTER_MAHSA_BulkImport_Template_V12_{today_str}_{new_num}.xlsm"
new_path = os.path.join(bulkimport_dir, new_file)

# 3) copy the file (binary copy preserves macros, etc.)
shutil.copy2(latest_path, new_path)
print("Copied", latest_file, "->", new_file)

# 4) find latest complete_thesauri_concepts_YYYYMMDD.csv
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

df = pd.read_csv(csv_path, dtype=str)  # read all as str to avoid formatting surprises

# 5) open the copy in Excel and replace Full_DropDowns contents using xlwings
app = xw.App(visible=False)     # set visible=True if you want to watch it
try:
    wb = app.books.open(new_path)
    if "Full_DropDowns" not in [s.name for s in wb.sheets]:
        raise KeyError("Full_DropDowns sheet not found in workbook.")
    sht = wb.sheets["Full_DropDowns"]

    # Clear existing *contents* (keeps formatting). Use .clear() if you want to remove formats too.
    sht.clear_contents()

    # Write headers + data starting at A1 (index=False so pandas index isn't written)
    sht.range("A1").options(index=False).value = df

    # Force Excel to recalculate (optional but useful for dynamic arrays)
    app.api.CalculateFull()  # full recalculation

    wb.save()
    print("Updated Full_DropDowns and saved", new_file)
finally:
    wb.close()
    app.quit()
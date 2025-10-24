import subprocess
import sys

# List your scripts in order
scripts = [
    "1_listname_thes_ arch_comparison.py",
    "2_concept_thes_arch_comparison.py",
    "3_bi_spreadsheet_concept_update.py",
    "4_list_concepts_in_CDB.py",
    "5_replace_CDB_concepts_with_arch_thesauri.py",
    "6_ODK_sheet_creator.py"
]

for i, script in enumerate(scripts):
    print(f"Running {script}...")

    # Run the script
    result = subprocess.run(["python", script])

    if result.returncode != 0:
        print(f"❌ Script {script} failed with return code {result.returncode}. Exiting.")
        sys.exit(1)  # exit with non-zero code to indicate failure

    # Only ask to continue if it's not the last script
    if i < len(scripts) - 1:
        while True:
            user_input = input(f"{script} completed. Proceed to next script? (Y/N): ").strip().lower()
            if user_input == 'y':
                break
            elif user_input == 'n':
                print("Execution stopped by user.")
                sys.exit(0)
            else:
                print("Please enter Y or N.")

# If all are completed successfully print message
print("✅ All scripts completed successfully.")
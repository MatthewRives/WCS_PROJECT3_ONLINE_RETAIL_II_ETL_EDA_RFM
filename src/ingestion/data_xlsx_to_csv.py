"""
=============================================================
Convert all Excel files's tabs to individual csv files
=============================================================
Script purpose:
    This script extract each tabs from Excel files in designated folder and export each one of them in another folder. 

Process:
    01. Create a CSV folder in ../data/ if it doesn't exist already
    02. Find folder ../data/raw
    03. For each Excel (xlsx, xls) file in ../data/raw : 
        - Determine how many sheet (tab) this file has
        - For each sheet :
            - Put the content in a dataframe (df)
            - Take and clean the name of the file
            - Take and clean the name of the sheet
            - Create csv file name by concatenating clean file name and cleaned sheet name 
            - Export the df in a csv file in ../data/csv
            - If a file with a similar name exists, overwrite it
    End of process

List of functions used: 
    - None

Potential improvements: 
    - Create a clean function and call it on file and sheet name
    - Take and clean the name of the file outside the sheets' loop 

WARNING:
    Running this script will rewrite any CSV file in the folder. 
    Proceed with caution and ensure you have proper backups before running this script.
"""

# 1. Import libraries ----
print(f"\n########### Import librairies ###########")
import os
import pandas as pd
import re
from datetime import datetime, timezone
from src.utils.watermark import get_watermark, set_watermark


# Paths inside the Docker container
RAW_PATH = "/opt/airflow/data/raw"
CSV_PATH = "/opt/airflow/data/csv"


def run():
    print("\n########### xlsx_to_csv | Start ###########")

    os.makedirs(CSV_PATH, exist_ok=True)

    # Get watermark — stores the last modification time we processed
    last_run = get_watermark("ingestion_xlsx_to_csv")

    excel_files = [
        f for f in os.listdir(RAW_PATH)
        if f.endswith(".xlsx") or f.endswith(".xls")
    ]
    total_files = len(excel_files)
    print(f"Found {total_files} Excel file(s) in raw folder")

    file_counter = 0
    for file in excel_files:

        file_path = os.path.join(RAW_PATH, file)

        # Incremental check — skip file if it hasn't been modified since last run
        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path), tz=timezone.utc).isoformat()
        if last_run and file_mtime <= last_run:
            print(f"  ↷ Skipping (unchanged): {file}")
            continue

        file_counter += 1
        excel = pd.ExcelFile(file_path)
        total_sheets = len(excel.sheet_names)
        print(f"Processing file {file_counter}/{total_files}: {file} ({total_sheets} sheet(s))")

        sheet_counter = 0
        for sheet in excel.sheet_names:
            sheet_counter += 1
            print(f"  Processing sheet {sheet_counter}/{total_sheets}: {sheet}")

            df = excel.parse(sheet)

            file_name = re.sub(r'\W+', '_', os.path.splitext(file)[0].upper())
            sheet_name = re.sub(r'\W+', '_', sheet.upper())
            csv_name = f"{file_name}_{sheet_name}.csv"
            csv_path = os.path.join(CSV_PATH, csv_name)

            df.to_csv(csv_path, index=False)
            print(f"    ✓ Saved: {csv_name}")

    if file_counter == 0:
        print("No new or modified Excel files found. Skipping.")
        return

    # Update watermark to now
    set_watermark("ingestion_xlsx_to_csv", datetime.utcnow().isoformat(), "timestamp")

    print("=" * 50)
    print(f"End of CSV conversion — {file_counter} file(s) processed")
    print("=" * 50)

if __name__ == "__main__":
    run()
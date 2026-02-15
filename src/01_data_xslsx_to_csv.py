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


# 2. Fetching raw excel files ----
print(f"\n########### Fetch raw files ###########")
## Defining path --- 
script_dir = os.path.dirname(os.path.abspath(__file__))

# Build absolute paths relative to the script location
folder_path_raw = os.path.join(script_dir, "..", "data", "raw")

# Normalize paths to remove '..'
folder_path_raw = os.path.abspath(folder_path_raw)


# 3. Add xlsx & xls files to list ----
print(f"\n########### Add raw files to list ###########")
excel_files = [file for file in os.listdir(folder_path_raw) if file.endswith(".xlsx") or file.endswith(".xls")]
total_files = len(excel_files)

print(f"Found {total_files} Excel file(s) to process")


# 4. Defining csv folder ----
print(f"\n########### Defining CSV folders ###########")
folder_path_csv = os.path.join(script_dir, "..", "data", "csv")
folder_path_csv = os.path.abspath(folder_path_csv)

# Create the csv folder if it doesn't exist
os.makedirs(folder_path_csv, exist_ok=True)


# 5. Convert Excel sheets to CSV ----
print(f"\n########### Convert Excel to CSV ###########")
file_counter = 0

for file in excel_files:
    file_counter += 1
    file_path = os.path.join(folder_path_raw, file)
    
    # Count sheets in file
    excel = pd.ExcelFile(file_path)
    total_sheets = len(excel.sheet_names)
    
    print(f"Processing file {file_counter}/{total_files}: {file}")
    print(f"  Contains {total_sheets} sheet(s)")
    
    # Take excel sheet name for csv file name
    sheet_counter = 0
    for sheet in excel.sheet_names:
        sheet_counter += 1
        print(f"  Processing sheet {sheet_counter}/{total_sheets}: {sheet}")
        
        df = excel.parse(sheet)

        # Naming convention
        file_name = os.path.splitext(file)[0].upper()
        file_name = re.sub(r'\W+', '_', file_name)

        sheet_name = sheet.upper()
        sheet_name = re.sub(r'\W+', '_', sheet_name)

        # Concatenate path
        csv_name = f"{file_name}_{sheet_name}.csv"
        csv_path = os.path.join(folder_path_csv, csv_name)

        # Save sheet as CSV file
        df.to_csv(csv_path, index=False)
        print(f"    ✓ Saved: {csv_name}")

print("="*50)    
print("End of CSV conversion")
print(f"Total: {file_counter} file(s) processed in {folder_path_csv}")
print("="*50)
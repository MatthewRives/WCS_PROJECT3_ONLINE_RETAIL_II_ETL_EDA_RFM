"""
=============================================================
Create Bronze layer and insert data from csv files
=============================================================
Script purpose:
    This script creates new tables for the bronze layer (medaillon data model), in the database, and insert data from the csv files in these tables.

Layer purpose:
    The Bronze layer serves as the system of record, storing data exactly as ingested from source systems (databases, APIs, logs, IoT devices, etc.).
    Preservation of Original Data
        No transformations, filtering, or schema enforcement.
        Retains all fields, even if malformed or redundant.
    Append-Only Model
        Data is immutable — new records are added, but existing ones are never modified.
        Supports time-travel (querying historical snapshots).
    Support for Diverse Formats
        Structured (database tables), semi-structured (JSON, XML), and unstructured (text logs).

Process:
    01. Connect to the database located ../data/database (it should be named DATAWAREHOUSE_ONLINE_RETAIL_II)
    02. Fetch the csv files located in ../data/csv.
    03. Open a transaction with the DB.
        - The next step must fully succeed for the data to be committed. 
        - Otherwise, if at least one fail, everything done for previous files is rolled back
    04. For each file in the folder:
        - Create dataframe (df)
        - Clean df columns name 
        - Find the type of data in each column and create a dictionnary from it
        - Create the table name with BRONZE_{file_name}
        - If a table with a similar name exists in the DB, drop it
        - Concatenate SQL statement (table name, col names, col type)
        - Create the table with the SQL statement
        - Insert data from the file to the DB table
        - Display how many rows have been inserted
    05. Close connection
    End of process

List of functions used: 
    - fx_connect_db : connect to the database, imported from connection_to_database.py
    - fx_retrieve_csv_files : find the in csv files in ../data/csv
    - fx_process_csv_to_bronze : use other functions to import CSV to DF, clean cols, define dtypes, create table name, drop/create table, import data from csv to table
        - fx_clean_col : transform df column name with upper + letters, numbers and _ only)
        - fx_map_dtype : from a provided df column, return the dtype of the data

Potential improvements: 
    - Not determined yet
        
WARNING:
    Running this script will drop the entire 'DATAWAREHOUSE_ONLINE_RETAIL_II' database if it exists. 
    All data in the database will be permanently deleted. 
    Proceed with caution and ensure you have proper backups before running this script.
"""


# 1. Import libraries ----
print(f"\n########### Import librairies ###########")
# pip install openpyxl
import os
import pandas as pd
import re
from datetime import datetime, timezone

from src.utils.connecting_to_database import fx_connect_db
from src.utils.create_table import fx_create_table
from src.utils.watermark import get_watermark, set_watermark

CSV_PATH = "/opt/airflow/data/csv"
RFM_PATH = "/opt/airflow/data/business_inputs/rfm/RFM_SCORING.xlsx"



# 3. Define common functions ----
## Create fx_clean_col function ----
def fx_clean_col(col):
    col = col.strip().upper()
    col = re.sub(r'\W+', '_', col)
    return col


## Create fx_map_dtype function ----
def fx_map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "REAL"
    return "TEXT"


## Create fx_process_csv_to_bronze function ----
def fx_process_csv_to_bronze(csv_file, conn):
    file_path = os.path.join(CSV_PATH, csv_file)
    df = pd.read_csv(file_path)
    df.columns = [fx_clean_col(col) for col in df.columns]
    
    ### Dtype definition ----
    dtype_mapping = {col: fx_map_dtype(df[col].dtype) for col in df.columns}
    print("Column list and types:")
    for col, sql_type in dtype_mapping.items():
        print(f"  {col:30} -> {sql_type}")
    
    ### Create table ----
    table_name = fx_create_table(
        "BRONZE",
        os.path.splitext(csv_file)[0].upper(),
        df,
        dtype_mapping,
        conn
    )
    return table_name, len(df)


# ==================================================================
# LOAD CSV FILES ----
# ==================================================================
def fx_load_csv_files_to_bronze(conn):
    """Incremental load: only process CSV files modified since last run."""
    last_run = get_watermark("bronze_csv_files")

    csv_files = [f for f in os.listdir(CSV_PATH) if f.endswith(".csv")]
    total_files = len(csv_files)
    print(f"Found {total_files} CSV file(s) in {CSV_PATH}")

    file_counter = 0
    latest_mtime = last_run

    for csv_file in csv_files:
        file_path = os.path.join(CSV_PATH, csv_file)
        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path), tz=timezone.utc).isoformat()

        if last_run and file_mtime <= last_run:
            print(f"  ↷ Skipping (unchanged): {csv_file}")
            continue

        file_counter += 1
        print("-" * 40)
        print(f"  Processing {file_counter}: {csv_file}")
        print("-" * 40)

        table_name, rows = fx_process_csv_to_bronze(csv_file, conn)
        print(f"  ✓ {table_name} — {rows} rows inserted")

        # Track the most recent mtime across all processed files
        if latest_mtime is None or file_mtime > latest_mtime:
            latest_mtime = file_mtime

    if file_counter == 0:
        print("No new or modified CSV files. Skipping.")
        return

    set_watermark("bronze_csv_files", latest_mtime, "timestamp")
    print(f"\n  Watermark updated to: {latest_mtime}")
    
    
# ==================================================================
# RFM table creation ----
# ==================================================================
def fx_load_rfm_mapping_to_bronze(conn):
    """RFM mapping — reloads only if the Excel file changed since last run."""
    last_run = get_watermark("bronze_rfm_mapping")

    file_mtime = datetime.fromtimestamp(os.path.getmtime(RFM_PATH), tz=timezone.utc).isoformat()

    if last_run and file_mtime <= last_run:
        print("  ↷ RFM mapping unchanged. Skipping.")
        return

    df = pd.read_excel(RFM_PATH, engine='openpyxl')
    df.columns = [fx_clean_col(col) for col in df.columns]

    dtype_mapping = {
        'RFM_SCORE': 'INTEGER',
        'RFM_SEGMENT': 'TEXT',
        'RFM_NAME': 'TEXT'
    }

    fx_create_table('BRONZE', 'RFM_MAPPING', df, dtype_mapping, conn)
    set_watermark("bronze_rfm_mapping", file_mtime, "timestamp")
    print("  ✓ BRONZE_RFM_MAPPING loaded and watermark updated.")


# ==================================================================
# Create bronze layer tables ----
# ==================================================================
def run():
    print("\n########### script_layer_bronze | Start ###########")
    try:
        conn = fx_connect_db()
        with conn:
            fx_load_csv_files_to_bronze(conn)
            fx_load_rfm_mapping_to_bronze(conn)

        print("=" * 50)
        print("Bronze layer completed successfully.")
        print("=" * 50)

    except Exception as error:
        print(f"Error: {error}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run()
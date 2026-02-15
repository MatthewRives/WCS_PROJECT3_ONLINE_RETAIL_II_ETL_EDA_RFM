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

import sqlite3
import pandas as pd
import re
import openpyxl

from module_connecting_to_database import *
from module_create_table import *


# 2. Connect to database ----
print(f"\n########### Connect to database ###########")
conn = fx_connect_db()
cursor = conn.cursor()


# 3. Define common functions ----
print(f"\n########### Common function ###########")
## Create fx_clean_col function ----
def fx_clean_col(col):
    col = col.strip().upper()
    col = re.sub(r'\W+', '_', col)
    return col


# 4. Import sales raw data to bronze layer ----
print(f"\n########### Import sales data ###########")
## Create fx_retrieve_csv_files function ----
def fx_retrieve_csv_files():
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Build absolute paths relative to the script location
    folder_path_csv = os.path.join(script_dir, "..", "data", "csv")

    # Normalize paths to remove '..'
    folder_path_csv = os.path.abspath(folder_path_csv)

    # Count total csv files
    csv_files = [file for file in os.listdir(folder_path_csv) if file.endswith(".csv")]
    return csv_files, folder_path_csv


## Create fx_map_dtype function ----
def fx_map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "REAL"
    return "TEXT"


## Create fx_process_csv_to_bronze function ----
def fx_process_csv_to_bronze(csv_file, folder_path_csv, conn):
    """Traite un fichier CSV : création table + insertion données"""
    
    ### Define file path ----
    file_path = os.path.join(folder_path_csv, csv_file)
    
    ### Read CSV File ----
    df = pd.read_csv(file_path)
    
    ### Clean cols name ----
    df.columns = [fx_clean_col(col) for col in df.columns]
    
    ### Dtype definition ----
    dtype_mapping = {}
    print("Column list and type: ")
    for col in df.columns:
        sql_type = fx_map_dtype(df[col].dtype)
        dtype_mapping[col] = sql_type
        print(f"  Column: {col:20} -> Type: {sql_type:10}")

    ### Define name table ----
    table_name = f"BRONZE_{os.path.splitext(csv_file)[0].upper()}"
    
    ### Drop existing table if exists ----
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    print(f"\n  Dropped existing table (if any): {table_name}")

    ### Create sql statement ----
    cols_sql = ", ".join([f"{col} {dtype_mapping[col]}" for col in df.columns])
    sql_statement = f"CREATE TABLE {table_name} ({cols_sql})"
    print(f"\n  SQL: {sql_statement}")

    ### Create table ----
    cursor.execute(sql_statement)
    print(f"\n  Table created: {table_name}")

    ### Insert data ----
    df.to_sql(
        name = table_name,
        con = conn,
        if_exists = "append",
        index = False
    )

    rows_inserted = len(df)
    print(f"\n  Inserted {rows_inserted} rows")

    return table_name


## Retrieve csv files ----
try:
    csv_files, folder_path = fx_retrieve_csv_files()

    total_files = len(csv_files)
    print(f"Found {total_files} csv file(s) to process")
    file_counter = 0

    with conn: # automatic commit or rollback for the enclosed operations
        for csv_file in csv_files:

            file_counter += 1
            print("-"*20) 
            print(f"  Processing file {file_counter}/{total_files}: {csv_file}")
            print("-"*20) 
     
            
            ## Call fx_process_csv_to_bronze ----
            # CSV to DF, Clean cols, dtype definition, table name, drop/create table, import data
            table = fx_process_csv_to_bronze(csv_file, folder_path, conn)
            print(f"✓ Table {table} created and fulfilled with data from csv {csv_file}.")
    
    print("="*50)    
    print("All files processed successfully!")
    print("="*50)

# Error management
except Exception as error:
    print(f"Error: {error}")
    import traceback
    traceback.print_exc()
    

# 5. Import RFM business inputs ----
print(f"\n########### Import RFM mapping ###########")
## Get excel file ----
### Defining path --- 
script_dir = os.path.dirname(os.path.abspath(__file__))

### Build absolute paths relative to the script location ----
folder_path_rfm = os.path.join(script_dir, "..", "data", "business_inputs", "rfm")

### Normalize paths to remove '..' ----
folder_path_rfm = os.path.abspath(folder_path_rfm)

### Read excel file
file_name = "RFM_SCORING.xlsx"
file_path = os.path.join(folder_path_rfm, file_name)
df_rfm_mapping = pd.read_excel(file_path, engine='openpyxl')


## Clean columns ----
df_rfm_mapping.columns = [fx_clean_col(col) for col in df_rfm_mapping.columns]


## Map dtype ----
dtype_mapping = {
        'RFM_SCORE': 'INTEGER', 
        'RFM_SEGMENT': 'TEXT',
        'RFM_NAME':'TEXT'
        }

## Create BRONZE_RFM_MAPPING table ----
create_bronze_rfm_mapping = fx_create_table('BRONZE', 'RFM_MAPPING', df_rfm_mapping, dtype_mapping, conn)
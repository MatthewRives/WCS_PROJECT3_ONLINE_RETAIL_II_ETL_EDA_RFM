"""
=============================================================
Create Database and Schemas
=============================================================
Script purpose:
    This script creates a new database named 'ONLINE_RETAIL_II_DATAWAREHOUSE' after checking if it already exists. 
    If the database exists, it is dropped and recreated. 

Process:
    01. Create a 'database' folder in ../datasets/ if it doesn't exists
    02. Create the database ONLINE_RETAIL_II_DATAWAREHOUSE.db in this folder
    03. Connect to the database
    End of process

List of functions used: 
    - None

Potential improvements: 
    - Not determined yet

WARNING:
    Running this script will drop the entire 'DataWarehouse_Online_Retail_II' database if it exists. 
    All data in the database will be permanently deleted. 
    Proceed with caution and ensure you have proper backups before running this script.
"""

# 1. Import libraries ----
import os
import sqlite3


# 2. Creating database ----
try:
    ## Define filepath ----
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Build absolute paths relative to the script location
    folder_path_database = os.path.join(script_dir, "..", "datasets", "database")

    # Normalize paths to remove '..'
    folder_path_database = os.path.abspath(folder_path_database)
    
    ## Creating folder if needed ----
    os.makedirs(folder_path_database, exist_ok=True)

    ## Concatenate path and file name ----
    db_name = "ONLINE_RETAIL_II_DATAWAREHOUSE.db"
    db_path = os.path.join(folder_path_database, db_name)

    ## Create database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("="*50)    
    print(f"Database created/connected at: {db_path}")
    print("="*50)

# 3. Error management ----
except Exception as error:
    print(f"Error: {error}")
    import traceback
    traceback.print_exc()
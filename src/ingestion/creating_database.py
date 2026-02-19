"""
=============================================================
Create Database and Schemas
=============================================================
Script purpose:
    This script creates a new database named 'ONLINE_RETAIL_II_DATAWAREHOUSE' after checking if it already exists. 
    If the database exists, it is dropped and recreated. 

Process:
    01. Create a 'database' folder in ../data/ if it doesn't exists
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
print(f"\n########### Import librairies ###########")
import os

import sqlite3


DB_PATH = "/opt/airflow/data/database/DATAWAREHOUSE_ONLINE_RETAIL_II.db"

def run():
    print("\n########### create_database | Start ###########")
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

        conn = sqlite3.connect(DB_PATH)
        conn.close()

        print("=" * 50)
        print(f"Database created/connected at: {DB_PATH}")
        print("=" * 50)

    except Exception as error:
        print(f"Error: {error}")
        import traceback
        traceback.print_exc()
        raise  # Re-raise so Airflow marks the task as failed

if __name__ == "__main__":
    run()
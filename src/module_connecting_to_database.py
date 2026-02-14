"""
=============================================================
Function: Connecting to database
=============================================================
Script purpose:
    ...

Process:
    01. ...
    End of process

List of functions used: 
    - ...

Potential improvements: 
    - Not determined yet
        
WARNING:
    ...
"""




import os
import sqlite3

def fx_connect_db():
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Build absolute paths relative to the script location
    folder_path_database = os.path.join(script_dir, "..", "datasets", "database")

    # Normalize paths to remove '..'
    folder_path_database = os.path.abspath(folder_path_database)
    print(folder_path_database)

    # Get database files
    database_list = [file for file in os.listdir(folder_path_database) if file.endswith(".db")]
    print(f"Database list: {database_list}")

    # If several db files exist, it's an error.
    if len(database_list) > 1:
        print("To many database file. Please correct it.")
        return None

    # If no db exist, it's an error.    
    if len(database_list) == 0:
        print("No database file found.")
        return None

    # Select the only database available
    database_file = database_list[0]
    print(f"Database file selected: {database_file}")

    # Create the full path to the database file
    db_path = os.path.join(folder_path_database, database_file)

    # Connect to the database
    conn = sqlite3.connect(db_path)

    print("="*50)    
    print(f"Database connected at: {db_path}")
    print("="*50)
    
    return conn
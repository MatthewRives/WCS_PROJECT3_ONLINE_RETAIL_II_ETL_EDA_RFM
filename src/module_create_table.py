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

Exemple of use: 
    df_exchange_rate = 

    dtype_mapping = {
        'STOCKCODE': 'TEXT', 
        'DESCRIPTION_RAW': 'TEXT',
        'PRODUCT_NAME':'TEXT'
        }

    create_silver_exchange_rate = fx_create_table('SILVER', 'EXCHANGE_RATE', df_exchange_rate, dtype_mapping, conn)

"""

# 1. Import librairies ----
import re
import sqlite3

from module_connecting_to_database import *



# 2. Create fx_create_table function ----
def fx_create_table(layer_name, table_name, df, dtype_mapping, conn):
    
    layer_name = re.sub(r'\W+', '_', layer_name.upper().strip())
    table_name = re.sub(r'\W+', '_', table_name.upper().strip())
    full_name = f"{layer_name}_{table_name}"

    print(f"\n########### Creating {full_name} table ###########")
    
    # Drop existing table if exists
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {full_name}")
    print(f"\n  Dropped existing table (if any): {full_name}")

    # Create sql statement
    print(f"\n---------- Concatenating SQL statement ----------")
    cols_sql = ", ".join([f"{col} {dtype}" for col, dtype in dtype_mapping.items()])
    sql_statement = f"CREATE TABLE {full_name} ({cols_sql})"
    print(f"\n  SQL: {sql_statement}")

    # Create table
    cursor.execute(sql_statement)
    print(f"\n  Table created: {full_name}")

    # Insert data
    df.to_sql(
        name=full_name,
        con=conn,
        if_exists="append",
        index=False,
        dtype=dtype_mapping
    )

    rows_inserted = len(df)
    print(f"\n  Inserted {rows_inserted} rows")

    return full_name
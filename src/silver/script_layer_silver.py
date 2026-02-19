"""
=============================================================
Create Silver layer and insert transformed data from bronze layer
=============================================================
Script purpose:
    This script creates new tables for the silver layer (medaillon data model), in the database, and insert transformed data from the bronze layer in these tables.

Layer purpose:
    The Silver layer standardizes and enriches data, resolving inconsistencies and preparing it for analytics.
    Join Operations
        Combining datasets (e.g., linking user IDs to profiles).
    Data Cleansing
        Handling missing values (NULL → default values).
        Standardizing formats (e.g., dates as YYYY-MM-DD).
    Deduplication
        Removing duplicate records using keys or timestamps.
    Schema Enforcement
        Defining strict schemas (column types, constraints).


Process:
    01. Connect to the database located ../data/database (it should be named DATAWAREHOUSE_ONLINE_RETAIL_II)
    02. 
    End of process

List of functions used: 
    - fx_connect_db : connect to the database, imported from connection_to_database.py
    - 

Potential improvements: 
    - Not determined yet
        
WARNING:

"""

import pandas as pd
from datetime import datetime, timezone

from src.utils.connecting_to_database import fx_connect_db
from src.utils.create_table import fx_create_table
from src.utils.watermark import get_watermark, set_watermark



# ── Cleaning functions ─────────────────────────────────

## Drop duplicates ----
def fx_clean_duplicates(df):
    df = df.drop_duplicates()
    return df


## Clean invoices ----
def fx_clean_invoice(df):
    print(f"\n────────── Clean INVOICE column ──────────")
    df["INVOICE"] = df["INVOICE"].str.strip().str.upper()
    df['INVOICE'] = df['INVOICE'].str.replace(r'\s+', '_', regex=True)
    df['INVOICE'] = df['INVOICE'].fillna("UNKNOWN")
    return df


## Clean stockcode ----
def fx_clean_stockcode(df):
    print(f"\n───── Clean STOCKCODE column ─────")
    df['STOCKCODE'] = df["STOCKCODE"].str.strip().str.upper()
    df['STOCKCODE'] = df['STOCKCODE'].str.replace(r'\s+', '_', regex=True)
    df['STOCKCODE'] = df['STOCKCODE'].fillna("UNKNOWN")
    return df


## Clean description ----
def fx_clean_description(df):
    print(f"\n───── Clean DESCRIPTION column ─────")
    print(f"  Before: {df['DESCRIPTION'].nunique()} unique values")
    df['DESCRIPTION'] = df['DESCRIPTION'].str.strip().str.upper()
    df['DESCRIPTION'] = df['DESCRIPTION'].str.replace(r'\s+', '_', regex=True)
    df['DESCRIPTION'] = df['DESCRIPTION'].fillna("UNKNOWN")
    print(f"  After: {df['DESCRIPTION'].nunique()} unique values")
    return df


## Clean quantity ----
def fx_clean_quantity(df):
    print(f"\n───── Clean QUANTITY column ─────")
    df['QUANTITY'] = pd.to_numeric(df['QUANTITY'], errors='coerce')
    return df


## Clean invoice_date ----
def fx_clean_invoicedate(df):
    print(f"\n───── Clean DATE column ─────")
    df['INVOICEDATE'] = pd.to_datetime(df['INVOICEDATE'], errors='coerce')
    df['INVOICE_DATE'] = df['INVOICEDATE'].dt.strftime('%Y-%m-%d')
    df['INVOICE_TIME'] = df['INVOICEDATE'].dt.strftime('%H:%M:%S')
    df = df.drop(columns=['INVOICEDATE'])
    return df


## Clean price ----
def fx_clean_price(df):
    print(f"\n───── Clean PRICE column ─────")
    df['PRICE'] = pd.to_numeric(df['PRICE'], errors='coerce')
    return df


## Clean customer id ----
def fx_clean_customer_id(df):
    print(f"\n───── Clean CUSTOMER_ID column ─────")
    df['CUSTOMER_ID'] = df['CUSTOMER_ID'].fillna("UNKNOWN")
    return df


## Clean country ----
def fx_clean_country(df):
    print(f"\n───── Clean COUNTRY column ─────")
    df['COUNTRY'] = df['COUNTRY'].str.strip().str.upper()
    df['COUNTRY'] = df['COUNTRY'].str.replace(r'\s+', '_', regex=True)
    df['COUNTRY'] = df['COUNTRY'].str.replace('UNSPECIFIED', 'UNKNOWN')
    df['COUNTRY'] = df['COUNTRY'].fillna('UNKNOWN')
    return df


## Mapping return vs sales ----
def fx_mapping_return_sales(df):
    print(f"\n───── Map Return vs Sales ─────")
    df['INVOICE_TYPE'] = df.apply(
        lambda x: 'RETURN' 
        if (x['QUANTITY'] < 0) or (not x['INVOICE'].isdigit()) 
        else 'SALE', 
        axis=1)
    return df



# ── Silver Sales ─────────────────────────────────────────────────

def fx_load_silver_sales(conn):
    print("\n########### Silver Sales ###########")
    
    last_run = get_watermark("silver_sales")
    
    # Load bronze sales tables
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name 
        FROM sqlite_master
        WHERE type='table' AND name LIKE 'BRONZE_ONLINE_RETAIL%'
        ORDER BY name
    """)


    bronze_tables = [row[0] for row in cursor.fetchall()]
    print(f"Tables studied: {bronze_tables}")


    # Create df from each table and add it to list ----
    print(f"\n───── Create df from tables ─────")
    df_list = []
    for table in bronze_tables:
        df_list.append(pd.read_sql_query(f'SELECT * FROM "{table}"', conn))
        
    df_sales = pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()


    # Incremental filter on INVOICEDATE ----
    df_sales["INVOICEDATE"] = pd.to_datetime(df_sales["INVOICEDATE"], errors="coerce")
    if last_run:
        before = len(df_sales)
        df_sales = df_sales[df_sales["INVOICEDATE"] > pd.to_datetime(last_run)]
        print(f"  Incremental filter: {before} → {len(df_sales)} rows (after {last_run})")
        
    if df_sales.empty:
        print("  No new sales data. Skipping.")
        return
    
    # Transformations
    df = df_sales.copy()
    df = fx_clean_duplicates(df)
    df = fx_clean_invoice(df)
    df = fx_clean_stockcode(df)
    df = fx_clean_description(df)
    df = fx_clean_quantity(df)
    df = fx_clean_invoicedate(df)
    df = fx_clean_price(df)
    df = fx_clean_customer_id(df)
    df = fx_clean_country(df)
    df = fx_mapping_return_sales(df)
    
    dtype_mapping = {
        "INVOICE":      "TEXT",
        "STOCKCODE":    "TEXT",
        "DESCRIPTION":  "TEXT",
        "QUANTITY":     "INTEGER",
        "PRICE":        "REAL",
        "CUSTOMER_ID":  "TEXT",
        "COUNTRY":      "TEXT",
        "INVOICE_DATE": "TEXT",
        "INVOICE_TIME": "TEXT",
        "INVOICE_TYPE": "TEXT"
    }
    
    fx_create_table("SILVER", "SALES", df, dtype_mapping, conn)

    new_watermark = df_sales["INVOICEDATE"].max().isoformat()
    set_watermark("silver_sales", new_watermark, "timestamp")
    print(f"  ✓ SILVER_SALES created — {len(df)} rows. Watermark: {new_watermark}")
    
    
# ── Silver RFM Mapping ───────────────────────────────────────────

def fx_load_silver_rfm_mapping(conn):
    print("\n########### Silver RFM Mapping ###########")

    last_run = get_watermark("silver_rfm_mapping")

    # Check if bronze rfm mapping changed
    bronze_mtime = datetime.fromtimestamp(
        conn.execute(
            "SELECT MAX(rowid) FROM BRONZE_RFM_MAPPING"
        ).fetchone()[0] or 0,
        tz=timezone.utc
    ).isoformat()

    if last_run and bronze_mtime <= last_run:
        print("  ↷ RFM mapping unchanged. Skipping.")
        return

    df = pd.read_sql_query('SELECT * FROM "BRONZE_RFM_MAPPING"', conn)
    df = fx_clean_duplicates(df)

    dtype_mapping = {
        "RFM_SCORE":   "INTEGER",
        "RFM_SEGMENT": "TEXT",
        "RFM_NAME":    "TEXT"
    }

    fx_create_table("SILVER", "RFM_MAPPING", df, dtype_mapping, conn)

    set_watermark("silver_rfm_mapping",
                  datetime.now(tz=timezone.utc).isoformat(), "timestamp")
    print(f"  ✓ SILVER_RFM_MAPPING created — {len(df)} rows.")


# ── Entry point ──────────────────────────────────────────────────

def run():
    print("\n########### script_layer_silver | Start ###########")
    try:
        conn = fx_connect_db()
        with conn:
            fx_load_silver_sales(conn)
            fx_load_silver_rfm_mapping(conn)

        print("=" * 50)
        print("Silver layer completed successfully.")
        print("=" * 50)

    except Exception as error:
        print(f"Error: {error}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run()
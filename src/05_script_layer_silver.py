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
    01. Connect to the database located ../datasets/database (it should be named DATAWAREHOUSE_ONLINE_RETAIL_II)
    02. 
    End of process

List of functions used: 
    - fx_connect_db : connect to the database, imported from connection_to_database.py
    - 

Potential improvements: 
    - Not determined yet
        
WARNING:

"""

# 1. Import library ----
import pandas as pd

from module_create_table import *
from module_connecting_to_database import *


# 2. Connect to database ----
conn = fx_connect_db()
cursor = conn.cursor()


# 3. Get BRONZE tables ----
cursor.execute("""
    SELECT name
    FROM sqlite_master
    WHERE type='table' AND name LIKE 'BRONZE_%'
    ORDER BY name;
    """
    )

tables = [row[0] for row in cursor.fetchall()]
print(f"Tables studied: {tables}")


# 4. Create a df from each table and add it to list ----
df_list = []
for table in tables:
    query = f'SELECT * FROM "{table}"'
    df_query = pd.read_sql_query(query, conn)
    df_list.append(df_query)


# 5. Concatenate the dfs from the df list ----
df = pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()


# 6. Check columns ----
col_list = df.columns.tolist()
print(f"Column list of df: {col_list}")
# ['INVOICE', 'STOCKCODE', 'DESCRIPTION', 'QUANTITY', 'INVOICEDATE', 'PRICE', 'CUSTOMER_ID', 'COUNTRY']


# 7. Define transformation functions ----
## Drop duplicates ----
def fx_clean_duplicates(df):
    df = df.drop_duplicates()
    return df

## Clean invoices ----
def fx_clean_invoice(df):
    print(f"\n---------- Clean INVOICE column ----------")
    df["INVOICE"] = df["INVOICE"].str.strip()
    df["INVOICE"] = df["INVOICE"].str.upper()
    df['INVOICE'] = df['INVOICE'].str.replace(r'\s+', '_', regex=True)
    df['INVOICE'] = df['INVOICE'].fillna("UNKNOWN")
    return df


## Clean stockcode ----
def fx_clean_stockcode(df):
    print(f"\n---------- Clean STOCKCODE column ----------")
    df['STOCKCODE'] = df["STOCKCODE"].str.strip()
    df['STOCKCODE'] = df["STOCKCODE"].str.upper()
    df['STOCKCODE'] = df['STOCKCODE'].str.replace(r'\s+', '_', regex=True)
    df['STOCKCODE'] = df['STOCKCODE'].fillna("UNKNOWN")
    return df


## Clean description ----
def fx_clean_description(df):
    print(f"\n---------- Clean DESCRIPTION column ----------")
    print(f"Before cleaning, there is {df['DESCRIPTION'].nunique()} unique values in DESCRIPTION column")
    
    df['DESCRIPTION'] = df['DESCRIPTION'].str.strip()
    df['DESCRIPTION'] = df['DESCRIPTION'].str.upper()
    df['DESCRIPTION'] = df['DESCRIPTION'].str.replace(r'\s+', '_', regex=True)
    df['DESCRIPTION'] = df['DESCRIPTION'].fillna("UNKNOWN")
    
    print(f"After fillna, there is {df['DESCRIPTION'].nunique()} unique values in DESCRIPTION column")

    return df


## Clean quantity ----
def fx_clean_quantity(df):
    print(f"\n---------- Clean QUANTITY column ----------")
    df['QUANTITY'] = pd.to_numeric(df['QUANTITY'], errors='coerce')
    return df


## Clean invoice_date ----
def fx_clean_invoicedate(df):
    print(f"\n---------- Clean DATE column ----------")
    df['INVOICEDATE'] = pd.to_datetime(df['INVOICEDATE'], errors='coerce')
    df['INVOICE_DATE'] = df['INVOICEDATE'].dt.strftime('%Y-%m-%d')
    df['INVOICE_TIME'] = df['INVOICEDATE'].dt.strftime('%H:%M:%S')
    df = df.drop(columns=['INVOICEDATE'])
    return df


## Clean price ----
def fx_clean_price(df):
    print(f"\n---------- Clean PRICE column ----------")
    df['PRICE'] = pd.to_numeric(df['PRICE'], errors='coerce')
    return df


## Clean customer id ----
def fx_clean_customer_id(df):
    print(f"\n---------- Clean CUSTOMER_ID column ----------")
    df['CUSTOMER_ID'] = df['CUSTOMER_ID'].fillna("UNKNOWN")
    return df


## Clean country ----
def fx_clean_country(df):
    print(f"\n---------- Clean COUNTRY column ----------")
    df['COUNTRY'] = df['COUNTRY'].str.strip()
    df['COUNTRY'] = df['COUNTRY'].str.upper()
    df['COUNTRY'] = df['COUNTRY'].str.replace(r'\s+', '_', regex=True)
    df['COUNTRY'] = df['COUNTRY'].str.replace('UNSPECIFIED', 'UNKNOWN')
    df['COUNTRY'] = df['COUNTRY'].fillna('UNKNOWN')
    return df


## Mapping return vs sales ----
def fx_mapping_return_sales(df):
    print(f"\n---------- Map Return vs Sales ----------")
    df['INVOICE_TYPE'] = df.apply(
    lambda x: 
        'RETURN' if (x['QUANTITY'] < 0) or (not x['INVOICE'].isdigit()) 
        else 'SALE', axis=1)
    return df


# 8. Call transformation functions ----
clean_df = df.copy()
clean_df = fx_clean_duplicates(clean_df)
clean_df = fx_clean_invoice(clean_df)
clean_df = fx_clean_stockcode(clean_df)
clean_df = fx_clean_description(clean_df)   
clean_df = fx_clean_quantity(clean_df)
clean_df = fx_clean_invoicedate(clean_df)
clean_df = fx_clean_price(clean_df)
clean_df = fx_clean_customer_id(clean_df)
clean_df = fx_clean_country(clean_df)
clean_df = fx_mapping_return_sales(clean_df)
df_silver_sales = clean_df


# 9. Create Silver Sales Table ----

## Check columns ----
col_list = df_silver_sales.columns.tolist()
print(f"Column list of cleaned df: {col_list}")
# ['INVOICE', 'STOCKCODE', 'DESCRIPTION', 'QUANTITY', 'PRICE', 'CUSTOMER_ID', 'COUNTRY', 'INVOICE_DATE', 'INVOICE_TIME', 'INVOICE_TYPE']


## Mapping dtype ----
dtype_mapping = {
    'INVOICE': 'TEXT', 
    'STOCKCODE': 'TEXT', 
    'DESCRIPTION': 'TEXT', 
    'QUANTITY': 'INTEGER', 
    'PRICE': 'REAL', 
    'CUSTOMER_ID': 'INTEGER', 
    'COUNTRY': 'TEXT', 
    'INVOICE_DATE': 'TEXT', 
    'INVOICE_TIME': 'TEXT',
    'INVOICE_TYPE' : 'TEXT'
    }


## Call fx_create_table ----
create_silver_sales_table = fx_create_table("SILVER", "ONLINE_RETAIL_II", df_silver_sales, dtype_mapping, conn)
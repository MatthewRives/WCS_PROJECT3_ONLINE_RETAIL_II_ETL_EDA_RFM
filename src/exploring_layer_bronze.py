"""
=============================================================
Explore Bronze tables
=============================================================
Script purpose:
    This script explores the tables and their data from the bronze layer. Allows us to evaluate which transformation are required for the silver layer. 

Process:
    01. Connect to the database located ../data/database (it should be named DATAWAREHOUSE_ONLINE_RETAIL_II)
    02. Get all tables in the DB starting with "BRONZE_" and add them to a table list
    03. For each table in the table list:
        - Create a dataframe (DF) from the table
        - Add the DF to a DF list
    04. Concatenate the DFs in the DF list
    05. Close connection with DB
    06. Define functions for specific exploration
    07. Call the generic exploration function, that will store several DF and graphs in a generic exploration dictionnary
    08. Call the specific exploration function, that will store several DF in a specific exploration dictionnary
    09. Merge the results of the two exploration in a single dictionnary
    10. Export the exploration results to an Excel file in ../data/data_exploration (it should be named bronze_data_exploration.xlsx), with a sheet per DF or graph.
    End of process

List of functions used: 
    - fx_connect_db : connect to the database, imported from connection_to_database.py
    - fx_df_exploration : 
        - fx_share_of_value : 
        - fx_col_fulfilling_rate : 
        - fx_need_trim : 
    - fx_data_exploration : 
    - fx_df_exploration_variance_graph : 
    - fx_correlation_matrix_plot : 

Potential improvements: 
    - Export graphs to folder ../_img to insert them in the documentation
        
WARNING:
    ...
"""

# 1. Import librairies ----
print(f"\n########### Import librairies ###########")
import sqlite3
from pyparsing import col
import pandas as pd
import datetime as dt

from module_connecting_to_database import *
from module_data_exploration import *
from module_export_data_to_xlsx import *


# 2. Connect to database ----
print(f"\n########### Connect to database ###########")
conn = fx_connect_db()
cursor = conn.cursor()


# 3. Find all Bronze tables in database ----
print(f"\n########### Import bronze tables ###########")
cursor.execute("""
SELECT name
FROM sqlite_master
WHERE type='table' AND name LIKE 'BRONZE_ONLINE_RETAIL%'
ORDER BY name;
""")

tables = [row[0] for row in cursor.fetchall()]
print(f"Tables studied: {tables}")


# 4. Create a df from each table and add it to list ----
print(f"\n########### Create DF ###########")
df_list = []
for table in tables:
    query = f'SELECT * FROM "{table}"'
    df_query = pd.read_sql_query(query, conn)
    df_list.append(df_query)


# 5. Concatenate the dfs from the df list ----
print(f"\n########### Concatenate df ###########")
df = pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()


# 6. List of specific exploration function ----
## Clean invoice ----
def fx_clean_invoice(df):
    print(f"\n───── Clean INVOICE column ─────")
    df["INVOICE"] = df["INVOICE"].str.strip()
    df["INVOICE"] = df["INVOICE"].str.upper()
    df['INVOICE'] = df['INVOICE'].str.replace(r'\s+', '_', regex=True)
    df['INVOICE'] = df['INVOICE'].str.replace(r'\s+', '_', regex=True)
    df['INVOICE_NUM'] = df['INVOICE'].str.extract(r'(\d+)', expand=False)
    df['INVOICE_ALPHA'] = df['INVOICE'].str.extract(r'([A-Za-z]+)', expand=False)
    return df


## Clean stockcode ----
def fx_clean_stockcode(df):
    print(f"\n───── Clean STOCKCODE column ─────")
    df['STOCKCODE'] = df["STOCKCODE"].str.strip()
    df['STOCKCODE'] = df["STOCKCODE"].str.upper()
    df['STOCKCODE'] = df['STOCKCODE'].str.replace(r'\s+', '_', regex=True)
    df['STOCKCODE'] = df['STOCKCODE'].str.replace(r'\s+', '_', regex=True)
    df['STOCKCODE_NUM'] = df['STOCKCODE'].str.extract(r'(\d+)', expand=False)
    df['STOCKCODE_ALPHA'] = df['STOCKCODE'].str.extract(r'([A-Za-z]+)', expand=False)
    return df


## Clean description ----
def fx_clean_description(df):
    print(f"\n───── Clean DESCRIPTION column ─────")
    print(f"Before cleaning, there is {df['DESCRIPTION'].nunique()} unique values in DESCRIPTION column")

    df['DESCRIPTION'] = df['DESCRIPTION'].str.replace('[^0-9a-zA-Z\s]+', ' ', regex=True)
    df['DESCRIPTION'] = df['DESCRIPTION'].str.strip()
    df['DESCRIPTION'] = df['DESCRIPTION'].str.upper()
    df['DESCRIPTION'] = df['DESCRIPTION'].str.replace(r'\s+', '_', regex=True)

    print(f"After cleaning, there is {df['DESCRIPTION'].nunique()} unique values in DESCRIPTION column")
    return df


## Split potential return from sales ----
def fx_split_return_from_sales(df):
    print(f"\n───── Split sales and returns ─────")
    # Create a df dedicated to return : invoice with C or negative quantity
    is_return = (df["QUANTITY"] < 0) | (df["INVOICE"].str.contains(r"[A-Za-z]", na=False))
    df_return = df[is_return].sort_values(by = 'INVOICE')
    df_sales  = df[~is_return].sort_values(by = 'INVOICE')
    return df_return, df_sales


## List description name per stockcode ----
def fx_description_name_per_stockcode(df_clean):
    print(f"\n───── Description name per stockcode ─────")
    df_description_name_per_stockcode = df_clean[['STOCKCODE', 'DESCRIPTION']].drop_duplicates()
    df_description_name_per_stockcode = df_description_name_per_stockcode.sort_values(by = ['STOCKCODE', 'DESCRIPTION'])
    return df_description_name_per_stockcode


## Count description per stockcode ----
def fx_description_count_per_stockcode(df_clean):
    print(f"\n───── Description count per stockcode ─────")
    df_description_count_per_stockcode = df_clean.groupby('STOCKCODE').agg(
    {"PRICE": pd.Series.nunique,
    "DESCRIPTION": pd.Series.nunique})
    df_description_count_per_stockcode = df_description_count_per_stockcode.sort_values(by = 'STOCKCODE')
    return df_description_count_per_stockcode


## Count description per stockcode and country ----
def fx_description_count_per_stockcode_per_country(df_clean):
    print(f"\n───── Description count per stockcode per country ─────")
    df_description_count_per_stockcode_per_country = df_clean.groupby(['STOCKCODE', 'COUNTRY']).agg(
    {"PRICE": pd.Series.nunique,
    "DESCRIPTION": pd.Series.nunique})
    df_description_count_per_stockcode_per_country = df_description_count_per_stockcode_per_country.sort_values(by = ['STOCKCODE', 'COUNTRY'])
    return df_description_count_per_stockcode_per_country


# 7. Regroup specific exploration functions ----
def fx_specific_exploration(df):
    print(f"\n########### Specific exploration ###########")
    df_clean = df.copy()
    df_clean = fx_clean_invoice(df_clean)
    df_clean = fx_clean_stockcode(df_clean)
    df_clean = fx_clean_description(df_clean)

    # RETURN, SALES
    df_return, df_sales = fx_split_return_from_sales(df_clean)
    
    # DESC COUNT STOCKCODE
    df_description_count_per_stockcode = fx_description_count_per_stockcode(df_clean)

    # DESC NAME STOCKCODE
    df_description_name_per_stockcode = fx_description_name_per_stockcode(df_clean)

    # DESC COUNT PER CODE COUNTRY
    df_description_count_per_stockcode_per_country = fx_description_count_per_stockcode_per_country(df_clean)

    print(f"\n########### Specific exploration result in dictionnary ###########")
    dictionnary_specific_exploration = {
        "Return": df_return,
        "Sales": df_sales,
        "Desc_count_stockcode": df_description_count_per_stockcode,
        "Desc_name_stockcode": df_description_name_per_stockcode,
        "Desc_count_per_code_country": df_description_count_per_stockcode_per_country
    }
    return dictionnary_specific_exploration


# 8. Call generic and specific exploration functions ----
print(f"\n########### Call exploration functions ###########")
dictionnary_generic_exploration = fx_generic_explo_dictionnary(df, 100)
dictionnary_specific_exploration = fx_specific_exploration(df)


# 9. Merge exploration dictionnaries ----
print(f"\n########### Merge exploration dictionnaries ###########")
dictionnary_all_exploration = dictionnary_generic_exploration | dictionnary_specific_exploration


# 10. Export all exploration to excel ----
print(f"\n########### Export to Excel ###########")
dict_data_to_export = dictionnary_all_exploration
fx_export_data_to_excel(dict_data_to_export, "bronze_data_exploration", "data_exploration")



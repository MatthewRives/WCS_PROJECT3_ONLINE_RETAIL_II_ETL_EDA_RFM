"""
=============================================================
Explore Silver table
=============================================================
Script purpose:
    This script explores the silver main table and its data from the silver layer. Allows us to evaluate which transformation are required for the gold layer. 

Process:
    01. Connect to the database located ../data/database (it should be named DATAWAREHOUSE_ONLINE_RETAIL_II)
    02. Get "SILVER_SALES" table and add them to a table list
    03. For each table in the table list:
        - Create a dataframe (DF) from the table
        - Add the DF to a DF list
    04. Concatenate the DFs in the DF list
    05. Close connection with DB
    06. Define functions for specific exploration
    07. Call the generic exploration function, that will store several DF and graphs in a generic exploration dictionnary
    08. Call the specific exploration function, that will store several DF in a specific exploration dictionnary
    09. Merge the results of the two exploration in a single dictionnary
    10. Export the exploration results to an Excel file in ../data/data_exploration (it should be named silver_data_exploration.xlsx), with a sheet per DF or graph.
    End of process

List of functions used: 
    - fx_connect_db : connect to the database, imported from connection_to_database.py
    - ...

Potential improvements: 
    - ...
        
WARNING:
    ...
"""

# 1. Import libraries ----
print(f"\n########### Import librairies ###########")
import sqlite3
import pandas as pd
import datetime as dt
from pyparsing import col

from module_connecting_to_database import *
from module_data_exploration import *
from module_export_data_to_xlsx import *


# 2. Connect to database ----
print(f"\n########### Connect to database ###########")
conn = fx_connect_db()
cursor = conn.cursor()


# 3. Get silver sales in the DB ----
print(f"\n########### Get Silver sales table ###########")
cursor.execute("""
SELECT name
FROM sqlite_master
WHERE type='table' AND name LIKE 'SILVER_SALES'
ORDER BY name;
""")

table_sales = [row[0] for row in cursor.fetchall()]
print(f"Table studied: {table_sales}")


# 4. Get silver sales as df ----
query = f'SELECT * FROM "{table_sales[0]}"'
df = pd.read_sql_query(query, conn)



# 5. [ITERATIVE] Create Special data exploration functions ----

## Create fx_get_unique_countries function ----
def fx_get_unique_countries(df):
    print(f"\n---------- Unique countries ----------")
    df_countries = pd.DataFrame(df['COUNTRY'].dropna().unique(), columns=['COUNTRY'])
    df_countries = df_countries.sort_values(by='COUNTRY').reset_index(drop=True)
    return df_countries


## Create fx_get_unique_description function ----
def fx_get_unique_description(df):
    print(f"\n---------- Unique Description ----------")
    df_description = pd.DataFrame(df['DESCRIPTION'].dropna().unique(), columns=['DESCRIPTION'])
    df_description = df_description.sort_values(by='DESCRIPTION').reset_index(drop=True)
    return df_description


## Create fx_description_name_per_stockcode function ----
def fx_description_name_per_stockcode(df):
    print(f"\n---------- Description name per stockcode ----------")
    df_description_name_per_stockcode = df[['STOCKCODE', 'DESCRIPTION']].drop_duplicates()
    df_description_name_per_stockcode = df_description_name_per_stockcode.sort_values(by = ['STOCKCODE', 'DESCRIPTION']).reset_index(drop=True)
    return df_description_name_per_stockcode


## Create fx_description_count_per_stockcode function ----
def fx_description_count_per_stockcode(df):
    print(f"\n---------- Description count per stockcode ----------")
    df_description_count_per_stockcode = df.groupby('STOCKCODE').agg(
    {"PRICE": pd.Series.nunique,
    "DESCRIPTION": pd.Series.nunique})

    df_description_count_per_stockcode = df_description_count_per_stockcode.sort_values(by = 'STOCKCODE').reset_index(drop=False)

    df_description_count_per_stockcode['ISSUE_COUNT_DESCRI'] = np.where(df_description_count_per_stockcode['DESCRIPTION'] > 1, '1 code = X descri', '1 code = 1 descri')

    # V2
    df_description_count_per_stockcode['LENGTH_CODE'] = df_description_count_per_stockcode['STOCKCODE'].apply(len)

    average_length = df_description_count_per_stockcode['LENGTH_CODE'].mean()

    df_description_count_per_stockcode['ISSUE_LENGTH_CODE'] = np.where(df_description_count_per_stockcode['LENGTH_CODE'] < average_length, 'MAYBE', 'MAYBE NO')

    # --
    price_count = df_description_count_per_stockcode['PRICE'].mean()

    df_description_count_per_stockcode['ISSUE_COUNT_PRICE_CODE'] = np.where(df_description_count_per_stockcode['PRICE'] > price_count, '1 code = X prices', '1 code = 1 price')

    return df_description_count_per_stockcode


## Create fx_description_count_per_stockcode_per_country function ----
def fx_description_count_per_stockcode_per_country(df):
    print(f"\n---------- Description count per stockcode per country ----------")
    df_description_count_per_stockcode_per_country = df.groupby(['STOCKCODE', 'COUNTRY']).agg(
    {"PRICE": pd.Series.nunique,
    "DESCRIPTION": pd.Series.nunique})

    df_description_count_per_stockcode_per_country = df_description_count_per_stockcode_per_country.sort_values(by = ['STOCKCODE', 'COUNTRY']).reset_index(drop=False)
    
    # V2
    df_description_count_per_stockcode_per_country['ISSUE_COUNT_DESCRI'] = np.where(df_description_count_per_stockcode_per_country['DESCRIPTION'] > 1, '1 pair = X descri', '1 pair = 1 descri')

    # --
    price_count = df_description_count_per_stockcode_per_country['PRICE'].mean()

    df_description_count_per_stockcode_per_country['ISSUE_COUNT_PRICE'] = np.where(df_description_count_per_stockcode_per_country['PRICE'] > price_count, '1 pair = X prices', '1 pair = 1 price')

    return df_description_count_per_stockcode_per_country


## Create fx_stockcode_count_per_description function ----
def fx_stockcode_count_per_description(df):
    print(f"\n---------- Stockcode count per description ----------")
    df_stockcode_count_per_description = df.groupby('DESCRIPTION').agg(
    {"STOCKCODE": pd.Series.nunique,
    "PRICE": pd.Series.nunique})

    df_stockcode_count_per_description = df_stockcode_count_per_description.sort_values(by = 'DESCRIPTION').reset_index(drop=False)

    # V2
    df_stockcode_count_per_description['ISSUE_COUNT_CODE'] = np.where(df_stockcode_count_per_description['STOCKCODE'] > 1, '1 descri = X codes', '1 descri = 1 code')

    # V3
    df_stockcode_count_per_description['LENGTH_DESCRI'] = df_stockcode_count_per_description['DESCRIPTION'].apply(len)

    average_length = df_stockcode_count_per_description['LENGTH_DESCRI'].mean()

    df_stockcode_count_per_description['ISSUE_LENGTH_DESCRI'] = np.where(df_stockcode_count_per_description['LENGTH_DESCRI'] < average_length, 'MAYBE', 'MAYBE NO')

    # --
    price_count = df_stockcode_count_per_description['PRICE'].mean()

    df_stockcode_count_per_description['ISSUE_COUNT_PRICE_DESCRI'] = np.where(df_stockcode_count_per_description['PRICE'] > price_count, '1 descri = X prices', '1 descri = 1 price')

    return df_stockcode_count_per_description


## Create fx_faulty_description function ----
def fx_faulty_description(df_to_explore, df_description_count_per_stockcode, df_stockcode_count_per_description):
    df_to_explore = df_to_explore.drop(['INVOICE_DATE', 'INVOICE_TIME'], axis = 1)

    df_description_count_per_stockcode = df_description_count_per_stockcode.drop(columns = ["PRICE", "DESCRIPTION"])

    df_stockcode_count_per_description = df_stockcode_count_per_description.drop(columns = ["PRICE", "STOCKCODE"])

    df_check = pd.merge(left = df_to_explore,
                        right = df_description_count_per_stockcode,
                        left_on = 'STOCKCODE',
                        right_on = 'STOCKCODE',
                        how='left')

    df_check = pd.merge(left = df_check,
                        right = df_stockcode_count_per_description,
                        left_on = 'DESCRIPTION',
                        right_on = 'DESCRIPTION',
                        how='left')
    
    mask = (
    (df_check["ISSUE_COUNT_DESCRI"] == "MAYBE") |
    (df_check["ISSUE_LENGTH_CODE"] == "MAYBE") |
    (df_check["ISSUE_COUNT_PRICE_CODE"] == "1 code = X prices") |
    (df_check["ISSUE_COUNT_CODE"] == "1 descri = X codes") |
    (df_check["ISSUE_LENGTH_DESCRI"] == "MAYBE") |
    (df_check["ISSUE_COUNT_PRICE_DESCRI"] == "1 descri = X prices")
        )
    
    df_check['TO_CHECK'] = mask.map({True: "YES", False: "NO"})

    return df_check


## Create fx_country_per_customer function ----
def fx_country_per_customer(df_to_explore):
    df_customer_country = df_to_explore[['CUSTOMER_ID', 'COUNTRY']].drop_duplicates()
    
    df_customer_multi_countries = (
        df_customer_country
        .groupby('CUSTOMER_ID')
        .filter(lambda x: len(x) > 1)
        .assign(CUSTOMER_ID=lambda x: x['CUSTOMER_ID'].astype(str)) # Convert to STR because of num and alpha values inside
        .sort_values(by = "CUSTOMER_ID", ascending = True)
    )

    return df_customer_multi_countries


# 6. Regroup specific exploration functions ----
def fx_specific_exploration(df):
    print(f"\n########### Specific exploration ###########")
    df_to_explore = df.copy()

    # Countries
    df_countries = fx_get_unique_countries(df_to_explore)

    # DESC UNIQUE
    df_description = fx_get_unique_description(df_to_explore)

    # DESC PER CODE
    df_description_name_per_stockcode = fx_description_name_per_stockcode(df_to_explore)

    # DESC COUNT PER CODE
    df_description_count_per_stockcode = fx_description_count_per_stockcode(df_to_explore)

    # DESC COUNT PER CODE PER COUNTRY
    df_description_count_per_stockcode_per_country = fx_description_count_per_stockcode_per_country(df_to_explore)

    # CODE COUNT PER DESC
    df_stockcode_count_per_description = fx_stockcode_count_per_description(df_to_explore)

    # CHECK FOR FAULTY DESCRI CODE
    df_faulty_description = fx_faulty_description(df_to_explore, df_description_count_per_stockcode, df_stockcode_count_per_description)

    # CUSTOMER MULTI COUNTRIES
    df_customer_multi_countries = fx_country_per_customer(df_to_explore)

    print(f"\n########### Specific exploration result in dictionnary ###########")
    dictionnary_specific_exploration = {
        "Countries": df_countries,
        "Desc unique": df_description,
        "Desc per code":df_description_name_per_stockcode,
        "Desc count per code":df_description_count_per_stockcode,
        "Desc count per code country":df_description_count_per_stockcode_per_country,
        "Code count per desc":df_stockcode_count_per_description,
        "Check faulty descri code":df_faulty_description,
        "Customer Multi Countries":df_customer_multi_countries
    }
    return dictionnary_specific_exploration


# 7. Call generic and specific exploration functions ----
print(f"\n########### Call exploration functions ###########")
dictionnary_generic_exploration = fx_generic_explo_dictionnary(df, 100)
dictionnary_specific_exploration = fx_specific_exploration(df)


# 8. Merge exploration dictionnaries ----
print(f"\n########### Merge exploration dictionnaries ###########")
dictionnary_all_exploration = dictionnary_generic_exploration | dictionnary_specific_exploration


# 9. Export all exploration to excel ----
print(f"\n########### Export to Excel ###########")
dict_data_to_export = dictionnary_all_exploration
fx_export_data_to_excel(dict_data_to_export, "silver_data_exploration", "data_exploration")
"""
=============================================================
Create Gold layer and insert transformed data from silver layer
=============================================================
Script purpose:
    This script creates views for the gold layer (medaillon data model), in the database, with transformed data and tables from the silver layer tables.

Layer purpose:
    The Gold layer delivers highly refined, aggregated data optimized for end-user consumption.
        Aggregation
            Summarizing data (e.g., daily revenue, customer cohorts).
        Dimensional Modeling
            Star or snowflake schemas for BI tools (facts + dimensions).
        Performance Optimization
            Partitioning, indexing, and materialized views for fast queries.
        Business Semantics
            Naming conventions aligned with organizational KPIs.

Process:
    01. Connect to the database located ../datasets/database (it should be named DATAWAREHOUSE_ONLINE_RETAIL_II)
    02. 
    End of process

List of functions used: 
    - fx_connect_db : connect to the database, imported from connection_to_database.py
    - 

Potential improvements: 
    - Not determined yet
        

- gold dim_customers
---- customer id
---- country id 
un client peut-il avoir plusieurs pays ?
-> un client peut avoir plsuieurs pays, sans plus d'info sur eux, inutile
-> et pas assez d'infos pour le rendre pertinent

- gold dim country
---- country id
---- country name
---- contient
---- currency id
---- currency name
---- timezone

- gold fact conversion rate per date
---- date
---- currency name 
---- exchange rate to gbp

- gold dim product 
---- product id = product id
---- stockcode
---- description raw


- gold fact sales refund
---- invoice id
---- product id
---- customer id 
---- invoice date
---- invoice time
---- quantity

WARNING:

"""

# 1. Import librairies ----
import sqlite3
import pandas as pd

from module_connecting_to_database import *
from module_export_data_to_xlsx import *
from module_create_table import *


# 2. Connect to database ----
conn = fx_connect_db()
cursor = conn.cursor()


# 3. Get all Silver tables and create df ----
## Import SILVER_ONLINE_RETAIL_II table ----
cursor.execute("""
SELECT name
FROM sqlite_master
WHERE type='table' AND name LIKE 'SILVER_ONLINE_RETAIL_II'
ORDER BY name;
""")

table = [row[0] for row in cursor.fetchall()]
print(f"List of studied table: {table}")

query = f'SELECT * FROM "{table[0]}"'
df_sales = pd.read_sql_query(query, conn)

print(df_sales.sample(5))



## Import SILVER_COUNTRY_METADATA table ----
print(f"\n########### Get SILVER_COUNTRY_METADATA table ###########")
cursor.execute("""
SELECT name
FROM sqlite_master
WHERE type='table' AND name LIKE 'SILVER_COUNTRY_METADATA'
ORDER BY name;
""")

table = [row[0] for row in cursor.fetchall()]
print(f"List of studied table: {table}")

query = f'SELECT * FROM "{table[0]}"'
df_country = pd.read_sql_query(query, conn)

print(df_country.sample(5))



## Import SILVER_PRODUCT_MAPPING table ----
cursor.execute("""
SELECT name
FROM sqlite_master
WHERE type='table' AND name LIKE 'SILVER_PRODUCT_MAPPING'
ORDER BY name;
""")

table = [row[0] for row in cursor.fetchall()]
print(f"List of studied table: {table}")

query = f'SELECT * FROM "{table[0]}"'
df_product = pd.read_sql_query(query, conn)

print(df_product.sample(5))



## Import SILVER_EXCHANGE_RATE table ----
cursor.execute("""
SELECT name
FROM sqlite_master
WHERE type='table' AND name LIKE 'SILVER_EXCHANGE_RATE'
ORDER BY name;
""")

table = [row[0] for row in cursor.fetchall()]
print(f"List of studied table: {table}")

query = f'SELECT * FROM "{table[0]}"'
df_exchange_rate = pd.read_sql_query(query, conn)

print(df_exchange_rate.sample(5))


# 4. Create Country ID in FACT table ----
## Create Country PK in DIM table ----
# Create a deterministic IDs (so IDs don’t change if the table is rebuilt) with a hash on the country name
df_country['COUNTRY_ID'] = df_country['COUNTRY_RAW'].apply(lambda x: abs(hash(x)) % 10_000)
print(df_country.sample(5))

## Add COUNTRY ID as FK in FACT SALES ----
df_sales = pd.merge(
    df_sales,
    df_country[['COUNTRY_RAW', 'COUNTRY_ID']],
    left_on='COUNTRY',
    right_on='COUNTRY_RAW',
    how='left'
    )

## Clean FACT SALES from country raw column ----
df_sales = df_sales.drop(['COUNTRY_RAW', 'COUNTRY'], axis = 1)
print(df_sales.sample(20))



# 5. Create Product ID in FACT table ----
## Create product PK in DIM table ----
# Create a deterministic IDs (so IDs don’t change if the table is rebuilt) with a hash on the country name
df_product['PRODUCT_ID'] = df_product['STOCKCODE'] + '_' + df_product['DESCRIPTION_RAW']
df_product['PRODUCT_ID'] = df_product['PRODUCT_ID'].apply(lambda x: abs(hash(x)) % 100_000)
print(df_product.sample(5))

## Add PRODUCT ID as FK in FACT SALES ----
df_sales = pd.merge(
    df_sales,
    df_product,
    left_on= ['STOCKCODE', 'DESCRIPTION'],
    right_on=['STOCKCODE', 'DESCRIPTION_RAW'],
    how='left'
    )

## Clean FACT SALES from description raw column ----
df_sales = df_sales.drop(columns = ['DESCRIPTION', 'DESCRIPTION_RAW', 'PRODUCT_NAME'])
print(df_sales.sample(20))


# 6. Create GOLD layer tables ----
## Create GOLD_FACT_SALES ----
### Check columns ----
print(f"GOLD FACT SALES columns: {df_sales.columns}")
#['INVOICE', 'STOCKCODE', 'QUANTITY', 'PRICE', 'CUSTOMER_ID',
#    'INVOICE_NUM', 'INVOICE_ALPHA', 'STOCKCODE_NUM', 'STOCKCODE_ALPHA',       
#    'INVOICE_DATE', 'INVOICE_TIME', 'COUNTRY_ID', 'PRODUCT_ID']

### Clean columns ----
df_sales = df_sales.drop(columns = ['INVOICE_NUM', 'INVOICE_ALPHA','STOCKCODE_NUM','STOCKCODE_ALPHA'])

### Map dtype ----
dtype_mapping = {
    'INVOICE': 'TEXT', 
    'INVOICE_TYPE':'TEXT',
    'STOCKCODE': 'TEXT', 
    'QUANTITY': 'INTEGER', 
    'PRICE': 'REAL', 
    'CUSTOMER_ID': 'INTEGER', 
    'INVOICE_DATE': 'TEXT', 
    'INVOICE_TIME': 'TEXT',
    'COUNTRY_ID':'INTEGER',
    'PRODUCT_ID':'INTEGER'
    }

### Create table ----
create_gold_fact_sales = fx_create_table("GOLD", "FACT_SALES", df_sales, dtype_mapping, conn)


## Create GOLD_DIM_COUNTRY table ----
### Check columns ----
print(f"GOLD DIM COUNTRY columns: {df_country.columns}")
# ['COUNTRY_RAW', 'COUNTRY_STANDARDIZED', 'COUNTRY_CONFIDENCE', 'CONTINENT', 'CAPITAL', 'ISO3', 'CURRENCY', 'TIMEZONE', 'COUNTRY_ID']

### Clean columns ----
df_country = df_country.drop(columns = ['COUNTRY_RAW', 'COUNTRY_CONFIDENCE'])

### Map dtype ----
dtype_mapping = {
    'COUNTRY_RAW':'TEXT', 
    'COUNTRY_STANDARDIZED':'TEXT', 
    'COUNTRY_CONFIDENCE':'TEXT',
    'CONTINENT':'TEXT', 
    'CAPITAL':'TEXT',
    'ISO3':'TEXT', 
    'CURRENCY':'TEXT', 
    'TIMEZONE':'TEXT', 
    'COUNTRY_ID':'INTEGER'
    }

### Create table ----
create_gold_dim_country = fx_create_table("GOLD", "DIM_COUNTRY", df_country, dtype_mapping, conn)


## Create GOLD_DIM_PRODUCT table ----
### Check columns ----
print(f"GOLD DIM PRODUCT columns: {df_product.columns}")
# ['STOCKCODE', 'DESCRIPTION_RAW', 'PRODUCT_NAME', 'PRODUCT_ID']

### Map dtype ----
dtype_mapping = {
    'STOCKCODE': 'TEXT', 
    'DESCRIPTION_RAW': 'TEXT',
    'PRODUCT_NAME': 'TEXT',
    'PRODUCT_ID': 'INTEGER' 
    }

### Create table ----
create_gold_dim_country = fx_create_table("GOLD", "DIM_PRODUCT", df_product, dtype_mapping, conn)


## Create GOLD_FACT_EXCHANGE_RATE table ----
### Check columns ----
print(f"GOLD FACT EXCHANGE RATE columns: {df_exchange_rate.columns}")
# ['INVOICE_DATE', 'CURRENCY', 'EXCHANGE_RATE_TO_GBP']

### Map dtype ----
dtype_mapping = {
    'INVOICE_DATE': 'TEXT', 
    'CURRENCY': 'TEXT', 
    'EXCHANGE_RATE_TO_GBP': 'REAL'
    }

### Create table ----
create_gold_fact_sales = fx_create_table("GOLD", "FACT_EXCHANGE_RATE", df_exchange_rate, dtype_mapping, conn)
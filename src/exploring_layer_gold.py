# 1. Library import ----
print(f"\n########### Import librairies ###########")
import os

import sqlite3
import pandas as pd
import datetime as dt

from module_connecting_to_database import *
from module_export_data_to_xlsx import *
from module_create_table import *


# 2. Connect to database ----
print(f"\n########### Connect to DB ###########")
conn = fx_connect_db()
cursor = conn.cursor()


# 3. Import table Gold Fact Sales ----
print(f"\n########### Get GOLD tables ###########")
cursor.execute("""
SELECT name
FROM sqlite_master
WHERE type='table' AND name LIKE 'GOLD_%'
ORDER BY name;
""")

tables = [row[0] for row in cursor.fetchall()]
print(f"List of studied table: {tables}")
# ['GOLD_DIM_COUNTRY', 'GOLD_DIM_CUSTOMER_RFM', 'GOLD_DIM_EXCHANGE_RATE', 'GOLD_DIM_PRODUCT', 'GOLD_DIM_RFM_MAPPING', 'GOLD_FACT_SALES']


# 4. Create a df from each table and add it to list ----
print(f"\n########### Create DF ###########")
df_list = []
for table in tables:
    query = f'SELECT * FROM "{table}"'
    df_query = pd.read_sql_query(query, conn)
    df_list.append(df_query)

# 5. Unpacking df ----
df_country, df_customer_rfm, df_exchange_rate, df_product, df_rfm_mapping, df_sales = df_list


# 6. Export to excel ----
print(f"\n########### Export to Excel ###########")
dict_data_to_export = {
    "Gold sales": df_sales,
    "Gold country metadata": df_country,
    "Gold product mapping": df_product,
    "Gold exchange rate": df_exchange_rate,
    "Gold rfm mapping rate": df_rfm_mapping,
    "Gold customer rfm": df_customer_rfm
    }

fx_export_data_to_excel(dict_data_to_export, "gold_layers", "data_exploration")
"""
=============================================================
...
=============================================================
Script purpose:

Process:
    01. Connect to the database located ../data/database (it should be named DATAWAREHOUSE_ONLINE_RETAIL_II)
    02. 
    End of process



Recency_Score, Frequency_Score, Monetary_Score
I rate the recency, frequency, and monetary scores between 5 and 1. In this section, data with a score of 5 will be the data with the best recency score for us, data with a score of 1 will have the worst score.

Recency_Score: You know, high recency values are bad for us. Because high recency values represent the day the customer stays away from the company. Customers with low recency values are customers who do not stay away from the company.

Frequency_Score: High-frequency values are good for us. The Frequency value is that customer use how many use our company more.

Monetary_Score: High-monetary values are good for us. The Monetary value is the customer how much pays the company.

Une nouvelle méthode de segmentation RFM est utilisée : 
•	Chaque score (R, F, M) est noté de 1 à 5. 
•	Score minimal 3 (1+1+1).
•	Score maximal 15 (5+5+5).
•	Les valeurs des bornes ne sont pas arbitraires, ni fixes. 
Les bornes varient selon les données fournies. Chaque borne représente un quintile de chaque critère (de 0 à 20%, de 20 à 40%, de 40 à 60%, de 60 à 80% de 80 à 100%).
Les avantages : 
•	Les bornes étant variables en fonction des données clients, elles se mettent à jour en permanence.
•	Si, pour une raison X ou Y, nous devions nous intéresser à l’un des scores, il sera plus simple de faire pondération. Par exemple, si nous souhaitons mettre en exergue la fidélité et donc la fréquence d’achat, le score Fréquence sera multiplié par 1,2 et les autres scores par 0,9.
•	Les clients peuvent être segmentés en groupe de quintile.



List of functions used: 
    - fx_connect_db : connect to the database, imported from connection_to_database.py
    - 

Potential improvements: 
    - Not determined yet

WARNING:

"""
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
print(f"\n########### Get GOLD_FACT_SALES table ###########")
cursor.execute("""
SELECT name
FROM sqlite_master
WHERE type='table' AND name LIKE 'GOLD_FACT_SALES'
ORDER BY name;
""")

table = [row[0] for row in cursor.fetchall()]
print(f"List of studied table: {table}")

query = f'SELECT * FROM "{table[0]}"'
df_sales = pd.read_sql_query(query, conn)

print(df_sales.sample(5))


# 4. Explore Sales data ----
print(f"\n########### Data Exploration ###########")

print(f"\n{df_sales.info()}")

print(f"\n{df_sales.describe().T}")

print(f"\nColumn list in the dataset: {df_sales.columns}")
# ['INVOICE', 'STOCKCODE', 'QUANTITY', 'PRICE', 'CUSTOMER_ID', 'INVOICE_DATE', 'INVOICE_TIME', 'COUNTRY_ID', 'PRODUCT_ID']

print(f"\nThe number of customer in the dataset: {df_sales["CUSTOMER_ID"].nunique()}")

print(f"\nThe number of product in the dataset: {df_sales["PRODUCT_ID"].nunique()}")

print(f"\nNull values:")
print(df_sales.isna().sum())


# 5. Prepare dataset for RFM ----
print(f"\n########### Prepare data for RFM ###########")

## Obtain max Date ----
### Convert the column to datetime ----
df_sales['INVOICE_DATE'] = pd.to_datetime(df_sales['INVOICE_DATE'])


### Get max date ----
date_max = df_sales['INVOICE_DATE'].max()
print(date_max)


### Add one day ----
today_date = date_max + dt.timedelta(days=1)
print(today_date)


## Remove non-sales ----
### Keep quantity > 0 only ----
df_sales = df_sales[df_sales['QUANTITY'] > 0]

### Keep price > 0 only ----
df_sales = df_sales[df_sales['PRICE'] > 0]


### Exclude Unknown Customer ID
print(f"Number of unique customer id before: {df_sales['CUSTOMER_ID'].nunique()}")

df_sales = df_sales[df_sales['CUSTOMER_ID'] != 'UNKNOWN']

print(f"Number of unique customer id after: {df_sales['CUSTOMER_ID'].nunique()}")

print(df_sales.head())


# 6. Create RFM df ----
print(f"\n########### Create RFM df ###########")
## Group by and aggregate ----
# as_index = False keep the CUSTOMER_ID as a column
df_rfm = df_sales.groupby('CUSTOMER_ID', as_index = False).agg({
    'INVOICE_DATE': lambda x: (today_date - x.max()).days,
    'INVOICE': lambda x: x.nunique(),
    'REVENUE': lambda x: x.sum()
})


## Rename columns ----
df_rfm.columns = ['CUSTOMER_ID', 'RECENCY', 'FREQUENCY', 'MONETARY']


# 7. Explore RFM Data ----
print(df_rfm)
print(df_rfm.columns)
print(df_rfm.info())
print(df_rfm.describe(include = "all").T)


# 8. Define RFM scores with quintile bins ----
print(f"\n########### Define RFM rfm ###########")

## Define bins for Recency ----
print(f"\n---------- Recency bins ----------")
# High level of recency : it was long time since purchase : bad
# Recency: lower is better, so reverse the ranking
df_rfm["RECENCY_SCORE"] = 6 - (pd.qcut(
    df_rfm["RECENCY"].rank(method='first'),
    q=5, 
    labels=False, 
    duplicates="drop") + 1) 


## Define bins for Frequency ----
# Frequency: higher is better
print(f"\n---------- Frequency bins ----------")
df_rfm["FREQUENCY_SCORE"] = pd.qcut(
    df_rfm["FREQUENCY"].rank(method='first'),  # This returns numeric ranks
    q=5, 
    labels=False,  # labels=False returns numeric bins (0, 1, 2, 3, 4)
    duplicates="drop") + 1  # Adding 1 gives (1, 2, 3, 4, 5)


## Define bins for Monetary ----
print(f"\n---------- Monetary bins ----------")
# Monetary: higher is better
df_rfm["MONETARY_SCORE"] = pd.qcut(
    df_rfm["MONETARY"], 
    q=5, 
    labels=False, 
    duplicates="drop") + 1


## Explore data ----
print(f"\n---------- Explore data ----------")
print(df_rfm.head())
print(df_rfm.info)


# 10. Create score summary ----
print(f"\n########### Create RFM Score summary ###########")

## Summerize Recency bins ----
print(f"\n---------- Recency summary ----------")
summary_recency = df_rfm.groupby("RECENCY_SCORE").agg(
    min_recency=("RECENCY", "min"),
    max_recency=("RECENCY", "max"),
    count=("RECENCY", "count")
).reset_index().sort_values(by="RECENCY_SCORE", ascending=True)

print(summary_recency)


## Summerize Frequency bins ----
print(f"\n---------- Frequency summary ----------")
summary_frequency = df_rfm.groupby("FREQUENCY_SCORE").agg(
    min_frequency=("FREQUENCY", "min"),
    max_frequency=("FREQUENCY", "max"),
    count=("FREQUENCY", "count")
).reset_index().sort_values(by="FREQUENCY_SCORE", ascending=True)

print(summary_frequency)


## Summerize Monetary bins ----
print(f"\n---------- Monetary summary ----------")
summary_monetary = df_rfm.groupby("MONETARY_SCORE").agg(
    min_monetary=("MONETARY", "min"),
    max_monteray=("MONETARY", "max"),
    count=("MONETARY", "count")
).reset_index().sort_values(by="MONETARY_SCORE", ascending=True)

print(summary_monetary)



## Combine RF scoring ----
df_rfm['RFM_SCORE'] = df_rfm[['RECENCY_SCORE', 'FREQUENCY_SCORE', 'MONETARY_SCORE']].astype(str).agg(''.join, axis=1)



# 11. Export to excel ----
print(f"\n########### Export to Excel ###########")
dict_data_to_export = {
    "Customer RFM score": df_rfm,
    "Summary recency": summary_recency,
    "Summary frequency": summary_frequency,
    "Summary monetary": summary_monetary,
    }

fx_export_data_to_excel(dict_data_to_export, "gold_customer_rfm", "data_exploration")


# 12. Create GOLD_DIM_CUSTOMER_RFM table ----
print(f"\n---------- Gold DIM Customer RFM ----------")


## Check columns ----
print(f"Gold DIM Customer RFM columns: {df_rfm.columns}")
# ['CUSTOMER_ID', 'RECENCY', 'FREQUENCY', 'MONETARY', 'RECENCY_SCORE', 'FREQUENCY_SCORE', 'MONETARY_SCORE', 'RFM_SCORE']


## Map dtype ----
dtype_mapping = {
    'CUSTOMER_ID': 'TEXT', 
    'RECENCY': 'TEXT', 
    'FREQUENCY': 'TEXT',
    'MONETARY': 'TEXT',
    'RECENCY_SCORE':'TEXT',
    'FREQUENCY_SCORE':'TEXT',
    'MONETARY_SCORE':'TEXT',
    'RFM_SCORE':'TEXT',
    }

## Create table ----
create_gold_dim_customer_rfm = fx_create_table("GOLD", "DIM_CUSTOMER_RFM", df_rfm, dtype_mapping, conn)
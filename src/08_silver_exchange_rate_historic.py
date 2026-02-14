"""
=============================================================
Create a Silver table for unique pair of date and foreign currency
=============================================================
Script purpose:
    ...

Table purpose:
    ...

Desired level of precision: 
    Foreign sales represents less than 9% of total sales.
    To minimize API call, we do not need the exchange rate to be precise by the hour or minute. 
    Only the day is required. 
    No need of time (hour), only date is needed to get the exchange rate.

Process:
    01. Connect to the database located ../datasets/database (it should be named DATAWAREHOUSE_ONLINE_RETAIL_II)
    02. Get the main "SILVER_ONLINE_RETAIL_II" table
    03. ...
    End of process

List of functions used: 
    - fx_connect_db : connect to the database, imported from connection_to_database.py
    - 

Potential improvements: 
    - Not determined yet
        
WARNING:

"""


# 1. Import library ----
import sqlite3
import pandas as pd
import requests

# pip install forex-python
from forex_python.converter import CurrencyRates
from forex_python.converter import RatesNotAvailableError

from module_connecting_to_database import *
from module_export_data_to_xlsx import *
from module_create_table import *


# 2. Connect to database ----
conn = fx_connect_db()
cursor = conn.cursor()


# 3. Get SILVER_ONLINE_RETAIL_II table ----
cursor.execute("""
SELECT name
FROM sqlite_master
WHERE type='table' AND name LIKE 'SILVER_ONLINE_RETAIL_II'
ORDER BY name;
""")

table_sales = [row[0] for row in cursor.fetchall()]
print(f"Table studied: {table_sales}")


# 4. Get unique countries and invoice data as df ----
query = f'SELECT DISTINCT COUNTRY, INVOICE_DATE FROM "{table_sales[0]}"'
df_invoice_pair_country_date = pd.read_sql_query(query, conn)


print(df_invoice_pair_country_date.sample(20))


# 5. Get SILVER_COUNTRY_METADATA table ----
print(f"\n########### Get SILVER_COUNTRY_METADATA table ###########")

cursor.execute("""
SELECT name
FROM sqlite_master
WHERE type='table' AND name LIKE 'SILVER_COUNTRY_METADATA'
ORDER BY name;
""")

table_country_metadata = [row[0] for row in cursor.fetchall()]
print(f"Table studied: {table_country_metadata}")


# 6. Get distinct country and currency ----
query = f'SELECT DISTINCT COUNTRY_RAW, CURRENCY FROM "{table_country_metadata[0]}"'
df_currency = pd.read_sql_query(query, conn)

print(df_currency.sample(20))


# 7. Merge dfs to get pair of country and currency ----

df_merged = df_invoice_pair_country_date.merge(
    df_currency, 
    left_on='COUNTRY', 
    right_on='COUNTRY_RAW', 
    how='left')

print(df_merged.sample(20))


# 8. Clean df to keep only currency and dates ----
df_exchange_rate = df_merged[['INVOICE_DATE', 'CURRENCY']].drop_duplicates().reset_index(drop=True)

print(df_exchange_rate.sample(20))
number_of_pair = len(df_exchange_rate)
print(f"\nNumber of unique currency and dates pairs: {number_of_pair}")


# 9. Get exchange rate by currency/date pair with API ----
## Create an empty list that will get missing value ----
missing_fx = []   # will convert to DataFrame later


## Create fx_get_rates function ----
def fx_get_rates(date, currency_to_check, base="GBP"):
    # minimize API calls by only fetching dates that actually have non-GBP currencies
    if currency_to_check == base:
        return 1

    # API Call
    # https://api.frankfurter.dev/v1/1999-01-04?base=USD&symbols=EUR
    #     {
    #   "base": "USD",
    #   "date": "1999-01-04",
    #   "rates": {
    #     "EUR": 0.84825
    #   }
    # }
    
    date_str = pd.to_datetime(date).strftime("%Y-%m-%d")

    url = f"https://api.frankfurter.app/{date_str}"
    params = {"base": base, "symbols": currency_to_check}

    try:
        request = requests.get(url, params=params, timeout=5)
        request.raise_for_status()
        data = request.json()
        rate = data["rates"][currency_to_check]
        if rate is None:
            raise KeyError("Rate missing in API response")
        return float(rate)

    # First source failed → try fallback
    except (requests.RequestException, KeyError, ValueError):
        try:
            c = CurrencyRates()
            return float(c.get_rate(base, currency_to_check, pd.to_datetime(date_str)))

        # Fallback also failed → log and return None
        except (RatesNotAvailableError, Exception):
            missing_fx.append({
                "date": date_str,
                "base": base,
                "target": currency_to_check
            })
            return None


## Call fx_get_rates for each currency/date pair ----
for idx, row in df_exchange_rate.iterrows():
    print(f"Pair {idx+1}/{len(df_exchange_rate)}: {row['INVOICE_DATE']}/{row['CURRENCY']}")
    df_exchange_rate.at[idx, 'EXCHANGE_RATE_TO_GBP'] = fx_get_rates(
        row['INVOICE_DATE'], 
        row['CURRENCY'], 
        "GBP"
    )

print(df_exchange_rate.sample(20))


# 10. Check if any exchange rate is missing ----
missing_fx = pd.DataFrame(missing_fx)
print(missing_fx)


# 11. Export to Excel ----
dict_data_to_export = {
    "Date Exchange Rate": df_exchange_rate
    }

fx_export_data_to_excel(dict_data_to_export, "silver_pair_currency_date", "data_exploration")




# 12. Create SILVER_EXCHANGE_RATE table in database ----
## Mapping dtype ----

dtype_mapping = {
    'INVOICE_DATE': 'TEXT', 
    'CURRENCY': 'TEXT',
    'EXCHANGE_RATE_TO_GBP':'REAL'
    }

## Call fx_create_table ----
create_silver_exchange_rate = fx_create_table("SILVER", "EXCHANGE_RATE", df_exchange_rate, dtype_mapping, conn)
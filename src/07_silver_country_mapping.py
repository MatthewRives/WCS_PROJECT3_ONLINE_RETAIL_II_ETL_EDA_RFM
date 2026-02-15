"""
=============================================================
Create a Silver table for country meta data and time zone calculation
=============================================================
Script purpose:
    ...

Table purpose:
    ...

Process:
    01. Connect to the database located ../data/database (it should be named DATAWAREHOUSE_ONLINE_RETAIL_II)
    02. Get the main "SILVER_SALES" table
    03. Get the list of unique countries from the "SILVER_SALES" table
    04. Standardize the country names to match API requirements (e.g., replace spaces with underscores, handle known exceptions)
    05. Get country metadata from the API (continent, capital, ISO3 code, currency, timezone) for each standardized country name
    06. Merge the country metadata back to the original country list, keeping track of the confidence level of the country name resolution (exact match, mapped, invalid)
    07. Create a new Silver table named "SILVER_COUNTRY_METADATA" with the country name and its corresponding time zone and currency information
    End of process

List of functions used: 
    - fx_connect_db : connect to the database, imported from connection_to_database.py
    - 

Potential improvements: 
    - Not determined yet
        
WARNING:
    ...
"""

# 1. Import librairies ----
print(f"\n########### Import librairies ###########")
import sqlite3
import pandas as pd
import re
import numpy as np
import requests
import pytz
from timezonefinder import TimezoneFinder
from geopy.geocoders import Nominatim
from datetime import datetime

from module_connecting_to_database import *
from module_export_data_to_xlsx import *
from module_create_table import *


# 2. Connect to database ----
print(f"\n########### Connect to database ###########")
conn = fx_connect_db()
cursor = conn.cursor()


# 3. Get SILVER ONLINE RETAIL II table ----
print(f"\n########### Get Silver Sales table ###########")
cursor.execute("""
SELECT name
FROM sqlite_master
WHERE type='table' AND name LIKE 'SILVER_SALES'
ORDER BY name;
""")

table = [row[0] for row in cursor.fetchall()]
print(f"Table studied: {table}")
# Table studied: ['SILVER_SALES']


# 4. Create a df with distinct countries ----
print(f"\n########### Get Distinct list of countries ###########")
query = f'SELECT DISTINCT COUNTRY FROM "{table[0]}"'
df_country = pd.read_sql_query(query, conn)


# 5. Rename COUNTRY to COUNTRY_RAW ----
df_country = df_country.rename(columns={'COUNTRY': 'COUNTRY_RAW'})
print(f"Country raw: {df_country['COUNTRY_RAW'].tolist()}")


# 6. Standardize country names ----
print(f"\n########### Standardize names ###########")
## Create fx_normalize_country function ----
def fx_normalize_country(x: str) -> str:
    return x.replace("_", " ").title().strip()

## Apply fx_normalize_country ----
df_country['COUNTRY_STANDARDIZED'] = df_country['COUNTRY_RAW'].apply(fx_normalize_country)


# 7. Map countries ----
print(f"\n########### Map countries ###########")
## Define a list of valid countries ----
valid_countries = set(df_country["COUNTRY_STANDARDIZED"])
print(valid_countries)


## Define an exception mapping ----
exception_map_name = {
    "European Community": "European Community",
    "Unknown": "Unknown",
    "West Indies": "West Indies",
    "Eire": "Ireland", 
    "Rsa": "Republic of South Africa",
    "Channel Islands": "Jersey"
    }


## Create fx_resolve_country function ----
print(f"\n---------- Resolve country ----------")
def fx_resolve_country(raw):
    if pd.isna(raw):
        return None, "INVALID"
    
    norm = fx_normalize_country(raw)

    # if country standardized in the exception mapping dictionnary as key, 
    # return the value of this key
    # and tag the col as mapped 
    if norm in exception_map_name:
        return exception_map_name[norm], "MAPPED"

    if norm in valid_countries:
        return norm, "EXACT"

    return None, "INVALID"


## Apply fx_resolve_country ----
df_country[["COUNTRY_STANDARDIZED", "COUNTRY_CONFIDENCE"]] = (
    df_country["COUNTRY_RAW"].apply(lambda x: pd.Series(fx_resolve_country(x)))
)

print(df_country)


# 8. Get country metadata from API ----
print(f"\n########### Call API ###########")
## Define metadata exception mapping ----

exception_map_metadata = {
    "European Community": "France",
    "Unknown": "United Kingdom",
    "West Indies": "Dominican Republic"
    }


## Define fx_get_metadata function ----
def fx_get_metadata(country_name: str) -> dict:

    # Handling country communities and unknown values
    # Returns the mapped value if the key exists, or returns the original country_name if it doesn't (the second parameter is the default).
    country_name = exception_map_metadata.get(country_name, country_name)
    
    url = f"https://restcountries.com/v3.1/name/{country_name}"

    params = {"fields": "region,capital,cca3,currencies,latlng"}

    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()[0]

    except Exception:
        print(f"Error fetching data for {country_name}")
        return {
            "CONTINENT": None,
            "CAPITAL": None,
            "ISO3": None,
            "CURRENCY": None,
            "TIMEZONE":None
        }

    currencies = data.get("currencies", {})
    currency_code = next(iter(currencies.keys()), None)

    # Extract lat/lng from API
    latlng = data.get("latlng", [])
    latitude = latlng[0] if len(latlng) >= 1 else None
    longitude = latlng[1] if len(latlng) >= 1 else None

    # Extract capital
    capital = (data.get("capital") or [None])[0]

    # Find timezone
    # Note : the rest countries API return all country's timezones
    # Ex: France has 10 different timezones. 
    # Need to search with capital lon and latitude instead.

    timezone_str = None
    tf = TimezoneFinder()

    if latitude is not None and longitude is not None:
        timezone_str = tf.timezone_at(lat=latitude, lng=longitude)
    elif capital:
        # Fallback to geocoding if lat/lng missing
        geolocator = Nominatim(user_agent="timezone_finder")
        location = geolocator.geocode(capital)
        if location:
            timezone_str = tf.timezone_at(lat=location.latitude, lng=location.longitude)

    # Get UTC offset
    utc_offset = None
    if timezone_str:
        try:
            tz = pytz.timezone(timezone_str)
            current_time = datetime.now(tz)
            utc_offset = current_time.strftime('%z')
        except Exception:
            utc_offset = None

    return {
        "CONTINENT": data.get("region"),
        "CAPITAL": capital,
        "ISO3": data.get("cca3"),
        "CURRENCY": currency_code,
        "TIMEZONE": utc_offset
    }

## List all countries for the API ----
countries = df_country["COUNTRY_STANDARDIZED"].dropna().unique()


## Call the API with fx_get_metadata ----
metadata = [fx_get_metadata(country) for country in countries]


## Create a metadata df with the results ----
df_country_metadata = pd.DataFrame(metadata)
df_country_metadata["COUNTRY_STANDARDIZED"] = countries


## Merge the metadata df with the country df ----
df_country = df_country.merge(
    df_country_metadata,
    on="COUNTRY_STANDARDIZED",
    how="left"
)


# 9. Export to Excel ----
print(f"\n########### Export to Excel ###########")
dict_data_to_export = {
    "Country Mapping": df_country
    }

fx_export_data_to_excel(dict_data_to_export, "silver_country_mapping", "data_exploration")


# 10. Create SILVER_COUNTRY_METADATA table ----
print(f"\n########### Create Silver Country Metadata table ###########")

## Map dtype ----
dtype_mapping = {
    'COUNTRY_RAW': 'TEXT', 
    'COUNTRY_STANDARDIZED': 'TEXT', 
    'COUNTRY_CONFIDENCE': 'TEXT', 
    'CONTINENT': 'TEXT', 
    'CAPITAL': 'TEXT', 
    'ISO3': 'TEXT', 
    'CURRENCY': 'TEXT', 
    'TIMEZONE': 'TEXT'
    }


## Call fx_create_table ----
create_silver_country_mapping = fx_create_table("SILVER", "COUNTRY_METADATA", df_country, dtype_mapping, conn)
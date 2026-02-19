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
import pandas as pd
import requests
import numpy as np
import re
import pytz
from timezonefinder import TimezoneFinder
from geopy.geocoders import Nominatim
from datetime import datetime, timezone

from src.utils.connecting_to_database import fx_connect_db
from src.utils.create_table import fx_create_table
from src.utils.export_data_to_xlsx import fx_export_data_to_excel
from src.utils.watermark import get_watermark, set_watermark



# ── Country name normalization ───────────────────────────────────

# Normalization ----
EXCEPTION_MAP_NAME = {
    "European Community": "European Community",
    "Unknown":            "Unknown",
    "West Indies":        "West Indies",
    "Eire":               "Ireland",
    "Rsa":                "Republic of South Africa",
    "Channel Islands":    "Jersey"
}

EXCEPTION_MAP_METADATA = {
    "European Community": "France",
    "Unknown":            "United Kingdom",
    "West Indies":        "Dominican Republic"
}

# Create fx_normalize_country function ----
def fx_normalize_country(x: str) -> str:
    return x.replace("_", " ").title().strip()


# Create fx_resolve_country function ----
def fx_resolve_country(raw):
    if pd.isna(raw):
        return None, "INVALID"
    norm = fx_normalize_country(raw)
    
    # if country standardized in the exception mapping dictionnary as key, 
    # return the value of this key
    # and tag the col as mapped 
    if norm in EXCEPTION_MAP_NAME:
        return EXCEPTION_MAP_NAME[norm], "MAPPED"
    
    if norm in set([fx_normalize_country(c) for c in [raw]]):
        return norm, "EXACT"
    return norm, "EXACT"





# ── API call ─────────────────────────────────────────────────────

# Fx API Metadata ----
def fx_get_metadata(country_name: str) -> dict:
    country_name = EXCEPTION_MAP_METADATA.get(country_name, country_name)
    url = f"https://restcountries.com/v3.1/name/{country_name}"
    params = {"fields": "region,capital,cca3,currencies,latlng"}

    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()[0]
    except Exception:
        print(f"  ✗ API error for: {country_name}")
        return {
            "CONTINENT": None, "CAPITAL": None,
            "ISO3": None, "CURRENCY": None, "TIMEZONE": None
        }

    currencies   = data.get("currencies", {})
    currency_code = next(iter(currencies.keys()), None)
    latlng       = data.get("latlng", [])
    latitude     = latlng[0] if len(latlng) >= 1 else None
    longitude    = latlng[1] if len(latlng) >= 2 else None
    capital      = (data.get("capital") or [None])[0]

    timezone_str = None
    tf = TimezoneFinder()
    if latitude is not None and longitude is not None:
        timezone_str = tf.timezone_at(lat=latitude, lng=longitude)
    elif capital:
        geolocator = Nominatim(user_agent="timezone_finder")
        location = geolocator.geocode(capital)
        if location:
            timezone_str = tf.timezone_at(
                lat=location.latitude, lng=location.longitude
            )

    utc_offset = None
    if timezone_str:
        try:
            tz = pytz.timezone(timezone_str)
            utc_offset = datetime.now(tz).strftime('%z')
        except Exception:
            utc_offset = None

    return {
        "CONTINENT": data.get("region"),
        "CAPITAL":   capital,
        "ISO3":      data.get("cca3"),
        "CURRENCY":  currency_code,
        "TIMEZONE":  utc_offset
    }





# ── Main logic ───────────────────────────────────────────────────

# Fx Get Existing Countries ----
def fx_get_existing_countries(conn) -> set:
    """Returns countries already stored in SILVER_COUNTRY_METADATA, if it exists."""
    try:
        df = pd.read_sql_query(
            'SELECT COUNTRY_RAW FROM "SILVER_COUNTRY_METADATA"', conn
        )
        return set(df["COUNTRY_RAW"].tolist())
    except Exception:
        return set()


# Fx Load Silver Country Mapping ----
def fx_load_silver_country_mapping(conn):
    print("\n########### Silver Country Mapping ###########")

    last_run = get_watermark("silver_country_mapping")

    # Get distinct countries from SILVER_SALES
    df_country = pd.read_sql_query(
        'SELECT DISTINCT COUNTRY FROM "SILVER_SALES"', conn
    )
    df_country = df_country.rename(columns={"COUNTRY": "COUNTRY_RAW"})

    # Incremental: skip countries we've already processed
    existing_countries = fx_get_existing_countries(conn)
    new_countries_mask = ~df_country["COUNTRY_RAW"].isin(existing_countries)
    df_new = df_country[new_countries_mask].copy()

    if df_new.empty:
        print("  No new countries to process. Skipping.")
        return

    print(f"  {len(df_new)} new country/ies to process "
          f"(skipping {len(existing_countries)} already known)")

    # Standardize names
    df_new[["COUNTRY_STANDARDIZED", "COUNTRY_CONFIDENCE"]] = (
        df_new["COUNTRY_RAW"].apply(
            lambda x: pd.Series(fx_resolve_country(x))
        )
    )

    # API calls — only for new countries
    countries = df_new["COUNTRY_STANDARDIZED"].dropna().unique()
    print(f"  Calling API for {len(countries)} country/ies...")
    metadata = [fx_get_metadata(c) for c in countries]

    df_metadata = pd.DataFrame(metadata)
    df_metadata["COUNTRY_STANDARDIZED"] = countries

    df_new = df_new.merge(df_metadata, on="COUNTRY_STANDARDIZED", how="left")

    # If table already exists, append new rows instead of dropping
    if existing_countries:
        df_existing = pd.read_sql_query(
            'SELECT * FROM "SILVER_COUNTRY_METADATA"', conn
        )
        df_final = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_final = df_new

    # Export to Excel for exploration
    fx_export_data_to_excel(
        {"Country Mapping": df_final},
        "silver_country_mapping",
        "data_exploration"
    )

    # Save to database
    dtype_mapping = {
        "COUNTRY_RAW":          "TEXT",
        "COUNTRY_STANDARDIZED": "TEXT",
        "COUNTRY_CONFIDENCE":   "TEXT",
        "CONTINENT":            "TEXT",
        "CAPITAL":              "TEXT",
        "ISO3":                 "TEXT",
        "CURRENCY":             "TEXT",
        "TIMEZONE":             "TEXT"
    }

    fx_create_table("SILVER", "COUNTRY_METADATA", df_final, dtype_mapping, conn)

    set_watermark("silver_country_mapping",
                  datetime.now(tz=timezone.utc).isoformat(), "timestamp")
    print(f"  ✓ SILVER_COUNTRY_METADATA — {len(df_final)} rows total "
          f"({len(df_new)} new).")


# Run ----
def run():
    print("\n########### silver_country_mapping | Start ###########")
    try:
        conn = fx_connect_db()
        with conn:
            fx_load_silver_country_mapping(conn)

        print("=" * 50)
        print("Country mapping completed successfully.")
        print("=" * 50)

    except Exception as error:
        print(f"Error: {error}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run()
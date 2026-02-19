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
    01. Connect to the database located ../data/database (it should be named DATAWAREHOUSE_ONLINE_RETAIL_II)
    02. Get the main "SILVER_SALES" table
    03. ...
    End of process

List of functions used: 
    - fx_connect_db : connect to the database, imported from connection_to_database.py
    - 

Potential improvements: 
    - Not determined yet
        
WARNING:

"""


import pandas as pd
import requests
from datetime import datetime, timezone

from forex_python.converter import CurrencyRates
from forex_python.converter import RatesNotAvailableError

from src.utils.connecting_to_database import fx_connect_db
from src.utils.create_table import fx_create_table
from src.utils.export_data_to_xlsx import fx_export_data_to_excel
from src.utils.watermark import get_watermark, set_watermark


# ── Exchange rate API ─────────────────────────────────────────────

# Exchange rate API ----
def fx_get_rates(date, currency_to_check, base="GBP") -> float | None:
    if currency_to_check == base:
        return 1.0

    date_str = pd.to_datetime(date).strftime("%Y-%m-%d")
    url = f"https://api.frankfurter.app/{date_str}"
    params = {"base": base, "symbols": currency_to_check}

    try:
        r = requests.get(url, params=params, timeout=5)
        r.raise_for_status()
        rate = r.json()["rates"][currency_to_check]
        if rate is None:
            raise KeyError("Rate missing in API response")
        return float(rate)

    except (requests.RequestException, KeyError, ValueError):
        try:
            c = CurrencyRates()
            return float(c.get_rate(base, currency_to_check, pd.to_datetime(date_str)))
        except (RatesNotAvailableError, Exception):
            print(f"  ✗ Rate unavailable: {date_str} / {currency_to_check}")
            return None


# ── Existing pairs helper ─────────────────────────────────────────

# Existing pairs helper ----
def fx_get_existing_pairs(conn) -> set:
    """Returns set of (INVOICE_DATE, CURRENCY) tuples already in SILVER_EXCHANGE_RATE."""
    try:
        df = pd.read_sql_query(
            'SELECT INVOICE_DATE, CURRENCY FROM "SILVER_EXCHANGE_RATE"', conn
        )
        return set(zip(df["INVOICE_DATE"], df["CURRENCY"]))
    except Exception:
        return set()


# ── Main logic ────────────────────────────────────────────────────

# Fx load silver exchange rate ----
def fx_load_silver_exchange_rate(conn):
    print("\n########### Silver Exchange Rate ###########")

    last_run = get_watermark("silver_exchange_rate")

    ## Get unique country/date pairs from SILVER_SALES ----
    df_sales = pd.read_sql_query(
        'SELECT DISTINCT COUNTRY, INVOICE_DATE FROM "SILVER_SALES"', conn
    )

    ## Incremental filter — only process invoice dates after last run ----
    if last_run:
        before = len(df_sales)
        df_sales = df_sales[df_sales["INVOICE_DATE"] > last_run]
        print(f"  Incremental filter: {before} → {len(df_sales)} pairs (after {last_run})")

    if df_sales.empty:
        print("  No new date/country pairs. Skipping.")
        return

    ## Get currency per country from SILVER_COUNTRY_METADATA ----
    df_currency = pd.read_sql_query(
        'SELECT DISTINCT COUNTRY_RAW, CURRENCY FROM "SILVER_COUNTRY_METADATA"', conn
    )

    ## Merge to get date/currency pairs ----
    df_merged = df_sales.merge(
        df_currency,
        left_on="COUNTRY",
        right_on="COUNTRY_RAW",
        how="left"
    )

    df_new_pairs = (
        df_merged[["INVOICE_DATE", "CURRENCY"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    ## Second incremental guard — skip pairs already in the table ----
    existing_pairs = fx_get_existing_pairs(conn)
    if existing_pairs:
        mask = df_new_pairs.apply(
            lambda row: (row["INVOICE_DATE"], row["CURRENCY"]) not in existing_pairs,
            axis=1
        )
        df_new_pairs = df_new_pairs[mask].reset_index(drop=True)

    if df_new_pairs.empty:
        print("  All date/currency pairs already fetched. Skipping.")
        return

    print(f"  {len(df_new_pairs)} new date/currency pair(s) to fetch")

    ## API calls — only for new pairs ----
    missing_fx = []
    for idx, row in df_new_pairs.iterrows():
        print(f"  Pair {idx+1}/{len(df_new_pairs)}: "
              f"{row['INVOICE_DATE']} / {row['CURRENCY']}")
        rate = fx_get_rates(row["INVOICE_DATE"], row["CURRENCY"])
        df_new_pairs.at[idx, "EXCHANGE_RATE_TO_GBP"] = rate
        if rate is None:
            missing_fx.append({
                "DATE": row["INVOICE_DATE"],
                "CURRENCY": row["CURRENCY"]
            })

    # Report missing rates ----
    if missing_fx:
        print(f"\n  ⚠ {len(missing_fx)} rate(s) could not be fetched:")
        for m in missing_fx:
            print(f"    {m['DATE']} / {m['CURRENCY']}")

    # Merge new pairs with existing data ----
    if existing_pairs:
        df_existing = pd.read_sql_query(
            'SELECT * FROM "SILVER_EXCHANGE_RATE"', conn
        )
        df_final = pd.concat([df_existing, df_new_pairs], ignore_index=True)
    else:
        df_final = df_new_pairs

    # Export to Excel ----
    fx_export_data_to_excel(
        {"Date Exchange Rate": df_final},
        "silver_pair_currency_date",
        "data_exploration"
    )

    # Save to database ----
    dtype_mapping = {
        "INVOICE_DATE":        "TEXT",
        "CURRENCY":            "TEXT",
        "EXCHANGE_RATE_TO_GBP": "REAL"
    }
    fx_create_table("SILVER", "EXCHANGE_RATE", df_final, dtype_mapping, conn)

    # Watermark = max invoice date processed ----
    new_watermark = df_new_pairs["INVOICE_DATE"].max()
    set_watermark("silver_exchange_rate", new_watermark, "timestamp")
    print(f"  ✓ SILVER_EXCHANGE_RATE — {len(df_final)} rows total "
          f"({len(df_new_pairs)} new). Watermark: {new_watermark}")


# Run ----
def run():
    print("\n########### silver_exchange_rate_historic | Start ###########")
    try:
        conn = fx_connect_db()
        with conn:
            fx_load_silver_exchange_rate(conn)

        print("=" * 50)
        print("Exchange rate completed successfully.")
        print("=" * 50)

    except Exception as error:
        print(f"Error: {error}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run()
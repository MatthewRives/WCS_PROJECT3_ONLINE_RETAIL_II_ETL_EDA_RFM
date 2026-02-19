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
    01. Connect to the database located ../data/database (it should be named DATAWAREHOUSE_ONLINE_RETAIL_II)
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
import pandas as pd
from datetime import datetime, timezone

from src.utils.connecting_to_database import fx_connect_db
from src.utils.create_table import fx_create_table
from src.utils.watermark import get_watermark, set_watermark

# ── Silver table loaders ──────────────────────────────────────────

# 2. Fx load silver tables ----
def fx_load_silver_tables(conn) -> dict:
    """Loads all required silver tables into dataframes."""
    tables = {
        "sales":          "SILVER_SALES",
        "country":        "SILVER_COUNTRY_METADATA",
        "product":        "SILVER_PRODUCT_MAPPING",
        "exchange_rate":  "SILVER_EXCHANGE_RATE",
        "rfm_mapping":    "SILVER_RFM_MAPPING"
    }
    dfs = {}
    for key, table_name in tables.items():
        print(f"  Loading {table_name}...")
        dfs[key] = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
    return dfs


# ── ID generation ─────────────────────────────────────────────────

# 3. ID generation ----
## Fx create country ids ----
def fx_create_country_ids(df_country: pd.DataFrame) -> pd.DataFrame:
    """Adds deterministic COUNTRY_ID using hash on COUNTRY_RAW."""
    df_country["COUNTRY_ID"] = df_country["COUNTRY_RAW"].apply(
        lambda x: abs(hash(x)) % 10_000
    )
    return df_country


## Fx create product ids ----
def fx_create_product_ids(df_product: pd.DataFrame) -> pd.DataFrame:
    """Adds deterministic PRODUCT_ID using hash on STOCKCODE + DESCRIPTION_RAW."""
    df_product["PRODUCT_ID"] = (
        df_product["STOCKCODE"] + "_" + df_product["DESCRIPTION_RAW"]
    ).apply(lambda x: abs(hash(x)) % 100_000)
    return df_product


# ── Fact table builder ────────────────────────────────────────────

# 4. Fact table builder ----
## Fx build fact sales ----
def fx_build_fact_sales(df_sales, df_country, df_product) -> pd.DataFrame:
    """Joins sales with country and product IDs, adds REVENUE column."""

    ### Add COUNTRY_ID as FK ----
    df = pd.merge(
        df_sales,
        df_country[["COUNTRY_RAW", "COUNTRY_ID"]],
        left_on="COUNTRY",
        right_on="COUNTRY_RAW",
        how="left"
    ).drop(columns=["COUNTRY_RAW", "COUNTRY"])

    ### Add PRODUCT_ID as FK ----
    df = pd.merge(
        df,
        df_product[["STOCKCODE", "DESCRIPTION_RAW", "PRODUCT_ID"]],
        left_on=["STOCKCODE", "DESCRIPTION"],
        right_on=["STOCKCODE", "DESCRIPTION_RAW"],
        how="left"
    ).drop(columns=["DESCRIPTION", "DESCRIPTION_RAW"])

    ### Revenue ----
    df["REVENUE"] = df["QUANTITY"] * df["PRICE"]

    return df


# ── Gold table writers ────────────────────────────────────────────

# 5. Gold table writers ----
## Fx create gold fact sales ----
def fx_create_gold_fact_sales(df_sales, conn):
    print("\n───── GOLD_FACT_SALES ─────")
    dtype_mapping = {
        "INVOICE":      "TEXT",
        "STOCKCODE":    "TEXT",
        "QUANTITY":     "INTEGER",
        "PRICE":        "REAL",
        "CUSTOMER_ID":  "TEXT",
        "INVOICE_DATE": "TEXT",
        "INVOICE_TIME": "TEXT",
        "INVOICE_TYPE": "TEXT",
        "COUNTRY_ID":   "INTEGER",
        "PRODUCT_ID":   "INTEGER",
        "REVENUE":      "REAL"
    }
    fx_create_table("GOLD", "FACT_SALES", df_sales, dtype_mapping, conn)
    print(f"  ✓ GOLD_FACT_SALES — {len(df_sales)} rows")


## Fx create gold dim country ----
def fx_create_gold_dim_country(df_country, conn):
    print("\n───── GOLD_DIM_COUNTRY ─────")
    df = df_country.drop(columns=["COUNTRY_RAW", "COUNTRY_CONFIDENCE"])
    dtype_mapping = {
        "COUNTRY_STANDARDIZED": "TEXT",
        "CONTINENT":            "TEXT",
        "CAPITAL":              "TEXT",
        "ISO3":                 "TEXT",
        "CURRENCY":             "TEXT",
        "TIMEZONE":             "TEXT",
        "COUNTRY_ID":           "INTEGER"
    }
    fx_create_table("GOLD", "DIM_COUNTRY", df, dtype_mapping, conn)
    print(f"  ✓ GOLD_DIM_COUNTRY — {len(df)} rows")


## Fx create gold dim product ----
def fx_create_gold_dim_product(df_product, conn):
    print("\n───── GOLD_DIM_PRODUCT ─────")
    dtype_mapping = {
        "STOCKCODE":       "TEXT",
        "DESCRIPTION_RAW": "TEXT",
        "PRODUCT_NAME":    "TEXT",
        "PRODUCT_ID":      "INTEGER"
    }
    fx_create_table("GOLD", "DIM_PRODUCT", df_product, dtype_mapping, conn)
    print(f"  ✓ GOLD_DIM_PRODUCT — {len(df_product)} rows")


## Fx create gold dim exchange rate ----
def fx_create_gold_dim_exchange_rate(df_exchange_rate, conn):
    print("\n───── GOLD_DIM_EXCHANGE_RATE ─────")
    dtype_mapping = {
        "INVOICE_DATE":         "TEXT",
        "CURRENCY":             "TEXT",
        "EXCHANGE_RATE_TO_GBP": "REAL"
    }
    fx_create_table("GOLD", "DIM_EXCHANGE_RATE", df_exchange_rate, dtype_mapping, conn)
    print(f"  ✓ GOLD_DIM_EXCHANGE_RATE — {len(df_exchange_rate)} rows")

## Fx create gold dim rfm mapping -----
def fx_create_gold_dim_rfm_mapping(df_rfm_mapping, conn):
    print("\n───── GOLD_DIM_RFM_MAPPING ─────")
    dtype_mapping = {
        "RFM_SCORE":   "INTEGER",
        "RFM_SEGMENT": "TEXT",
        "RFM_NAME":    "TEXT"
    }
    fx_create_table("GOLD", "DIM_RFM_MAPPING", df_rfm_mapping, dtype_mapping, conn)
    print(f"  ✓ GOLD_DIM_RFM_MAPPING — {len(df_rfm_mapping)} rows")


# ── Main logic ────────────────────────────────────────────────────

# 6. Fx load gold layer ----
def fx_load_gold_layer(conn):
    print("\n########### Gold Layer ###########")

    last_run = get_watermark("gold_layer")

    ## Check if silver sales has new data since last gold run ----
    df_check = pd.read_sql_query(
        'SELECT MAX(INVOICE_DATE) as MAX_DATE FROM "SILVER_SALES"', conn
    )
    max_silver_date = df_check["MAX_DATE"].iloc[0]

    if last_run and max_silver_date <= last_run:
        print("  Silver data unchanged since last gold run. Skipping.")
        return

    print(f"  New data detected (max silver date: {max_silver_date})")

    ## Load all silver tables ----
    dfs = fx_load_silver_tables(conn)

    ## Generate IDs ----
    dfs["country"] = fx_create_country_ids(dfs["country"])
    dfs["product"] = fx_create_product_ids(dfs["product"])

    ## Build fact table ----
    df_fact_sales = fx_build_fact_sales(
        dfs["sales"], dfs["country"], dfs["product"]
    )

    ## Write all gold tables ----
    fx_create_gold_fact_sales(df_fact_sales, conn)
    fx_create_gold_dim_country(dfs["country"], conn)
    fx_create_gold_dim_product(dfs["product"], conn)
    fx_create_gold_dim_exchange_rate(dfs["exchange_rate"], conn)
    fx_create_gold_dim_rfm_mapping(dfs["rfm_mapping"], conn)

    ## Watermark = max invoice date in silver sales ----
    set_watermark("gold_layer", max_silver_date, "timestamp")
    print(f"\n  Watermark updated to: {max_silver_date}")


# 7. Run ----
def run():
    print("\n########### script_layer_gold | Start ###########")
    try:
        conn = fx_connect_db()
        with conn:
            fx_load_gold_layer(conn)

        print("=" * 50)
        print("Gold layer completed successfully.")
        print("=" * 50)

    except Exception as error:
        print(f"Error: {error}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run()
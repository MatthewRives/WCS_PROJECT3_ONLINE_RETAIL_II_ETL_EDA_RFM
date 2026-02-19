"""
=============================================================
Create a Silver product table
=============================================================
Script purpose:
    ...

Table purpose:
    ...

Desired level of precision: 
    ...

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
    - When calling the silver table, Must not remove duplicates because of the mode method used later
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone

from src.utils.connecting_to_database import fx_connect_db
from src.utils.create_table import fx_create_table
from src.utils.export_data_to_xlsx import fx_export_data_to_excel
from src.utils.watermark import get_watermark, set_watermark


# ── Business logic constants ──────────────────────────────────────

# Business logic constants ----
PRODUCTS_TO_KEEP = [
    "DOTCOM_POSTAGE", "POSTAGE", "COLUMBIAN_CANDLE_ROUND",
    "METAL_SIGN_CUPCAKE_SINGLE_HOOK", "COLOURING_PENCILS_BROWN_TUBE",
    "WHITE_BAMBOO_RIBS_LAMPSHADE", "MODERN_CHRISTMAS_TREE_CANDLE",
    "FAIRY_CAKE_PLACEMATS", "FRENCH_LATTICE_CUSHION_COVER",
    "FRENCH_FLORAL_CUSHION_COVER", "CHARLIE_LOLA_RED_HOT_WATER_BOTTLE",
    "BLUE_FLOCK_GLASS_CANDLEHOLDER", "BROCANTE_SHELF_WITH_HOOKS",
    "BLACK_SILOUETTE_CANDLE_PLATE", "CHERRY_BLOSSOM_DECORATIVE_FLASK",
    "COLUMBIAN_CANDLE_RECTANGLE", "COLUMBIAN_CUBE_CANDLE",
    "BATHROOM_METAL_SIGN", "ACRYLIC_JEWEL_SNOWFLAKE_BLUE",
    "ACRYLIC_JEWEL_SNOWFLAKE_PINK", "EAU_DE_NILE_JEWELLED_PHOTOFRAME",
    "PAPER_LANTERN_9_POINT_SNOW_STAR", "HOME_SWEET_HOME_BLACKBOARD",
    "HEART_T_LIGHT_HOLDER", "FRENCH_PAISLEY_CUSHION_COVER",
    "FROSTED_WHITE_BASE", "GINGHAM_HEART_DECORATION",
    "RETRO_PLASTIC_POLKA_TRAY", "RETRO_PLASTIC_DAISY_TRAY",
    "RETRO_PLASTIC_70_S_TRAY", "PRINTING_SMUDGES_THROWN_AWAY",
    "PINK_JEWELLED_PHOTO_FRAME", "PINK_FLOWERS_RABBIT_EASTER",
    "PINK_FLOCK_GLASS_CANDLEHOLDER", "PINK_FAIRY_CAKE_CUSHION_COVER",
    "PINK_BUTTERFLY_CUSHION_COVER", "PASTEL_PINK_PHOTO_ALBUM",
    "PASTEL_BLUE_PHOTO_ALBUM", "RUSTY_THROW_AWAY",
    "ROUND_BLUE_CLOCK_WITH_SUCKER", "REVERSE_21_5_10_ADJUSTMENT",
    "SWEETHEART_WIRE_WALL_TIDY", "STORAGE_TIN_VINTAGE_LEAF",
    "SQUARE_CHERRY_BLOSSOM_CABINET", "SET_OF_4_FAIRY_CAKE_PLACEMATS",
    "SILVER_VANILLA_FLOWER_CANDLE_POT", "ROSE_DU_SUD_CUSHION_COVER",
    "WATERING_CAN_GREEN_DINOSAUR", "WATERING_CAN_PINK_BUNNY"
]

PRODUCTS_TO_REMOVE = [
    "FBA", "SHOW", "20713", "21494", "22467", "22719", "DIRTY", "RUSTY",
    "SHORT", "17129C", "ADJUST", "DAMGES", "FAULTY", "MANUAL", "CRACKED",
    "DAGAMED", "POSTAGE", "REX_USE", "WET_CTN", "CARRIAGE", "CAT_BOWL",
    "DISCOUNT", "FOR_SHOW", "MISSINGS", "SHOWROOM", "BREAKAGES",
    "CANT_FIND", "SOLD_AS_C", "SOLD_AS_D", "AMAZON_FEE", "CAN_T_FIND",
    "DOTCOM_SET", "RUST_FIXED", "SALE_ERROR", "STOCK_TAKE", "THROW_AWAY",
    "WET_MOULDY", "WRONG_INVC", "21733_MIXED", "BAD_QUALITY",
    "CRUSHED_CTN", "DAMAGES_ETC", "DOTCOM_SETS", "DOTCOMSTOCK",
    "ENTRY_ERROR", "FOUND_AGAIN", "MICHEL_OOPS", "SOLD_AS_A_B",
    "SOLD_IN_SET", "TAIG_ADJUST", "WET_CARTONS", "WET_DAMAGES",
    "WET_ROTTING", "85123A_MIXED", "AMAZON_SALES", "BANK_CHARGES",
    "BROKEN_GLASS", "DOTCOM_EMAIL", "LABEL_MIX_UP", "PHIL_SAID_SO",
    "POOR_QUALITY", "SHOW_DISPLAY", "SHOW_SAMPLES", "SOLD_AS_GOLD",
    "WATER_DAMAGE", "AMAZON_ADJUST", "CAME_AS_GREEN", "CODING_MIX_UP",
    "DAMAGED_DIRTY", "DAMAGED_STOCK", "DOTCOM_ADJUST", "MIX_UP_WITH_C",
    "RE_ADJUSTMENT", "SOLD_AS_17003", "SOLD_AS_22467", "WEBSITE_FIXED",
    "WRONG_BARCODE", "12_S_SOLD_AS_1", "DAMAGES_DOTCOM",
    "DOTCOM_POSTAGE", "FOUND_IN_W_HSE", "INVCD_AS_84879",
    "INVOICE_506647", "WRONG_CTN_SIZE", "BARCODE_PROBLEM",
    "DAMAGES_DISPLAY", "DAMAGES_SAMPLES", "MIXED_WITH_BLUE",
    "MY_ERROR_CONNOR", "OOPS_ADJUSTMENT", "REVERSE_MISTAKE",
    "SAMPLES_DAMAGES", "TEMP_ADJUSTMENT", "AMAZON_SOLD_SETS",
    "INCORRECT_CREDIT", "MOULDY_UNSALEABLE", "RUSTY_CONNECTIONS",
    "RUSTY_THROWN_AWAY", "SOLD_INDIVIDUALLY", "THROWN_AWAY_RUSTY",
    "WRONGLY_SOLD_SETS", "AMAZON_SOLD_AS_SET"
]


# ── Description cleaning ──────────────────────────────────────────

# Fx Clean description ----
def fx_clean_description(df) -> pd.DataFrame:
    print(f"  Before: {df['DESCRIPTION_RAW'].nunique()} unique values in DESCRIPTION_RAW")
    desc = df["DESCRIPTION_RAW"].astype("string")
    cleaned = (
        desc.str.normalize("NFKD")
            .str.replace(r'[^\w]', '_', regex=True)
            .str.replace(r'\s+', '_', regex=True)
            .str.replace(r'^_+|_+$', '', regex=True)
            .str.replace(r'_+', '_', regex=True)
            .str.strip()
    )
    cleaned = cleaned.mask(cleaned == "")
    df["DESCRIPTION_CLEAN"] = cleaned.str.upper().fillna("UNKNOWN")
    print(f"  After:  {df['DESCRIPTION_CLEAN'].nunique()} unique values in DESCRIPTION_CLEAN")
    return df


# ── Best description resolution ───────────────────────────────────

# Fx best description resolution ----
def fx_get_best_description(group) -> str:
    valid = group.dropna()
    if len(valid) == 0:
        return "UNKNOWN"
    mode_result = valid.mode()
    if len(mode_result) == 1:
        return mode_result.iloc[0]
    return max(mode_result, key=len)


# ── Refactored: single generic naming function ────────────────────

# Fx naming product ----
def fx_naming_product(df, source_col: str, target_col: str) -> pd.DataFrame:
    """For each STOCKCODE, fills missing values in source_col with
    the most frequent (or longest) description, writes result to target_col."""
    print(f"\n---------- fx_naming_product: {source_col} → {target_col} ----------")

    df_work = df.copy()
    df_work[source_col] = (
        df_work[source_col]
        .replace("UNKNOWN", "")
        .str.strip()
        .replace("", None)
    )

    best_desc = df_work.groupby("STOCKCODE")[source_col].apply(fx_get_best_description)

    df[target_col] = df_work.apply(
        lambda row: (
            row[source_col]
            if pd.notna(row[source_col])
            else best_desc.get(row["STOCKCODE"], "UNKNOWN")
        ),
        axis=1
    ).fillna("UNKNOWN")

    print(f"  {df[target_col].nunique()} unique values in {target_col}")
    return df


# ── Incremental helper ────────────────────────────────────────────

# Incremental helper ----
def fx_get_existing_stockcodes(conn) -> set:
    """Returns stockcodes already stored in SILVER_PRODUCT_MAPPING."""
    try:
        df = pd.read_sql_query(
            'SELECT DISTINCT STOCKCODE FROM "SILVER_PRODUCT_MAPPING"', conn
        )
        return set(df["STOCKCODE"].tolist())
    except Exception:
        return set()


# ── Exploration dataframes ────────────────────────────────────────

# Fx Build exploration dfs ----
def fx_build_exploration_dfs(df_product) -> tuple:
    """Builds the three exploration dataframes for Excel export."""

    df_base = df_product.drop(columns=["DESCRIPTION_RAW"]).drop_duplicates()

    ## Count products per stockcode ----
    df_count = (
        df_base.groupby("STOCKCODE")["PRODUCT_NAME"]
        .count()
        .reset_index(name="COUNT_PRODUCT_PER_CODE")
    )

    ## Stockcodes with multiple product names ----
    df_multi_product = (
        pd.merge(
            df_count[df_count["COUNT_PRODUCT_PER_CODE"] > 1],
            df_base,
            on="STOCKCODE",
            how="inner"
        )
        .drop_duplicates()
        .sort_values("STOCKCODE")
    )

    ## Product names shared by multiple stockcodes ----
    df_multi_code = (
        df_base.groupby("PRODUCT_NAME")["STOCKCODE"]
        .count()
        .reset_index(name="COUNT_CODE_PER_PRODUCT")
        .pipe(lambda d: d[d["COUNT_CODE_PER_PRODUCT"] > 1])
        .sort_values("COUNT_CODE_PER_PRODUCT", ascending=False)
    )

    return df_count, df_multi_product, df_multi_code


# ── Main logic ────────────────────────────────────────────────────
# Main logic ----
def fx_load_silver_product_mapping(conn):
    print("\n########### Silver Product Mapping ###########")

    # Incremental check — skip if no new stockcodes
    existing_stockcodes = fx_get_existing_stockcodes(conn)

    df_product = pd.read_sql_query(
        'SELECT STOCKCODE, DESCRIPTION AS DESCRIPTION_RAW FROM "SILVER_SALES"', conn
    )

    current_stockcodes = set(df_product["STOCKCODE"].unique())
    new_stockcodes = current_stockcodes - existing_stockcodes

    if not new_stockcodes and existing_stockcodes:
        print("  No new stockcodes found. Skipping.")
        return

    print(f"  {len(new_stockcodes)} new stockcode(s) to process "
          f"(skipping {len(existing_stockcodes)} already known)")


    ## ── Cleaning pipeline ─────────────────────────────────────────
    
    ## Cleaning pipeline ----
    print("\n---------- Clean DESCRIPTION ----------")
    df_product = fx_clean_description(df_product)

    ## Pass 1: initial product name resolution ----
    df_product = fx_naming_product(df_product, "DESCRIPTION_CLEAN", "PRODUCT_NAME")

    ## Remove repeating manual inputs (pass 1) ----
    df_code_count_per_product = (
        df_product.drop(columns=["DESCRIPTION_RAW"])
        .drop_duplicates()
        .groupby("PRODUCT_NAME")["STOCKCODE"]
        .count()
        .reset_index(name="COUNT_CODE_PER_PRODUCT")
    )
    products_to_remove_pass1 = (
        df_code_count_per_product.loc[
            ~df_code_count_per_product["PRODUCT_NAME"].isin(PRODUCTS_TO_KEEP),
            "PRODUCT_NAME"
        ].tolist()
    )
    df_product.loc[
        df_product["PRODUCT_NAME"].isin(products_to_remove_pass1),
        "PRODUCT_NAME"
    ] = np.nan

    ## Pass 2: re-resolve after nulling pass-1 removals ----
    df_product = fx_naming_product(df_product, "PRODUCT_NAME", "PRODUCT_NAME_CLEAN_1")
    df_product = df_product.drop_duplicates().sort_values("STOCKCODE")

    ## Remove non-repeating manual inputs (pass 2)
    df_product["NAME_LENGTH"] = df_product["PRODUCT_NAME_CLEAN_1"].str.len()
    df_product.loc[
        df_product["PRODUCT_NAME_CLEAN_1"].isin(PRODUCTS_TO_REMOVE),
        "PRODUCT_NAME_CLEAN_1"
    ] = np.nan

    ## Pass 3: final re-resolve after nulling pass-2 removals ----
    df_product = fx_naming_product(df_product, "PRODUCT_NAME_CLEAN_1", "PRODUCT_NAME_CLEAN_2")

    ## Final cleanup ----
    df_product = (
        df_product
        .drop(columns=["DESCRIPTION_CLEAN", "PRODUCT_NAME",
                        "PRODUCT_NAME_CLEAN_1", "NAME_LENGTH"])
        .drop_duplicates()
        .sort_values(["STOCKCODE", "PRODUCT_NAME_CLEAN_2"])
        .rename(columns={"PRODUCT_NAME_CLEAN_2": "PRODUCT_NAME"})
    )

    print(f"\n  Final: {df_product['PRODUCT_NAME'].nunique()} unique product names")


    # ── Merge with existing if needed ─────────────────────────────
    ## Merge with existing if needed ----
    if existing_stockcodes:
        df_existing = pd.read_sql_query(
            'SELECT * FROM "SILVER_PRODUCT_MAPPING"', conn
        )
        df_final = (
            pd.concat([df_existing, df_product], ignore_index=True)
            .drop_duplicates()
            .sort_values(["STOCKCODE", "PRODUCT_NAME"])
        )
    else:
        df_final = df_product


    # ── Exploration export ────────────────────────────────────────
    ## Exploration export ----
    df_count, df_multi_product, df_multi_code = fx_build_exploration_dfs(df_final)

    fx_export_data_to_excel(
        {
            "Product and code":       df_final,
            "Count Product per Code": df_count,
            "Multi product per Code": df_multi_product,
            "Multi code per product": df_multi_code
        },
        "silver_pair_code_product",
        "data_exploration"
    )


    # ── Save to database ──────────────────────────────────────────
    ## Save to database ----
    dtype_mapping = {
        "STOCKCODE":       "TEXT",
        "DESCRIPTION_RAW": "TEXT",
        "PRODUCT_NAME":    "TEXT"
    }
    fx_create_table("SILVER", "PRODUCT_MAPPING", df_final, dtype_mapping, conn)

    set_watermark("silver_product_mapping",
                  datetime.now(tz=timezone.utc).isoformat(), "timestamp")
    print(f"  ✓ SILVER_PRODUCT_MAPPING — {len(df_final)} rows total "
          f"({len(df_product)} new).")

# Run ----
def run():
    print("\n########### silver_product_mapping | Start ###########")
    try:
        conn = fx_connect_db()
        with conn:
            fx_load_silver_product_mapping(conn)

        print("=" * 50)
        print("Product mapping completed successfully.")
        print("=" * 50)

    except Exception as error:
        print(f"Error: {error}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run()
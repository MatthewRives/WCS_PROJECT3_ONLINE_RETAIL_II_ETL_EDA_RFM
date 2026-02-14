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
    - When calling the silver table, Must not remove duplicates because of the mode method used later
"""

# 1. Import librairies ----
import sqlite3
import pandas as pd
import numpy as np
import requests

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

# 4. Get list of STOCKCODE and DESCRIPTION from table to df ----
# Must not remove duplicates because of the mode method used later
query = f'SELECT STOCKCODE, DESCRIPTION AS DESCRIPTION_RAW FROM "{table_sales[0]}"'
df_product = pd.read_sql_query(query, conn)



# 5. Create DESCRIOTION_CLEAN column ----
## Create fx_clean_description function ----

def fx_clean_description(df):
    print(f"Before cleaning, there is {df['DESCRIPTION_RAW'].nunique()} unique values in DESCRIPTION RAW column")

    desc = df['DESCRIPTION_RAW'].astype('string')

    cleaned = (desc
        .str.normalize('NFKD')
        .str.replace(r'[^\w]', '_', regex=True) # handle any non alpha num character
        .str.replace(r'\s+', '_', regex=True) # handle multiple spaces by one underscore
        .str.replace(r'^_+|_+$', '', regex=True) # handle both leading and trailing underscores
        .str.replace(r'_+', '_', regex=True) 
        .str.strip()
        )

    cleaned = cleaned.mask(cleaned == '')  # empty → NA
    df['DESCRIPTION_CLEAN'] = cleaned.str.upper()
    df['DESCRIPTION_CLEAN'] = df['DESCRIPTION_CLEAN'].fillna('UNKNOWN')

    print(f"After cleaning, there is {df['DESCRIPTION_CLEAN'].nunique()} unique values in DESCRIPTION CLEAN column")
    return df

## Call fx_clean_description ----
df_product = fx_clean_description(df_product)


# 6. Create PRODUCT_NAME column ----
## Create fx_get_best_description function ----
def fx_get_best_description(group):
    # Function to find the best description per stockcode        
    
    # Get only valid (non-null) descriptions
    valid = group.dropna()

    # CASE 1 : No description for the code
    if len(valid) == 0:
        return "UNKNOWN"
    
    # Find the most frequent value with mode() method
    mode_result = valid.mode()
    
    # CASE 2 : Only one most frequent description
    if len(mode_result) == 1:
        return mode_result.iloc[0]
    
    # CASE 3: Multiple modes (tie) - take the longest one
    else:
        return max(mode_result, key=len)
    
## Create fx_naming_product function ----
def fx_naming_product(df):
    """Filling missing values in DESCRIPTION CLEAN column with the most frequent description for each stockcode. 
    Otherwise with the longest one. 
    Otherwise UNKNOWN."""

    print(f"Before cleaning, there is {df['DESCRIPTION_CLEAN'].nunique()} unique values in DESCRIPTION CLEAN column")

    # Create a working copy and clean the data
    df_work = df.copy()

    # Replace "UNKNOWN" with empty string and strip whitespace
    df_work['DESCRIPTION_CLEAN'] = (df_work['DESCRIPTION_CLEAN']
                                     .replace("UNKNOWN", "")
                                     .str.strip()
                                     .replace("", None))  # Convert empty strings to NaN
        
    # Get best description per stockcode
    best_desc_per_stockcode = df_work.groupby('STOCKCODE')['DESCRIPTION_CLEAN'].apply(fx_get_best_description)

    # Create PRODUCT_NAME column
    # If DESCRIPTION_CLEAN is missing, use the best description for that STOCKCODE
    df['PRODUCT_NAME'] = df_work.apply(
        lambda row: (row['DESCRIPTION_CLEAN'] 
                     if pd.notna(row['DESCRIPTION_CLEAN']) 
                     else best_desc_per_stockcode.get(row['STOCKCODE'], "UNKNOWN")), axis=1)
   
   # Final fallback to UNKNOWN
    df['PRODUCT_NAME'] = df['PRODUCT_NAME'].fillna("UNKNOWN")
    
    print(f"After cleaning, there is {df['PRODUCT_NAME'].nunique()} unique values in PRODUCT NAME column")

    return df

## Call fx_naming_product ----
df_product = fx_naming_product(df_product)


# 7. Drop duplicates after use of mode() ----
df_product = df_product.drop(columns = ["DESCRIPTION_CLEAN"]).drop_duplicates().sort_values(by="STOCKCODE", ascending=True)


# 8. Data exploration ----
## Count product per code ----
df_count_product_per_code = df_product.copy()

df_count_product_per_code = df_count_product_per_code.drop(columns = ["DESCRIPTION_RAW"]).drop_duplicates()

df_count_product_per_code = df_count_product_per_code.groupby('STOCKCODE')['PRODUCT_NAME'].count().reset_index(name='COUNT_PRODUCT_PER_CODE')


## Count code with count product > 1 ----
df_multi_product_per_code = df_count_product_per_code[df_count_product_per_code['COUNT_PRODUCT_PER_CODE'] > 1].copy()

df_multi_product_per_code = pd.merge(
    df_multi_product_per_code,
    df_product,
    on="STOCKCODE",
    how="inner"
    ).drop(columns=["DESCRIPTION_RAW"]).drop_duplicates().sort_values(by='STOCKCODE', ascending=True)


## Count product with count code > 1 ----
# Find too much common product name, sign of manual entry
df_code_count_per_product = df_product.copy()

df_code_count_per_product = df_code_count_per_product.drop(columns = ["DESCRIPTION_RAW"]).drop_duplicates()

df_code_count_per_product = df_code_count_per_product.groupby('PRODUCT_NAME')['STOCKCODE'].count().reset_index(name='COUNT_CODE_PER_PRODUCT')

df_code_count_per_product = df_code_count_per_product[df_code_count_per_product['COUNT_CODE_PER_PRODUCT'] > 1].sort_values(by="COUNT_CODE_PER_PRODUCT", ascending=False)


# 9. [ITERATIVE] Find and remove repeating manual inputs ----

## From data exploration, get description to keep ----
# check the file and MULTI CODE PER PRODUCT


# List of correct product: 
product_to_keep = [
    "DOTCOM_POSTAGE",
    "POSTAGE",
    "COLUMBIAN_CANDLE_ROUND",
    "METAL_SIGN_CUPCAKE_SINGLE_HOOK",
    "COLOURING_PENCILS_BROWN_TUBE",
    "WHITE_BAMBOO_RIBS_LAMPSHADE",
    "MODERN_CHRISTMAS_TREE_CANDLE",
    "FAIRY_CAKE_PLACEMATS",
    "FRENCH_LATTICE_CUSHION_COVER",
    "FRENCH_FLORAL_CUSHION_COVER",
    "CHARLIE_LOLA_RED_HOT_WATER_BOTTLE",
    "BLUE_FLOCK_GLASS_CANDLEHOLDER",
    "BROCANTE_SHELF_WITH_HOOKS",
    "BLACK_SILOUETTE_CANDLE_PLATE",
    "CHERRY_BLOSSOM_DECORATIVE_FLASK",
    "COLUMBIAN_CANDLE_RECTANGLE",
    "COLUMBIAN_CUBE_CANDLE",
    "BATHROOM_METAL_SIGN",
    "ACRYLIC_JEWEL_SNOWFLAKE_BLUE",
    "ACRYLIC_JEWEL_SNOWFLAKE_PINK",
    "EAU_DE_NILE_JEWELLED_PHOTOFRAME",
    "PAPER_LANTERN_9_POINT_SNOW_STAR",
    "HOME_SWEET_HOME_BLACKBOARD",
    "HEART_T_LIGHT_HOLDER",
    "FRENCH_PAISLEY_CUSHION_COVER",
    "FROSTED_WHITE_BASE",
    "GINGHAM_HEART_DECORATION",
    "RETRO_PLASTIC_POLKA_TRAY",
    "RETRO_PLASTIC_DAISY_TRAY",
    "RETRO_PLASTIC_70_S_TRAY",
    "PRINTING_SMUDGES_THROWN_AWAY",
    "PINK_JEWELLED_PHOTO_FRAME",
    "PINK_FLOWERS_RABBIT_EASTER",
    "PINK_FLOCK_GLASS_CANDLEHOLDER",
    "PINK_FAIRY_CAKE_CUSHION_COVER",
    "PINK_BUTTERFLY_CUSHION_COVER",
    "PASTEL_PINK_PHOTO_ALBUM",
    "PASTEL_BLUE_PHOTO_ALBUM",
    "RUSTY_THROW_AWAY",
    "ROUND_BLUE_CLOCK_WITH_SUCKER",
    "REVERSE_21_5_10_ADJUSTMENT",
    "SWEETHEART_WIRE_WALL_TIDY",
    "STORAGE_TIN_VINTAGE_LEAF",
    "SQUARE_CHERRY_BLOSSOM_CABINET",
    "SET_OF_4_FAIRY_CAKE_PLACEMATS",
    "SILVER_VANILLA_FLOWER_CANDLE_POT",
    "ROSE_DU_SUD_CUSHION_COVER",
    "WATERING_CAN_GREEN_DINOSAUR",
    "WATERING_CAN_PINK_BUNNY"]


## Build removal list ----
products_to_remove = (
    df_code_count_per_product.loc[
        ~df_code_count_per_product["PRODUCT_NAME"].isin(product_to_keep),
        "PRODUCT_NAME"
    ].tolist()
    )


## Null out unwanted manual inputs (#1) ----
df_product.loc[df_product["PRODUCT_NAME"].isin(products_to_remove), "PRODUCT_NAME"] = np.nan


# 10. Clean product name (#1) ----
## Create fx_naming_product_clean function ----
def fx_naming_product_clean(df):

    df_work = df.copy()

    # Get best description per stockcode
    best_desc_per_stockcode = df_work.groupby('STOCKCODE')['PRODUCT_NAME'].apply(fx_get_best_description)

    # Create PRODUCT_NAME_CLEAN column
    # If PRODUCT_NAME is missing, use the best description for that STOCKCODE
    df['PRODUCT_NAME_CLEAN_1'] = df_work.apply(
        lambda row: (row['PRODUCT_NAME'] 
                     if pd.notna(row['PRODUCT_NAME']) 
                     else best_desc_per_stockcode.get(row['STOCKCODE'], "UNKNOWN")), axis=1)
   
   # Final fallback to UNKNOWN
    df['PRODUCT_NAME_CLEAN_1'] = df['PRODUCT_NAME_CLEAN_1'].fillna("UNKNOWN")
    
    print(f"After cleaning, there is {df['PRODUCT_NAME_CLEAN_1'].nunique()} unique values in PRODUCT NAME column")

    return df


## Call fx_naming_product_clean ---
df_product = fx_naming_product_clean(df_product)


## Remove duplicates ----
df_product = df_product.drop_duplicates().sort_values(by="STOCKCODE", ascending=True)


# 11. [ITERATIVE] Find and remove non-repeating manual inputs ----
## Get the length of product names ----
df_product['NAME_LENGTH'] = df_product['PRODUCT_NAME_CLEAN_1'].str.len()
df_product = df_product.sort_values(by='NAME_LENGTH', ascending=True)

## Create a listo of manual inputs from data exploration ----
products_to_remove = [
    "FBA",
    "SHOW",
    "20713",
    "21494",
    "22467",
    "22719",
    "DIRTY",
    "RUSTY",
    "SHORT",
    "17129C",
    "ADJUST",
    "DAMGES",
    "FAULTY",
    "MANUAL",
    "CRACKED",
    "DAGAMED",
    "POSTAGE",
    "REX_USE",
    "WET_CTN",
    "CARRIAGE",
    "CAT_BOWL",
    "DISCOUNT",
    "FOR_SHOW",
    "MISSINGS",
    "SHOWROOM",
    "BREAKAGES",
    "CANT_FIND",
    "SOLD_AS_C",
    "SOLD_AS_D",
    "AMAZON_FEE",
    "CAN_T_FIND",
    "DOTCOM_SET",
    "RUST_FIXED",
    "SALE_ERROR",
    "STOCK_TAKE",
    "THROW_AWAY",
    "WET_MOULDY",
    "WRONG_INVC",
    "21733_MIXED",
    "BAD_QUALITY",
    "CRUSHED_CTN",
    "DAMAGES_ETC",
    "DOTCOM_SETS",
    "DOTCOMSTOCK",
    "ENTRY_ERROR",
    "FOUND_AGAIN",
    "MICHEL_OOPS",
    "SOLD_AS_A_B",
    "SOLD_IN_SET",
    "TAIG_ADJUST",
    "WET_CARTONS",
    "WET_DAMAGES",
    "WET_ROTTING",
    "85123A_MIXED",
    "AMAZON_SALES",
    "BANK_CHARGES",
    "BROKEN_GLASS",
    "DOTCOM_EMAIL",
    "LABEL_MIX_UP",
    "PHIL_SAID_SO",
    "POOR_QUALITY",
    "SHOW_DISPLAY",
    "SHOW_SAMPLES",
    "SOLD_AS_GOLD",
    "WATER_DAMAGE",
    "AMAZON_ADJUST",
    "CAME_AS_GREEN",
    "CODING_MIX_UP",
    "DAMAGED_DIRTY",
    "DAMAGED_STOCK",
    "DOTCOM_ADJUST",
    "MIX_UP_WITH_C",
    "RE_ADJUSTMENT",
    "SOLD_AS_17003",
    "SOLD_AS_22467",
    "WEBSITE_FIXED",
    "WRONG_BARCODE",
    "12_S_SOLD_AS_1",
    "DAMAGES_DOTCOM",
    "DOTCOM_POSTAGE",
    "FOUND_IN_W_HSE",
    "INVCD_AS_84879",
    "INVOICE_506647",
    "WRONG_CTN_SIZE",
    "BARCODE_PROBLEM",
    "DAMAGES_DISPLAY",
    "DAMAGES_SAMPLES",
    "MIXED_WITH_BLUE",
    "MY_ERROR_CONNOR",
    "OOPS_ADJUSTMENT",
    "REVERSE_MISTAKE",
    "SAMPLES_DAMAGES",
    "TEMP_ADJUSTMENT",
    "AMAZON_SOLD_SETS",
    "INCORRECT_CREDIT",
    "MOULDY_UNSALEABLE",
    "RUSTY_CONNECTIONS",
    "RUSTY_THROWN_AWAY",
    "SOLD_INDIVIDUALLY",
    "THROWN_AWAY_RUSTY",
    "WRONGLY_SOLD_SETS",
    "AMAZON_SOLD_AS_SET"]

## Null out unwanted manual inputs (#2) ----
df_product.loc[df_product["PRODUCT_NAME_CLEAN_1"].isin(products_to_remove), "PRODUCT_NAME_CLEAN_1"] = np.nan


# 12. Clean product name (#2) ----
## Create fx_naming_product_clean function ----
def fx_naming_product_clean(df):

    df_work = df.copy()

    # Get best description per stockcode
    best_desc_per_stockcode = df_work.groupby('STOCKCODE')['PRODUCT_NAME_CLEAN_1'].apply(fx_get_best_description)

    # Create PRODUCT_NAME_CLEAN column
    # If PRODUCT_NAME is missing, use the best description for that STOCKCODE
    df['PRODUCT_NAME_CLEAN_2'] = df_work.apply(
        lambda row: (row['PRODUCT_NAME_CLEAN_1'] 
                     if pd.notna(row['PRODUCT_NAME_CLEAN_1']) 
                     else best_desc_per_stockcode.get(row['STOCKCODE'], "UNKNOWN")), axis=1)
   
   # Final fallback to UNKNOWN
    df['PRODUCT_NAME_CLEAN_2'] = df['PRODUCT_NAME_CLEAN_2'].fillna("UNKNOWN")
    
    print(f"After cleaning, there is {df['PRODUCT_NAME_CLEAN_2'].nunique()} unique values in PRODUCT NAME column")

    return df

## Call fx_naming_product_clean ----
df_product = fx_naming_product_clean(df_product).drop(columns = ['PRODUCT_NAME', 'PRODUCT_NAME_CLEAN_1', 'NAME_LENGTH'])

## Remove duplicates ----
df_product = df_product.drop_duplicates().sort_values(by=["STOCKCODE", "PRODUCT_NAME_CLEAN_2"], ascending=True).rename(columns={"PRODUCT_NAME_CLEAN_2": "PRODUCT_NAME"})

print(f"After cleaning, there is {df_product['PRODUCT_NAME'].nunique()} unique values in PRODUCT_NAME column")



# 13. Export to excel ----
dict_data_to_export = {
    "Product and code": df_product,
    "Count Product per Code": df_count_product_per_code,
    "Multi product per Code": df_multi_product_per_code,
    "Multi code per product": df_code_count_per_product
    }

fx_export_data_to_excel(dict_data_to_export, "silver_pair_code_product", "data_exploration")


# 14. Create SILVER_PRODUCT_MAPPING table ----
## Mapping dtype ----
dtype_mapping = {
    'STOCKCODE': 'TEXT', 
    'DESCRIPTION_RAW': 'TEXT',
    'PRODUCT_NAME':'TEXT'
    }

## Call fx_create_table ----
create_silver_product_mapping = fx_create_table("SILVER", "PRODUCT_MAPPING", df_product, dtype_mapping, conn)
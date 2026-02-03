"""
=============================================================
Explore Bronze tables
=============================================================
Script purpose:
    This script explores the tables and their data from the bronze layer. Allows us to evaluate which transformation are required for the silver layer. 

Process:
    01. Connect to the database located ../datasets/database (it should be named DATAWAREHOUSE_ONLINE_RETAIL_II)
    02. Get all tables in the DB
    03. For each table:
        - Create a DF with it
        - Add the DF to a DF list
    04. Close connection with DB
    05. Concatenate the DFs in the DF list
    06. Display DF exploration
    07. Data exploration
    08. Display data variance map in a new window (must be closed for the script to resume)
    09. Display correlation heatmap in a new window (must be closed for the script to resume)
    10. Display list of top and lowest correlation
    End of process

List of functions used: 
    - fx_connect_db : connect to the database, imported from connection_to_database.py
    - fx_df_exploration : 
        - fx_share_of_value : 
        - fx_col_fulfilling_rate : 
        - fx_need_trim : 
    - fx_data_exploration : 
    - fx_df_exploration_variance_graph : 
    - fx_correlation_matrix_plot : 

Potential improvements: 
    - Export graphs to folder ../_img to insert them in the documentation
        
WARNING:
    The script open an external window for the graphs. 
    The first graph window must be closed for the script to resume. 
    Same for the second graph window. 
"""




import sqlite3
import pandas as pd
from tabulate import tabulate
from connecting_to_database import fx_connect_db
import datetime as dt
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

pd.set_option("display.max_columns", None)
# pd.set_option("display.max_rows", None)
pd.set_option("display.float_format", lambda x: '%.3f' % x)



conn = fx_connect_db()
cursor = conn.cursor()


# Get all tables in the DB
cursor.execute("""
SELECT name
FROM sqlite_master
WHERE type='table'
ORDER BY name;
""")

tables = [row[0] for row in cursor.fetchall()]


# For each table, create a DF and add it to a df list
df_list = []
for table in tables:
    query = f'SELECT * FROM "{table}"'
    df_query = pd.read_sql_query(query, conn)
    df_list.append(df_query)

# Concatenate the dfs from the df list
df = pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()


conn.close()


# ---
def fx_col_fulfilling_rate(df):
    rates = {}
    for col in df.columns:
        fill_rate = round((1 - df[col].isna().sum() / len(df[col])) * 100,2)
        rates[col] = fill_rate
    for key, values in rates.items():
        print(f"{key:15} --> {values:8.2f}%")


# ---
def fx_share_of_value(df):
    for col in df.columns:
        print(f"\n{col}:")
        print("-" * 40)
        
        shares = df[col].value_counts(normalize=True) * 100

        # Show top 20 values, but at least those >5%
        top_20 = shares.head(20)
        filtered = shares[shares > 5]
        to_show = top_20 if len(top_20) >= len(filtered) else filtered    

        for value, share in to_show.items():
            print(f"{str(value):20} --> {share:8.2f}%")


# ---
def fx_need_trim(df):
    for col in df.columns:
        needs_trim = df[col].dropna().apply(
                lambda x: isinstance(x, str) and (x.startswith(' ') or x.endswith(' '))
            ).sum()
        print(f"{col:15} --> {needs_trim:8.2f}")


# ---
def fx_df_exploration(df):
    print("\n########### Head ###########")
    print(df.head())

    print("\n########### Tail ###########")
    print(df.tail())

    print("\n########### Shape ###########")
    print(df.shape)

    print("\n########### Info df ###########")
    print(df.info())

    print("\n########### Info col ###########")
    cols = df.columns.tolist()
    print(cols)

    print("\n########### Describe ###########")
    print(df.describe(include="all").T)

    print("\n########### NA ###########")
    print(df.isnull().sum())

    print("\n########### Fullfilling rates ###########")
    print(fx_col_fulfilling_rate(df))

    print("\n########### Need trim ###########")
    print(fx_need_trim(df))    


# ---
def fx_data_exploration(df):
    print("\n########### Unique Customers ###########")
    count_unique_customer = df["CUSTOMER_ID"].nunique()
    print("The number of customer:", count_unique_customer)

    print("\n########### Unique Invoices ###########")
    count_unique_invoice = df["INVOICE"].nunique()
    print("The number of invoice:", count_unique_invoice)

    print("\n########### Sold quantity ###########")
    sold_quantity = df["QUANTITY"].sum()
    print("The sum of product sold:", sold_quantity)

    print("\n########### Total revenue ###########")
    total_revenue = ((df["QUANTITY"] * df["PRICE"]).sum()).astype(float)
    print("The sum of sales (quantity * price):", total_revenue)

    print("\n########### Average Order Value ###########")
    aov = round(total_revenue / count_unique_invoice,2)
    print("The AOV:", aov)

    print("\n########### Unique products ###########")
    count_unique_product = df["DESCRIPTION"].nunique()
    print("The sum of product in the dataset:", count_unique_product)

    print("\n########### Highest prices ###########")
    print("Our dataset, sorted by highest price")
    print(df.sort_values(by = "PRICE", ascending  = False).head())

    print("\n########### Lowest prices ###########")
    print("Our dataset, sorted by lowest price")
    print(df.sort_values(by = "PRICE", ascending  = True).head())

    print("\n########### Share of each value ###########")
    print(fx_share_of_value(df))


# ---
def fx_df_exploration_variance_graph(df):
    """The features in the dataset with a skewness of 0 shows a symmetrical distribution. If the skewness is 1 or above it suggests a positively skewed (right-skewed) distribution. In a right-skewed distribution the tail extends more to the right which shows the presence of extremely high values."""

    sns.set_style("darkgrid")
    numerical_columns = df.select_dtypes(include=["int64", "float64"]).columns

    plt.figure(figsize=(8, len(numerical_columns) * 3))
    for idx, feature in enumerate(numerical_columns, 1):
        plt.subplot(len(numerical_columns), 2, idx)
        sns.histplot(df[feature], kde=True)
        plt.title(f"{feature} | Skewness: {round(df[feature].skew(), 2)}")

    plt.tight_layout()
    plt.show()


# ---
def fx_correlation_matrix_plot(df):
    numeric_cols = df.select_dtypes(include='number') 
    corr_matrix = numeric_cols.corr()
    mask = np.triu(np.ones_like(corr_matrix, dtype=bool), k=1)

    plt.figure(figsize=(8, 5))

    sns.heatmap(corr_matrix, annot=True, fmt='.2f', cmap='Pastel2', mask=mask, linewidths=2)

    plt.title('Correlation Heatmap')
    plt.show()

    # The mask exclude auto-correlated features where corr == 1
    corr_series = corr_matrix.where(mask)
    
    # Transform correlation matrix into a 1D serie
    corr_series = corr_series.unstack()

    # Remove NaN values
    corr_series = corr_series.dropna()

    top5_pos = corr_series.sort_values(ascending=False).head(5)
    print("\n########### Top 5 highest correlation ###########")
    print(top5_pos)

    # Trier pour les corrélations négatives les plus fortes
    top5_neg = corr_series.sort_values(ascending=True).head(5)
    print("\n########### Top 5 lowest correlation ###########")
    print(top5_neg)



# LIST OF EXPLORATION FUNCTION TO ACTIVATE

print(fx_df_exploration(df))
print(fx_data_exploration(df))
print(fx_df_exploration_variance_graph(df))
print(fx_correlation_matrix_plot(df))



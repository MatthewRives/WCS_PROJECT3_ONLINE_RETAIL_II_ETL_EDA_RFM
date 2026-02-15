"""
=============================================================
Function: Generic Data exploration
=============================================================
Script purpose:
    ...

Process:
    01. ...
    End of process

List of functions used: 
    - ...

Potential improvements: 
    - Not determined yet
        
WARNING:
    ...
"""

# 1. Import librairies  ----
# import os

import io

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
# import xlsxwriter
import re 


# 2. Create generic functions ----

## Create fx head ----
def fx_explo_head(df, size=100):
    print(f"\n---------- Head (first {size} rows) ----------")
    return df.head(size)

## Create fx tail ----
def fx_explo_tail(df, size=100):
    print(f"\n---------- Tail (last {size} rows) ----------")
    return df.tail(size)

## Create fx sample ----
def fx_explo_sample(df, size=100):
    print(f"\n---------- Sample (random {size} rows) ----------")
    return df.sample(size) 

## Create fx df info ----
def fx_explo_info(df):
    info_data = []
    for col in df.columns:
        info_data.append({
            "Column": col,
            "Non-Null Count": df[col].notna().sum(),
            "Null Count": df[col].isna().sum(),
            "Dtype": str(df[col].dtype)
        })
    
    info_df = pd.DataFrame(info_data)

    summary = pd.DataFrame({
        "Column": ["Total Entries", "Total Columns", "Memory Usage"],
        "Non-Null Count": [len(df), len(df.columns), ""],
        "Null Count": ["", "", ""],
        "Dtype": ["", "", f"{df.memory_usage(deep=True).sum() / 1024**2:.1f} MB"]
    })
    
    return pd.concat([summary, info_df], ignore_index=True)

## Create fx describe ----
def fx_explo_describe(df):
    print(f"\n---------- Describe ----------")
    return df.describe(include = "all").T

## Create fx fulfilling rate ----
def fx_explo_fulfilling_rate(df):
    print(f"\n---------- Fulfilling rates ----------")
    df_fullfill_rate = []
    for col in df.columns:
        fill_rate = round((1 - df[col].isna().sum() / len(df[col])) * 100, 2)
        df_fullfill_rate.append({
            "col": col,
            "fill_rate": fill_rate
        })
    return pd.DataFrame(df_fullfill_rate)

## Create fx missing rate ----
def fx_explo_missing_rate(df):
    print(f"\n---------- Missing rates ----------")
    df_missing_rate = []
    for col in df.columns:
        null_rate = round(df[col].isna().sum() / len(df[col]) * 100, 2)
        df_missing_rate.append({
                "col": col,
                "null_rate": null_rate
            })
    return pd.DataFrame(df_missing_rate) 

## Create fx share of values ----
def fx_explo_share_of_value(df):
    print(f"\n---------- Share of value ----------")
    df_share_of_value = []
    for col in df.columns:  
        shares = df[col].value_counts(normalize=True) * 100
        top_20 = shares.head(20)
        filtered = shares[shares > 5]
        to_show = top_20 if len(top_20) >= len(filtered) else filtered    
        for value, share in to_show.items():
            df_share_of_value.append({
                "col": col,
                "value": value,
                "share": share
            })
    return pd.DataFrame(df_share_of_value)

## Create fx need trim ----
def fx_explo_need_trim(df):
    print(f"\n---------- Need trim ----------")
    # Create a df with the number of values that need to be trimmed for each column
    df_col_to_trim = []
    for col in df.columns:
        needs_trim = df[col].dropna().apply(lambda x: isinstance(x, str) and (x.startswith(' ') or x.endswith(' '))).sum()
        df_col_to_trim.append({
            "col": col,
            "needs_trim": needs_trim
        })
    return pd.DataFrame(df_col_to_trim)

## Create fx cols to lists ----
def fx_explo_cols_to_list(df):
    print(f"\n---------- Columns list ----------")
    return df.columns.to_list()

## Create fx variance graph ----
def fx_explo_variance_graph(df):
    print(f"\n---------- Variance graph ----------")
    """The features in the dataset with a skewness of 0 shows a symmetrical distribution. If the skewness is 1 or above it suggests a positively skewed (right-skewed) distribution. In a right-skewed distribution the tail extends more to the right which shows the presence of extremely high values."""

    sns.set_style("darkgrid")
    numerical_columns = df.select_dtypes(include=["int64", "float64"]).columns

    plt.figure(figsize=(8, len(numerical_columns) * 3))
    for idx, feature in enumerate(numerical_columns, 1):
        plt.subplot(len(numerical_columns), 2, idx)
        sns.histplot(df[feature], kde=True)
        plt.title(f"{feature} | Skewness: {round(df[feature].skew(), 2)}")

    plt.tight_layout()
    fig = plt.gcf() #get current figure

    return fig

## Create fx correlation matrix graph ----
def fx_explo_correlation_matrix_plot(df):
    print(f"\n---------- Correlation matrix ----------")
    numeric_cols = df.select_dtypes(include='number') 
    corr_matrix = numeric_cols.corr()
    mask = np.triu(np.ones_like(corr_matrix, dtype=bool), k=1)

    plt.figure(figsize=(8, 5))

    sns.heatmap(corr_matrix, annot=True, fmt='.2f', cmap='Pastel2', mask=mask, linewidths=2)

    plt.title('Correlation Heatmap')
    fig = plt.gcf() #get current figure

    return fig

## Create fx correlation matrix series ----
def fx_explo_correlation_matrix_series(df):
    print(f"\n---------- Correlation matrix series ----------")
    numeric_cols = df.select_dtypes(include='number')
    corr_matrix = numeric_cols.corr()
    mask = np.triu(np.ones_like(corr_matrix, dtype=bool), k=1)

    # The mask exclude auto-correlated features where corr == 1
    corr_series = corr_matrix.where(mask)
    
    # Transform correlation matrix into a 1D serie
    corr_series = corr_series.unstack()

    # Remove NaN values
    corr_series = corr_series.dropna()

    top10_pos = corr_series.sort_values(ascending=False).head(10)
    top10_neg = corr_series.sort_values(ascending=True).head(10)

    df_top10_pos = pd.DataFrame(top10_pos, columns=['correlation']).reset_index().rename(columns={'level_0': 'feature_1', 'level_1': 'feature_2'})
    df_top10_neg = pd.DataFrame(top10_neg, columns=['correlation']).reset_index().rename(columns={'level_0': 'feature_1', 'level_1': 'feature_2'})

    return df_top10_pos, df_top10_neg


# 3. Combine all generic explo functions ----
def fx_generic_explo_dictionnary(df, size = 100):
    print(f"\n########### Generic exploration ###########")
    
    # Basics
    df_head = fx_explo_head(df, size)
    df_tail = fx_explo_tail(df, size)
    df_sample = fx_explo_sample(df, size)
    df_info = fx_explo_info(df)
    list_col = fx_explo_cols_to_list(df)
    df_describe = fx_explo_describe(df)

    # Missing values
    df_missing_rates = fx_explo_missing_rate(df)
    df_fullfill_rates = fx_explo_fulfilling_rate(df)

    # Need correction (trim, case, etc.)
    df_trim = fx_explo_need_trim(df)

    # Data variance
    df_share_value = fx_explo_share_of_value(df)
    graph_variance = fx_explo_variance_graph(df)

    # Correlations
    graph_corr_matrix = fx_explo_correlation_matrix_plot(df)
    df_top10_pos, df_top10_neg = fx_explo_correlation_matrix_series(df)

    print(f"\n########### Generic exploration result in dictionnary ###########")
    dictionnary_generic_exploration = {
        "Head": df_head,
        "Tail": df_tail,
        "Sample": df_sample,
        "Info": df_info,
        "Col_list": list_col,
        "Describe": df_describe,
        "Missing_rates": df_missing_rates,
        "Fullfill_rates": df_fullfill_rates,
        "Trim_me": df_trim,
        "Share_value": df_share_value,
        "Variance": graph_variance,
        "Corr_Matrix": graph_corr_matrix,
        "Corr_Series_Pos": df_top10_pos,
        "Corr_Series_Neg": df_top10_neg
    }

    return dictionnary_generic_exploration
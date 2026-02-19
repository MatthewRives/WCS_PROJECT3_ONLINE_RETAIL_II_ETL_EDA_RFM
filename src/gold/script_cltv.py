"""
=============================================================
...
=============================================================
Script purpose:
    This script

Process:
    01. Connect to the database located ../data/database (it should be named DATAWAREHOUSE_ONLINE_RETAIL_II)
    02. 
    End of process

List of functions used: 
    - fx_connect_db : connect to the database, imported from connection_to_database.py
    - 

Potential improvements: 
    - Not determined yet

WARNING:

"""

# 1. Import librairies ----
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import warnings
warnings.filterwarnings("ignore")

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn import metrics
import matplotlib.pyplot as plt

from src.utils.connecting_to_database import fx_connect_db
from src.utils.create_table import fx_create_table
from src.utils.export_data_to_xlsx import fx_export_data_to_excel
from src.utils.watermark import get_watermark, set_watermark

# ── Data loading ──────────────────────────────────────────────────

# 2. Data loading ----
## Fx load and clean sales ----
def fx_load_and_clean_sales(conn) -> pd.DataFrame:
    """Loads and cleans GOLD_FACT_SALES for CLTV modeling."""
    print("\n########### Load GOLD_FACT_SALES ###########")

    df = pd.read_sql_query('SELECT * FROM "GOLD_FACT_SALES"', conn)
    print(f"  Raw shape: {df.shape}")

    # Quality report
    df_quality = pd.DataFrame({
        "Check": ["Negative Quantities", "Negative Prices",
                  "Zero Quantities", "Zero Prices"],
        "Count": [
            (df["QUANTITY"] < 0).sum(), (df["PRICE"] < 0).sum(),
            (df["QUANTITY"] == 0).sum(), (df["PRICE"] == 0).sum()
        ]
    })
    df_quality["Percentage"] = (
        df_quality["Count"] / len(df) * 100
    ).round(2)
    print(f"\nQuality issues:\n{df_quality}")

    # Filter
    initial = len(df)
    df = df[df["QUANTITY"] > 0]
    df = df[df["PRICE"] > 0]
    df = df[df["CUSTOMER_ID"] != "UNKNOWN"]
    df = df.dropna(subset=["CUSTOMER_ID", "INVOICE_DATE", "REVENUE"])
    print(f"  Rows removed: {initial - len(df)} | Final shape: {df.shape}")

    df["INVOICE_DATE"] = pd.to_datetime(df["INVOICE_DATE"])
    df["YEAR_MONTH"] = df["INVOICE_DATE"].dt.to_period("M").astype(str)
    df["CUSTOMER_ID"] = df["CUSTOMER_ID"].astype(str)

    return df


## Fx load and clean RFM ----
def fx_load_and_clean_rfm(conn) -> pd.DataFrame:
    """Loads and cleans GOLD_DIM_CUSTOMER_RFM."""
    print("\n########### Load GOLD_DIM_CUSTOMER_RFM ###########")

    df = pd.read_sql_query('SELECT * FROM "GOLD_DIM_CUSTOMER_RFM"', conn)
    print(f"  Raw shape: {df.shape}")

    initial = len(df)
    df = df[df["CUSTOMER_ID"] != "UNKNOWN"]
    df["CUSTOMER_ID"] = df["CUSTOMER_ID"].astype(str)
    print(f"  Rows removed: {initial - len(df)} | Final shape: {df.shape}")

    return df

# ── Feature engineering ───────────────────────────────────────────

# 3. Feature engineering ----
## Fx Create time features ----
def fx_create_time_features(df_sales, df_rfm) -> tuple:
    """Creates time-based features and merges with RFM."""
    print("\n########### Time-Based Features ###########")

    monthly_revenue = df_sales.pivot_table(
        index="CUSTOMER_ID",
        columns="YEAR_MONTH",
        values="REVENUE",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    month_cols = monthly_revenue.columns[1:]
    n_months = len(month_cols)
    print(f"  Date range: {month_cols[0]} → {month_cols[-1]} ({n_months} months)")

    features = monthly_revenue[["CUSTOMER_ID"]].copy()

    ### Overall stats ----
    features["AVG_MONTHLY_REVENUE"] = monthly_revenue[month_cols].mean(axis=1)
    features["STD_MONTHLY_REVENUE"] = monthly_revenue[month_cols].std(axis=1)
    features["MAX_MONTHLY_REVENUE"] = monthly_revenue[month_cols].max(axis=1)

    ### Period splits ----
    first_half  = month_cols[:n_months // 2]
    second_half = month_cols[n_months // 2:]
    recent_3m   = month_cols[-3:]
    early_3m    = month_cols[:3]

    features["FIRST_HALF_REVENUE"]  = monthly_revenue[first_half].sum(axis=1)
    features["SECOND_HALF_REVENUE"] = monthly_revenue[second_half].sum(axis=1)
    features["RECENT_3M_REVENUE"]   = monthly_revenue[recent_3m].sum(axis=1)
    features["EARLY_3M_REVENUE"]    = monthly_revenue[early_3m].sum(axis=1)

    features["REVENUE_TREND"] = (
        monthly_revenue[second_half].mean(axis=1) -
        monthly_revenue[first_half].mean(axis=1)
    )
    features["GROWTH_RATE"] = (
        features["SECOND_HALF_REVENUE"] / (features["FIRST_HALF_REVENUE"] + 1) - 1
    )

    features["MONTHS_ACTIVE"]         = (monthly_revenue[month_cols] > 0).sum(axis=1)
    features["ACTIVITY_RATE"]         = features["MONTHS_ACTIVE"] / n_months
    features["PURCHASE_CONSISTENCY"]  = (
        1 - features["STD_MONTHLY_REVENUE"] /
        (features["AVG_MONTHLY_REVENUE"] + 1)
    )

    df_features = features.merge(df_rfm, on="CUSTOMER_ID", how="left")
    print(f"  Combined features shape: {df_features.shape}")

    return df_features, monthly_revenue, month_cols


# ── Target variable ───────────────────────────────────────────────

# 4. Target variable ----
## Fx create target ----
def fx_create_target(monthly_revenue, month_cols, train_months=12) -> pd.Series:
    """Creates target variable from future revenue."""
    print("\n########### Target Variable ###########")

    target_months = month_cols[train_months:]
    if len(target_months) == 0:
        raise ValueError(f"Not enough data — need more than {train_months} months.")

    print(f"  Training period: {month_cols[0]} → {month_cols[train_months - 1]}")
    print(f"  Target period:   {target_months[0]} → {target_months[-1]}")

    y = monthly_revenue[target_months].sum(axis=1)

    cap_value = y.quantile(0.95)
    y_capped = y.clip(upper=cap_value)
    print(f"  Capping at 95th percentile: £{cap_value:.2f} "
          f"({(y > cap_value).sum()} values capped)")

    return y_capped

# ── Model training ────────────────────────────────────────────────

def fx_train_and_evaluate_models(X, y) -> tuple:
    """Trains multiple models and returns results and predictions."""
    print("\n########### Model Training ###########")

    X = X.loc[:, ~X.columns.duplicated()]
    if X.isnull().any().any():
        print("  WARNING: Missing values detected — filling with 0.")
        X = X.fillna(0)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    print(f"  Train: {len(X_train)} | Test: {len(X_test)}")

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled  = scaler.transform(X_test)

    models = {
        "Linear Regression":   LinearRegression(),
        "Ridge":               Ridge(alpha=1.0),
        "Lasso":               Lasso(alpha=1.0),
        "Random Forest":       RandomForestRegressor(
                                   n_estimators=100, random_state=42, max_depth=10),
        "Gradient Boosting":   GradientBoostingRegressor(
                                   n_estimators=100, random_state=42, max_depth=5)
    }

    tree_models = {"Random Forest", "Gradient Boosting"}
    results, predictions = [], {}

    for name, model in models.items():
        print(f"\n  --- {name} ---")
        is_tree = name in tree_models
        X_tr = X_train if is_tree else X_train_scaled
        X_te = X_test  if is_tree else X_test_scaled

        model.fit(X_tr, y_train)
        y_pred = model.predict(X_te)

        cv_scores = cross_val_score(
            model, X_tr, y_train, cv=5,
            scoring="neg_mean_absolute_error"
        )

        r2   = metrics.r2_score(y_test, y_pred)
        mae  = metrics.mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(metrics.mean_squared_error(y_test, y_pred))
        mape = np.mean(np.abs((y_test - y_pred) / (y_test + 1))) * 100

        print(f"  R²: {r2:.4f} | MAE: £{mae:.2f} | "
              f"RMSE: £{rmse:.2f} | MAPE: {mape:.2f}%")
        print(f"  CV MAE: £{-cv_scores.mean():.2f} "
              f"(+/- £{cv_scores.std():.2f})")

        results.append({
            "MODEL":  name,
            "R2":     round(r2, 4),
            "MAE":    round(mae, 2),
            "RMSE":   round(rmse, 2),
            "MAPE":   round(mape, 2),
            "CV_MAE": round(-cv_scores.mean(), 2)
        })
        predictions[name] = y_pred

    df_results = pd.DataFrame(results).sort_values("R2", ascending=False)
    print(f"\n  Best model: {df_results.iloc[0]['MODEL']}")

    return (df_results, predictions,
            X_train_scaled, X_test_scaled,
            y_train, y_test, scaler, models,
            X_train, X_test)

# ── Feature importance ────────────────────────────────────────────

# Feature importance  ----
## Fx analyse feature importance ----
def fx_analyze_feature_importance(model, feature_names, model_name) -> pd.DataFrame | None:
    """Returns feature importance dataframe."""
    print(f"\n########### Feature Importance ({model_name}) ###########")

    if hasattr(model, "coef_"):
        importance = model.coef_
    elif hasattr(model, "feature_importances_"):
        importance = model.feature_importances_
    else:
        print("  Model doesn't support feature importance.")
        return None

    df_importance = pd.DataFrame({
        "FEATURE":    feature_names,
        "IMPORTANCE": np.abs(importance)
    }).sort_values("IMPORTANCE", ascending=False)

    print(df_importance.to_string(index=False))
    return df_importance

# ── Visualizations ────────────────────────────────────────────────

# Visualization ----

## Fx Create visualization ----
def fx_create_visualizations(y_test, predictions, feature_importance=None):
    """Creates prediction vs actual plots and residual analysis."""
    print("\n########### Creating Visualizations ###########")

    n_models = len(predictions)
    fig = plt.figure(figsize=(16, 10))

    for idx, (model_name, y_pred) in enumerate(predictions.items(), 1):
        ax = plt.subplot(3, 3, idx)
        ax.scatter(y_test, y_pred, alpha=0.5, s=20)
        ax.plot([y_test.min(), y_test.max()],
                [y_test.min(), y_test.max()],
                "r--", lw=2)
        ax.set_xlabel("Actual CLV (£)")
        ax.set_ylabel("Predicted CLV (£)")
        ax.set_title(model_name)
        ax.grid(True, alpha=0.3)

    # Residuals for best model
    best_name  = list(predictions.keys())[0]
    y_pred_best = predictions[best_name]
    residuals  = y_test - y_pred_best

    ax = plt.subplot(3, 3, n_models + 1)
    ax.scatter(y_pred_best, residuals, alpha=0.5, s=20)
    ax.axhline(y=0, color="r", linestyle="--", lw=2)
    ax.set_xlabel("Predicted CLV (£)")
    ax.set_ylabel("Residuals (£)")
    ax.set_title(f"Residuals — {best_name}")
    ax.grid(True, alpha=0.3)

    ax = plt.subplot(3, 3, n_models + 2)
    ax.hist(residuals, bins=50, edgecolor="black", alpha=0.7)
    ax.axvline(x=0, color="r", linestyle="--", lw=2)
    ax.set_xlabel("Residuals (£)")
    ax.set_ylabel("Frequency")
    ax.set_title("Residuals Distribution")
    ax.grid(True, alpha=0.3)

    if feature_importance is not None:
        ax = plt.subplot(3, 3, n_models + 3)
        top = feature_importance.head(10)
        ax.barh(range(len(top)), top["IMPORTANCE"])
        ax.set_yticks(range(len(top)))
        ax.set_yticklabels(top["FEATURE"])
        ax.set_xlabel("Importance")
        ax.set_title("Top 10 Features")
        ax.grid(True, alpha=0.3, axis="x")

    plt.tight_layout()

    # Return first axes object — compatible with fx_export_data_to_excel
    return fig.axes[0]

# ── Database tables ───────────────────────────────────────────────

# Database tables ----
## Fx Create CLTV tables ---
def fx_create_cltv_tables(df_predictions, df_results,
                           df_importance, conn):
    """Writes GOLD_DIM_CUSTOMER_CLTV and GOLD_DIM_CLTV_MODEL_RESULTS."""
    print("\n########### Writing CLTV tables ###########")

    ### GOLD_DIM_CUSTOMER_CLTV ----
    dtype_cltv = {
        "CUSTOMER_ID":   "TEXT",
        "ACTUAL_CLV":    "REAL",
        "PREDICTED_CLV": "REAL",
        "ERROR":         "REAL",
        "ERROR_PCT":     "REAL"
    }
    fx_create_table("GOLD", "DIM_CUSTOMER_CLTV", df_predictions, dtype_cltv, conn)
    print(f"  ✓ GOLD_DIM_CUSTOMER_CLTV — {len(df_predictions)} rows")

    ### GOLD_DIM_CLTV_MODEL_RESULTS ----
    dtype_results = {
        "MODEL":  "TEXT",
        "R2":     "REAL",
        "MAE":    "REAL",
        "RMSE":   "REAL",
        "MAPE":   "REAL",
        "CV_MAE": "REAL"
    }
    fx_create_table("GOLD", "DIM_CLTV_MODEL_RESULTS",
                    df_results, dtype_results, conn)
    print(f"  ✓ GOLD_DIM_CLTV_MODEL_RESULTS — {len(df_results)} rows")

# ── Main logic ────────────────────────────────────────────────────

# Main ----
## Fx run CLTV ----
def fx_run_cltv(conn):
    """Full CLTV pipeline."""
    print("\n########### CLTV Pipeline ###########")

    last_run = get_watermark("gold_cltv")

    # Incremental check
    df_check = pd.read_sql_query(
        'SELECT MAX(INVOICE_DATE) as MAX_DATE FROM "GOLD_FACT_SALES"', conn
    )
    max_gold_date = df_check["MAX_DATE"].iloc[0]

    if last_run and max_gold_date <= last_run:
        print("  GOLD_FACT_SALES unchanged since last CLTV run. Skipping.")
        return

    print(f"  New data detected (max gold date: {max_gold_date})")

    # Load data
    df_sales = fx_load_and_clean_sales(conn)
    df_rfm   = fx_load_and_clean_rfm(conn)

    # Features
    df_features, monthly_revenue, month_cols = fx_create_time_features(
        df_sales, df_rfm
    )

    # Prepare feature matrix
    X = (df_features
         .drop(columns=["CUSTOMER_ID"], errors="ignore")
         .select_dtypes(include=[np.number]))
    feature_cols = X.columns.tolist()
    print(f"  Feature matrix: {X.shape}")

    # Target
    y = fx_create_target(monthly_revenue, month_cols, train_months=12)

    # Train
    (df_results, predictions,
     X_train_scaled, X_test_scaled,
     y_train, y_test, scaler, models,
     X_train_raw, X_test_raw) = fx_train_and_evaluate_models(X, y)

    # Best model
    best_model_name = df_results.iloc[0]["MODEL"]
    best_model      = models[best_model_name]
    print(f"\n  Best model selected: {best_model_name}")

    # Feature importance
    df_importance = fx_analyze_feature_importance(
        best_model, feature_cols, best_model_name
    )

    # Visualizations
    fig_axes = fx_create_visualizations(y_test, predictions, df_importance)

    # Prediction dataframe
    df_predictions = pd.DataFrame({
        "CUSTOMER_ID":   df_features.loc[y_test.index, "CUSTOMER_ID"].values,
        "ACTUAL_CLV":    y_test.values,
        "PREDICTED_CLV": predictions[best_model_name],
        "ERROR":         y_test.values - predictions[best_model_name],
        "ERROR_PCT":     (
            (y_test.values - predictions[best_model_name])
            / (y_test.values + 1) * 100
        )
    })

    # Export to Excel
    export_dict = {
        "Customer CLTV":     df_predictions,
        "Model Results":     df_results,
        "Time Features":     df_features,
        "Visualizations":    fig_axes
    }
    if df_importance is not None:
        export_dict["Feature Importance"] = df_importance

    fx_export_data_to_excel(export_dict, "cltv_exploration", "data_exploration")

    # Write to database
    with conn:
        fx_create_cltv_tables(df_predictions, df_results, df_importance, conn)

    # Watermark
    set_watermark("gold_cltv", max_gold_date, "timestamp")
    print(f"  ✓ CLTV complete. Watermark: {max_gold_date}")


# Run ----
def run():
    print("\n########### script_cltv | Start ###########")
    try:
        conn = fx_connect_db()
        fx_run_cltv(conn)

        print("=" * 50)
        print("CLTV completed successfully.")
        print("=" * 50)

    except Exception as error:
        print(f"Error: {error}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run()

 
# # Clean ----
# ## Keep quantity > 0 only ----
# df_sales = df_sales[df_sales['QUANTITY'] > 0]

# ## Keep price > 0 only ----
# df_sales = df_sales[df_sales['PRICE'] > 0]

# ## Convert date ----
# df_sales['INVOICE_DATE'] = pd.to_datetime(df_sales['INVOICE_DATE'])

# # Create group ----
# # Define analysis date (last date in dataset + 1 day)
# snapshot_date = df_sales['INVOICE_DATE'].max() + pd.Timedelta(days=1)

# df_sales_group = df_sales.groupby('CUSTOMER_ID').agg({
#     'INVOICE_DATE': lambda date: (snapshot_date - date.max()).days,  # Days since last purchase
#     'INVOICE': lambda num: len(num), 
#     'QUANTITY': lambda quant: quant.sum(),
#     'REVENUE': lambda price: price.sum()
# })


# df_sales_group.columns = ['RECENCY', 'FREQUENCY', 'SOLD_QUANTITY', 'REVENUE']

# # Add tenure as separate feature
# df_sales_tenure = df_sales.groupby('CUSTOMER_ID')['INVOICE_DATE'].agg(TENURE=lambda x: (x.max() - x.min()).days).reset_index()

# df_sales_group_reset = df_sales_group.reset_index().merge(df_sales_tenure, on='CUSTOMER_ID')


# # Calculate CLTV using following formula:
# #  CLTV = ((Average Order Value x Purchase Frequency)/Churn Rate) x Profit margin.

# #  Customer Value = Average Order Value * Purchase Frequency
# # 1. Calculate Average Order Value

# df_sales_group['AOV']=df_sales_group['REVENUE']/df_sales_group['FREQUENCY']
# print(df_sales_group)

# # 2. Calculate Purchase Frequency
# purchase_frequency=sum(df_sales_group['FREQUENCY'])/df_sales_group.shape[0]


# # 3. Calculate Repeat Rate and Churn Rate
# repeat_rate=df_sales_group[df_sales_group['FREQUENCY'] > 1].shape[0]/df_sales_group.shape[0]

# churn_rate=1-repeat_rate 

# print(purchase_frequency,repeat_rate,churn_rate)


# # 4. Calculate Profit Margin
# # Profit margin is the commonly used profitability ratio. It represents how much percentage of total sales has earned as the gain. Let's assume our business has approx 5% profit on the total sale (REVENUE).

# # Profit Margin
# df_sales_group['PROFIT_MARGIN']=df_sales_group['REVENUE']*0.05


# # 5. Calculate Customer Lifetime Value

# # Customer Value
# df_sales_group['CLV']=(df_sales_group['AOV']*purchase_frequency)/churn_rate


# # Customer Lifetime Value
# df_sales_group['CLTV']=df_sales_group['CLV']*df_sales_group['PROFIT_MARGIN']

# print(df_sales_group)


# # Prediction Model for CLTV ----
# df_sales['YEAR_MONTH'] = df_sales['INVOICE_DATE'].apply(lambda x: x.strftime('%Y-%m'))

# print(df_sales)


# # After creating df_sales_group with RFM features
# df_sales_group_reset = df_sales_group.reset_index()


# prediction_sales = df_sales.pivot_table(
#     index=['CUSTOMER_ID'], 
#     columns=['YEAR_MONTH'],
#     values='REVENUE', 
#     aggfunc='sum',
#     fill_value=0).reset_index()


# # Merge RFM features with prediction_sales
# prediction_sales = prediction_sales.merge(
#     df_sales_group_reset[['CUSTOMER_ID', 'RECENCY', 'FREQUENCY', 'AOV', 'SOLD_QUANTITY']], 
#     on='CUSTOMER_ID', 
#     how='left'
#     )


# # Feature engineering
# X = prediction_sales.copy()
# X['AVG_MONTHLY_REVENUE'] = prediction_sales.iloc[:,1:13].mean(axis=1)
# X['TOTAL_REVENUE_6MONTH'] = prediction_sales.iloc[:,1:7].sum(axis=1)
# X['REVENUE_TREND'] = (prediction_sales.iloc[:,7:13].mean(axis=1) - 
#                       prediction_sales.iloc[:,1:7].mean(axis=1))
# # Days between first and last purchase
# X['CUSTOMER_AGE_DAYS'] = df_sales_group_reset['TENURE']
# # Purchase acceleration (are they buying more recently?)
# X['RECENT_ACTIVITY'] = prediction_sales.iloc[:, 10:13].sum(axis=1)  # Last 3 months
# X['EARLY_ACTIVITY'] = prediction_sales.iloc[:, 1:4].sum(axis=1)     # First 3 months
# # Consistency
# X['MONTHS_ACTIVE'] = (prediction_sales.iloc[:, 1:13] > 0).sum(axis=1)

# # Use BOTH time-based AND RFM features
# X = X[['AVG_MONTHLY_REVENUE', 
#        'TOTAL_REVENUE_6MONTH', 
#        'REVENUE_TREND', 
#        'RECENCY', 
#        'FREQUENCY', 
#        'AOV', 
#        'SOLD_QUANTITY',
#        'AVG_MONTHLY_REVENUE',
#        'TOTAL_REVENUE_6MONTH',
#        'CUSTOMER_AGE_DAYS',
#        'RECENT_ACTIVITY',
#        'EARLY_ACTIVITY',
#        'MONTHS_ACTIVE'
#        ]]


# # Target remains the same
# y = prediction_sales.iloc[:,13:26].sum(axis=1) 

# # Check for extreme values
# print(y.describe())
# print(f"Customers with CLV > £10,000: {(y > 10000).sum()}")
# # Cap extreme values
# y_capped = y.clip(upper=y.quantile(0.95))





# # Continue with train/test split
# X_train, X_test, y_train, y_test = train_test_split(X, y_capped, random_state=42)
# linreg = LinearRegression()
# linreg.fit(X_train, y_train)
# y_pred = linreg.predict(X_test)


# # Standard scaller ----
# scaler = StandardScaler()
# X_train_scaled = scaler.fit_transform(X_train)
# X_test_scaled = scaler.transform(X_test)

# linreg.fit(X_train_scaled, y_train)
# y_pred = linreg.predict(X_test_scaled)


# # Evaluate
# print("R-Square:", metrics.r2_score(y_test, y_pred))
# print("MAE:", metrics.mean_absolute_error(y_test, y_pred))
# print("RMSE:", np.sqrt(metrics.mean_squared_error(y_test, y_pred)))
# print(f"Average actual CLV: £{y_test.mean():.2f}")
# print(f"RMSE as % of average CLV: {(np.sqrt(metrics.mean_squared_error(y_test, y_pred))/y_test.mean())*100:.1f}%")


# # See which features matter most
# feature_importance = pd.DataFrame({
#     'feature': X.columns,
#     'coefficient': linreg.coef_
# }).sort_values('coefficient', key=abs, ascending=False)
# print("\nFeature Importance:")
# print(feature_importance)


# # Check the distribution of your target
# print(y.describe())
# print(f"\nMedian CLV: £{y.median():.2f}")
# print(f"Max CLV: £{y.max():.2f}")
# print(f"% of customers with CLV = 0: {(y == 0).sum() / len(y) * 100:.1f}%")


# # Check predictions vs actuals
# print(f"\nPredicted CLV range: £{y_pred.min():.2f} to £{y_pred.max():.2f}")
# print(f"Actual CLV range: £{y_test.min():.2f} to £{y_test.max():.2f}")


# # Visualize
# import matplotlib.pyplot as plt
# plt.scatter(y_test, y_pred, alpha=0.3)
# plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--')
# plt.xlabel('Actual CLV')
# plt.ylabel('Predicted CLV')
# plt.title('Predictions vs Actual')
# plt.show()












# R-Square: 0.4486201512385152
# MAE: 1288.8885250213239
# MSE 10976218.977535972
# RMSE: 3313.03772654885



# R-Square: 0.449 (44.9%)

# Your model explains ~45% of the variance in future CLV
# The remaining 55% is due to factors not captured by these 3 features
# This is reasonable for customer behavior prediction (typically 0.3-0.7 is good)

# Coefficients:
# Intercept: -253.80
# Coefficients: [5.43, 1.26, 10.44]
# ```

# **Interpretation:**
# ```
# Future CLV = -253.80 + (5.43 × avg_monthly_revenue) + 
#              (1.26 × total_revenue_6m) + (10.44 × revenue_trend)

# Revenue trend (10.44) has the strongest impact - customers with increasing spending are most valuable
# Average monthly revenue (5.43) is also important
# Total 6-month revenue (1.26) has smaller direct effect (likely correlated with average)

# Error Metrics:

# MAE: £1,289 - On average, predictions are off by ~£1,289
# RMSE: £3,313 - Larger errors are penalized more; some predictions are quite far off


# Is This Good?
# It depends on your business context:

# If average CLV is £10,000: These errors are acceptable (10-30% error rate)
# If average CLV is £3,000: The model has high error relative to target values

# Check this:

# print(f"Average actual CLV: £{y_test.mean():.2f}")
# print(f"RMSE as % of average CLV: {(3313/y_test.mean())*100:.1f}%")

# Ways to Improve:

# Add more features:

# Recency (days since last purchase)
# Frequency (number of transactions)
# Product categories purchased
# Customer tenure


# Try other models:

# Random Forest
# XGBoost
# May capture non-linear patterns better


# Feature engineering:

# Seasonality indicators
# Purchase acceleration/deceleration
# Customer segmentation



























# # Let's sum all the months sales.

# prediction_sales['CLV']=prediction_sales.iloc[:,2:].sum(axis=1)
# print(prediction_sales)
# print(prediction_sales.columns)

# # Selecting Feature ----
# X= prediction_sales.drop(['CUSTOMER_ID', 'CLV'], axis = 1)
# y=prediction_sales[['CLV']]


# X_train, X_test, y_train, y_test = train_test_split(X, y,random_state=0)


# # Model Development ----
# # import model


# # instantiate
# linreg = LinearRegression()

# # fit the model to the training data (learn the coefficients)
# linreg.fit(X_train, y_train)

# # make predictions on the testing set
# y_pred = linreg.predict(X_test)

# # print the intercept and coefficients
# print(linreg.intercept_)
# print(linreg.coef_)

# # Check predictions ----


# # compute the R Square for model
# print("R-Square:",metrics.r2_score(y_test, y_pred))

# # Model Evaluation ----
# # calculate MAE using scikit-learn
# print("MAE:",metrics.mean_absolute_error(y_test,y_pred))

# #calculate mean squared error
# print("MSE",metrics.mean_squared_error(y_test, y_pred))
# # compute the RMSE of our predictions
# print("RMSE:",np.sqrt(metrics.mean_squared_error(y_test, y_pred)))

# # R-Square (R² = 1.0)

# # Meaning: The model explains 100% of the variance in your target variable
# # Range: 0 to 1 (higher is better)
# # Your score: Perfect fit - the model predictions match actual values almost exactly

# # MAE (Mean Absolute Error = 1.35e-11)

# # Meaning: On average, predictions are off by 0.0000000000135 units
# # Interpretation: Essentially zero error - predictions are nearly identical to actual values

# # MSE (Mean Squared Error = 3.12e-21)

# # Meaning: Average of squared errors (penalizes larger errors more heavily)
# # Your score: Virtually zero

# # RMSE (Root Mean Squared Error = 5.59e-11)

# # Meaning: Square root of MSE, in the same units as your target variable
# # Interpretation: Average prediction error is 0.0000000000559 units

# # 1. Verify train/test split
# print(f"Train size: {len(X_train)}, Test size: {len(X_test)}")

# # 2. Check for target leakage in features
# print(X_train.columns)  # Does any feature essentially reveal the target?

# # 3. Check correlation with target
# print(X_train.corrwith(y_train).abs().sort_values(ascending=False))
"""
=============================================================
...
=============================================================
Script purpose:

Process:
    01. Connect to the database located ../data/database (it should be named DATAWAREHOUSE_ONLINE_RETAIL_II)
    02. 
    End of process



Recency_Score, Frequency_Score, Monetary_Score
I rate the recency, frequency, and monetary scores between 5 and 1. In this section, data with a score of 5 will be the data with the best recency score for us, data with a score of 1 will have the worst score.

Recency_Score: You know, high recency values are bad for us. Because high recency values represent the day the customer stays away from the company. Customers with low recency values are customers who do not stay away from the company.

Frequency_Score: High-frequency values are good for us. The Frequency value is that customer use how many use our company more.

Monetary_Score: High-monetary values are good for us. The Monetary value is the customer how much pays the company.

Une nouvelle méthode de segmentation RFM est utilisée : 
•	Chaque score (R, F, M) est noté de 1 à 5. 
•	Score minimal 3 (1+1+1).
•	Score maximal 15 (5+5+5).
•	Les valeurs des bornes ne sont pas arbitraires, ni fixes. 
Les bornes varient selon les données fournies. Chaque borne représente un quintile de chaque critère (de 0 à 20%, de 20 à 40%, de 40 à 60%, de 60 à 80% de 80 à 100%).
Les avantages : 
•	Les bornes étant variables en fonction des données clients, elles se mettent à jour en permanence.
•	Si, pour une raison X ou Y, nous devions nous intéresser à l’un des scores, il sera plus simple de faire pondération. Par exemple, si nous souhaitons mettre en exergue la fidélité et donc la fréquence d’achat, le score Fréquence sera multiplié par 1,2 et les autres scores par 0,9.
•	Les clients peuvent être segmentés en groupe de quintile.



List of functions used: 
    - fx_connect_db : connect to the database, imported from connection_to_database.py
    - 

Potential improvements: 
    - Not determined yet

WARNING:



📅 TIME-BASED METRICS
RECENCY
What: Days since the customer's last purchase
Calculation: (snapshot_date - last_purchase_date).days
Example: Last purchase was 15 days ago → RECENCY = 15
Interpretation:

Low recency (e.g., 5 days) = Recent buyer, highly engaged
High recency (e.g., 365 days) = Inactive, at risk of churning



TENURE
What: Total days the customer has been active
Calculation: (last_purchase_date - first_purchase_date).days
Example: First purchase Jan 1, last purchase Dec 31 → TENURE = 364 days
Interpretation:
High tenure = Long-term, loyal customer
Low tenure = New or short-term customer



DATE_FIRST_PURCHASE
What: The exact date of the customer's first order
Use: Identify customer acquisition date, cohort analysis

DATE_LAST_PURCHASE
What: The exact date of the customer's most recent order
Use: Track engagement recency, identify at-risk customers


🛒 TRANSACTION METRICS
FREQUENCY
What: Total number of unique orders/invoices
Calculation: count(distinct invoice_id)
Example: 8 separate orders → FREQUENCY = 8
Interpretation:

High frequency = Repeat buyer, loyal
Low frequency (1-2) = One-time or occasional buyer



SOLD_QUANTITY
What: Total number of items purchased across all orders
Calculation: sum(quantity)
Example: Bought 3 items in order 1, 5 in order 2 → SOLD_QUANTITY = 8

AVG_BASKET_SIZE
What: Average number of items per order
Calculation: sum(quantity) / count(orders)
Example: 20 items across 4 orders → AVG_BASKET_SIZE = 5
Interpretation:

High = Bulk buyers, larger orders
Low = Small, frequent purchases




💰 REVENUE METRICS
TOTAL_REVENUE
What: Total amount spent by the customer (lifetime value)
Calculation: sum(order_value)
Example: Spent $500 across all orders
Interpretation: Most important metric for identifying high-value customers

AVG_ORDER_VALUE (AOV)
What: Average spending per order
Calculation: total_revenue / frequency
Example: $1,000 revenue over 5 orders → AOV = $200
Interpretation:

High AOV = Premium buyers, larger transactions
Low AOV = Budget buyers, smaller purchases



MAX_ORDER_VALUE
What: The largest single order amount
Use: Identify spending capacity, spot one-time big purchases

MIN_ORDER_VALUE
What: The smallest single order amount
Use: Understand minimum engagement threshold


📊 DERIVED RATE METRICS
PURCHASE_FREQUENCY_RATE

What: How often the customer buys relative to their tenure
Calculation: frequency / (tenure + 1)
Example: 10 orders over 100 days → Rate = 0.10 (10% purchase rate per day)
Interpretation:

High rate = Consistently active buyer
Low rate = Sporadic buyer despite long tenure



REVENUE_PER_DAY
What: Average daily revenue contribution
Calculation: total_revenue / (tenure + 1)
Example: $365 spent over 365 days → $1 per day
Use: Compare customer value across different tenures


🎯 RFM SCORING METRICS
RECENCY_SCORE
What: Quartile/quintile ranking of recency (lower days = higher score)
Scale: Typically 1-5 (5 = most recent)
Example: Recency of 5 days might score 5, recency of 200 days might score 1

FREQUENCY_SCORE
What: Quartile/quintile ranking of purchase frequency
Scale: Typically 1-5 (5 = highest frequency)
Example: 20 orders might score 5, 1 order might score 1

MONETARY_SCORE
What: Quartile/quintile ranking of total revenue
Scale: Typically 1-5 (5 = highest spender)
Example: $10,000 spent might score 5, $50 spent might score 1

RFM_SCORE
What: Combined segmentation identifier
Format: Usually concatenated string like "555", "111", "345"
Example:

"555" = Champions (recent, frequent, high-spending)
"511" = Recent but low-value
"155" = High-value but inactive


Use: Segment customers into actionable groups (Champions, Loyal, At-Risk, etc.)


💡 Key Relationships:

RECENCY + FREQUENCY = Engagement pattern
FREQUENCY + MONETARY = Customer value tier
TENURE + FREQUENCY = Purchase rate consistency
RFM_SCORE = Overall customer segmentation for targeted marketing

"""

# 1. Import librairies ----
import pandas as pd
import datetime as dt
from datetime import datetime, timezone

from src.utils.connecting_to_database import fx_connect_db
from src.utils.create_table import fx_create_table
from src.utils.export_data_to_xlsx import fx_export_data_to_excel
from src.utils.watermark import get_watermark, set_watermark


# ── Data preparation ──────────────────────────────────────────────

# 2. Data preparation ----
## Fx Prepare sales ----
def fx_prepare_sales(conn) -> pd.DataFrame:
    """Loads GOLD_FACT_SALES and filters for valid sales only."""
    print("\n########### Load GOLD_FACT_SALES ###########")
    df = pd.read_sql_query('SELECT * FROM "GOLD_FACT_SALES"', conn)

    df["INVOICE_DATE"] = pd.to_datetime(df["INVOICE_DATE"])

    # Keep valid sales only
    df = df[df["QUANTITY"] > 0]
    df = df[df["PRICE"] > 0]
    df = df[df["CUSTOMER_ID"] != "UNKNOWN"]

    print(f"  {len(df)} valid sales rows loaded")
    print(f"  {df['CUSTOMER_ID'].nunique()} unique customers")
    return df


# ── RFM aggregation ───────────────────────────────────────────────

# 3. RFM aggregation ----
## Fx build RFM ----
def fx_build_rfm(df_sales: pd.DataFrame) -> pd.DataFrame:
    """Aggregates sales data into RFM metrics per customer."""
    print("\n########### Build RFM metrics ###########")

    snapshot_date = df_sales["INVOICE_DATE"].max() + pd.Timedelta(days=1)
    print(f"  Snapshot date: {snapshot_date.date()}")

    df_rfm = df_sales.groupby("CUSTOMER_ID").agg(
        RECENCY=("INVOICE_DATE", lambda x: (snapshot_date - x.max()).days),
        TENURE=("INVOICE_DATE", lambda x: (x.max() - x.min()).days),
        DATE_FIRST_PURCHASE=("INVOICE_DATE", "min"),
        DATE_LAST_PURCHASE=("INVOICE_DATE", "max"),
        FREQUENCY=("INVOICE", "nunique"),
        SOLD_QUANTITY=("QUANTITY", "sum"),
        AVG_BASKET_SIZE=("QUANTITY", "mean"),
        TOTAL_REVENUE=("REVENUE", "sum"),
        AVG_ORDER_VALUE=("REVENUE", "mean"),
        MAX_ORDER_VALUE=("REVENUE", "max"),
        MIN_ORDER_VALUE=("REVENUE", "min")
    ).reset_index()

    # Complementary metrics
    df_rfm["PURCHASE_FREQUENCY_RATE"] = (
        df_rfm["FREQUENCY"] / (df_rfm["TENURE"] + 1)
    )
    df_rfm["REVENUE_PER_DAY"] = (
        df_rfm["TOTAL_REVENUE"] / (df_rfm["TENURE"] + 1)
    )
    df_rfm["IS_REPEAT_CUSTOMER"] = (df_rfm["FREQUENCY"] > 1).astype(int)
    df_rfm["AVG_DAYS_BETWEEN_PURCHASES"] = (
        df_rfm["TENURE"] / df_rfm["FREQUENCY"]
    )
    df_rfm["CHURN_RISK_SCORE"] = (
        df_rfm["RECENCY"] / (df_rfm["AVG_DAYS_BETWEEN_PURCHASES"] + 1)
    )
    df_rfm["IS_ACTIVE"] = (df_rfm["RECENCY"] <= 90).astype(int)

    print(f"  Repeat customer rate: {df_rfm['IS_REPEAT_CUSTOMER'].mean():.2%}")
    print(f"  Active customer rate: {df_rfm['IS_ACTIVE'].mean():.2%}")
    return df_rfm


# ── RFM scoring ───────────────────────────────────────────────────

# RFM scoring 
## Fx score RFM 
def fx_score_rfm(df_rfm: pd.DataFrame) -> pd.DataFrame:
    """Adds R, F, M scores (1-5) and combined RFM_SCORE."""
    print("\n########### RFM Scoring ###########")

    ### Recency: lower is better → reverse ranking ----
    df_rfm["RECENCY_SCORE"] = 6 - (
        pd.qcut(
            df_rfm["RECENCY"].rank(method="first"),
            q=5, labels=False, duplicates="drop"
        ) + 1
    )

    ### Frequency: higher is better ----
    df_rfm["FREQUENCY_SCORE"] = (
        pd.qcut(
            df_rfm["FREQUENCY"].rank(method="first"),
            q=5, labels=False, duplicates="drop"
        ) + 1
    )

    ### Monetary: higher is better ----
    df_rfm["MONETARY_SCORE"] = (
        pd.qcut(
            df_rfm["TOTAL_REVENUE"].rank(method="first"),
            q=5, labels=False, duplicates="drop"
        ) + 1
    )

    ### Combined score as concatenated string ----
    df_rfm["RFM_SCORE"] = (
        df_rfm[["RECENCY_SCORE", "FREQUENCY_SCORE", "MONETARY_SCORE"]]
        .astype(str)
        .agg("".join, axis=1)
    )

    return df_rfm


# ── Score summaries ───────────────────────────────────────────────

# 4. Score summaries -----
## Fx build score summaries ----
def fx_build_score_summaries(df_rfm: pd.DataFrame) -> tuple:
    """Builds recency, frequency and monetary summary dataframes."""
    print("\n########### Build score summaries ###########")

    summary_recency = (
        df_rfm.groupby("RECENCY_SCORE")
        .agg(MIN_RECENCY=("RECENCY", "min"),
             MAX_RECENCY=("RECENCY", "max"),
             COUNT=("RECENCY", "count"))
        .reset_index()
        .sort_values("RECENCY_SCORE")
    )

    summary_frequency = (
        df_rfm.groupby("FREQUENCY_SCORE")
        .agg(MIN_FREQUENCY=("FREQUENCY", "min"),
             MAX_FREQUENCY=("FREQUENCY", "max"),
             COUNT=("FREQUENCY", "count"))
        .reset_index()
        .sort_values("FREQUENCY_SCORE")
    )

    summary_monetary = (
        df_rfm.groupby("MONETARY_SCORE")
        .agg(MIN_MONETARY=("TOTAL_REVENUE", "min"),
             MAX_MONETARY=("TOTAL_REVENUE", "max"),
             COUNT=("TOTAL_REVENUE", "count"))
        .reset_index()
        .sort_values("MONETARY_SCORE")
    )

    print(f"\nRecency summary:\n{summary_recency}")
    print(f"\nFrequency summary:\n{summary_frequency}")
    print(f"\nMonetary summary:\n{summary_monetary}")

    return summary_recency, summary_frequency, summary_monetary


# ── Main logic ────────────────────────────────────────────────────

# 5. Main logic ----
## Fx load gold RFM scoring ----
def fx_load_gold_rfm_scoring(conn):
    print("\n########### Gold RFM Scoring ###########")

    last_run = get_watermark("gold_rfm_scoring")

    ### Incremental check — skip if GOLD_FACT_SALES hasn't changed ----
    df_check = pd.read_sql_query(
        'SELECT MAX(INVOICE_DATE) as MAX_DATE FROM "GOLD_FACT_SALES"', conn
    )
    max_gold_date = df_check["MAX_DATE"].iloc[0]

    if last_run and max_gold_date <= last_run:
        print("  GOLD_FACT_SALES unchanged since last RFM run. Skipping.")
        return

    print(f"  New data detected (max gold date: {max_gold_date})")

    ### Pipeline ----
    df_sales = fx_prepare_sales(conn)
    df_rfm   = fx_build_rfm(df_sales)
    df_rfm   = fx_score_rfm(df_rfm)

    summary_recency, summary_frequency, summary_monetary = (
        fx_build_score_summaries(df_rfm)
    )

    ### Export to Excel ----
    fx_export_data_to_excel(
        {
            "Customer RFM score": df_rfm,
            "Summary recency":    summary_recency,
            "Summary frequency":  summary_frequency,
            "Summary monetary":   summary_monetary
        },
        "gold_customer_rfm",
        "data_exploration"
    )

    ### Save to database ----
    dtype_mapping = {
        "CUSTOMER_ID":               "TEXT",
        "RECENCY":                   "TEXT",
        "TENURE":                    "TEXT",
        "DATE_FIRST_PURCHASE":       "TEXT",
        "DATE_LAST_PURCHASE":        "TEXT",
        "FREQUENCY":                 "INTEGER",
        "SOLD_QUANTITY":             "REAL",
        "AVG_BASKET_SIZE":           "REAL",
        "TOTAL_REVENUE":             "REAL",
        "AVG_ORDER_VALUE":           "REAL",
        "MAX_ORDER_VALUE":           "REAL",
        "MIN_ORDER_VALUE":           "REAL",
        "PURCHASE_FREQUENCY_RATE":   "REAL",
        "REVENUE_PER_DAY":           "REAL",
        "IS_REPEAT_CUSTOMER":        "INTEGER",
        "AVG_DAYS_BETWEEN_PURCHASES":"REAL",
        "CHURN_RISK_SCORE":          "REAL",
        "IS_ACTIVE":                 "INTEGER",
        "RECENCY_SCORE":             "INTEGER",
        "FREQUENCY_SCORE":           "INTEGER",
        "MONETARY_SCORE":            "INTEGER",
        "RFM_SCORE":                 "TEXT"
    }

    fx_create_table("GOLD", "DIM_CUSTOMER_RFM", df_rfm, dtype_mapping, conn)

    set_watermark("gold_rfm_scoring", max_gold_date, "timestamp")
    print(f"  ✓ GOLD_DIM_CUSTOMER_RFM — {len(df_rfm)} customers. "
          f"Watermark: {max_gold_date}")


## Run ----
def run():
    print("\n########### script_rfm_scoring | Start ###########")
    try:
        conn = fx_connect_db()
        with conn:
            fx_load_gold_rfm_scoring(conn)

        print("=" * 50)
        print("RFM scoring completed successfully.")
        print("=" * 50)

    except Exception as error:
        print(f"Error: {error}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run()
import sys
sys.path.insert(0, "/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# ── Imports ───────────────────────────────────────────────────────

from src.utils.watermark import create_watermark_table

from src.ingestion.data_xlsx_to_csv import run as run_xlsx_to_csv
from src.ingestion.creating_database import run as run_create_database

from src.bronze.script_layer_bronze  import run as run_bronze

from src.silver.script_layer_silver          import run as run_silver
from src.silver.silver_country_mapping       import run as run_country_mapping
from src.silver.silver_exchange_rate_historic import run as run_exchange_rate
from src.silver.silver_product_mapping       import run as run_product_mapping

from src.gold.script_layer_gold   import run as run_gold
from src.gold.script_rfm_scoring  import run as run_rfm
from src.gold.script_cltv         import run as run_cltv

# ── Default arguments ─────────────────────────────────────────────

default_args = {
    "owner":            "airflow",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False
}

# ── DAG definition ────────────────────────────────────────────────

with DAG(
    dag_id="online_retail_etl",
    description="Incremental ETL pipeline: Bronze → Silver → Gold",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 6 * * *",  # Every day at 6:00 AM
    catchup=False,
    tags=["etl", "retail", "incremental"]
) as dag:

    # ── Init ──────────────────────────────────────────────────────

    task_init_watermarks = PythonOperator(
        task_id="init_watermarks",
        python_callable=create_watermark_table
    )

    # ── Ingestion ─────────────────────────────────────────────────

    task_xlsx_to_csv = PythonOperator(
        task_id="xlsx_to_csv",
        python_callable=run_xlsx_to_csv
    )

    task_create_database = PythonOperator(
        task_id="create_database",
        python_callable=run_create_database
    )

    # ── Bronze ────────────────────────────────────────────────────

    task_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=run_bronze
    )

    # ── Silver ────────────────────────────────────────────────────

    task_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=run_silver
    )

    task_country_mapping = PythonOperator(
        task_id="silver_country_mapping",
        python_callable=run_country_mapping
    )

    task_exchange_rate = PythonOperator(
        task_id="silver_exchange_rate",
        python_callable=run_exchange_rate
    )

    task_product_mapping = PythonOperator(
        task_id="silver_product_mapping",
        python_callable=run_product_mapping
    )

    # ── Gold ──────────────────────────────────────────────────────

    task_gold = PythonOperator(
        task_id="build_gold",
        python_callable=run_gold
    )

    task_rfm = PythonOperator(
        task_id="rfm_scoring",
        python_callable=run_rfm
    )

    task_cltv = PythonOperator(
        task_id="cltv",
        python_callable=run_cltv
    )

    # ── Dependencies ──────────────────────────────────────────────
    #
    # init_watermarks
    #       ↓
    # xlsx_to_csv
    #       ↓
    # create_database
    #       ↓
    # load_bronze
    #       ↓
    # transform_silver
    #       ↓
    # country_mapping → exchange_rate → product_mapping
    #                                       ↓
    #                                   build_gold
    #                                       ↓
    #                                  rfm_scoring → cltv

    task_init_watermarks >> task_xlsx_to_csv
    task_xlsx_to_csv     >> task_create_database
    task_create_database >> task_bronze
    task_bronze          >> task_silver
    task_silver          >> task_country_mapping
    task_country_mapping >> task_exchange_rate
    task_exchange_rate   >> task_product_mapping
    task_product_mapping >> task_gold
    task_gold            >> task_rfm
    task_rfm             >> task_cltv
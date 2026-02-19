import sqlite3
import os

DB_PATH = os.environ.get("DB_PATH", "/opt/airflow/data/database/DATAWAREHOUSE_ONLINE_RETAIL_II.db")

def get_connection() -> sqlite3.Connection:
    """Returns a SQLite connection with row factory enabled (rows behave like dicts)."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn
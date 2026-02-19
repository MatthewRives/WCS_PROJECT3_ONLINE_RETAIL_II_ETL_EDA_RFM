import sqlite3
import os
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", "data/database/warehouse.db")

def get_connection():
    return sqlite3.connect(DB_PATH)

def create_watermark_table():
    """Run once at pipeline startup to ensure the table exists."""
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS _watermarks (
                table_name      TEXT PRIMARY KEY,
                last_value      TEXT,       -- timestamp OR id, stored as text
                watermark_type  TEXT,       -- 'timestamp' or 'id'
                updated_at      TEXT
            )
        """)
        conn.commit()

def get_watermark(table_name: str) -> str | None:
    """Returns the last processed value for a given table, or None if first run."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT last_value FROM _watermarks WHERE table_name = ?",
            (table_name,)
        ).fetchone()
    return row[0] if row else None

def set_watermark(table_name: str, value: str, watermark_type: str = "timestamp"):
    """Updates (or inserts) the watermark for a given table."""
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO _watermarks (table_name, last_value, watermark_type, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(table_name) DO UPDATE SET
                last_value = excluded.last_value,
                watermark_type = excluded.watermark_type,
                updated_at = excluded.updated_at
        """, (table_name, str(value), watermark_type, datetime.utcnow().isoformat()))
        conn.commit()
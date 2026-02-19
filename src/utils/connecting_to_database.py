"""
=============================================================
Function: Connecting to database
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

# 1. Import librairies ----
import sqlite3
from src.utils.db import DB_PATH

def fx_connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    print("=" * 50)
    print(f"Database connected at: {DB_PATH}")
    print("=" * 50)
    return conn
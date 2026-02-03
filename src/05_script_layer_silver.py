"""
=============================================================
Create Silver layer and insert transformed data from bronze layer
=============================================================
Script purpose:
    This script creates new tables for the silver layer (medaillon data model), in the database, and insert transformed data from the bronze layer in these tables.

Process:
    01. Connect to the database located ../datasets/database (it should be named DATAWAREHOUSE_ONLINE_RETAIL_II)
    02. 
    End of process

List of functions used: 
    - fx_connect_db : connect to the database, imported from connection_to_database.py
    - 

Potential improvements: 
    - Not determined yet
        
WARNING:

"""


import os
import sqlite3
import pandas as pd
from connecting_to_database import fx_connect_db
import re
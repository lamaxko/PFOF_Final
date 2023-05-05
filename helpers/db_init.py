import sqlite3
import time
from datetime import datetime, timedelta

def con_setup():
        con = sqlite3.connect('PFOF.db')
        cur = con.cursor()
        return con, cur

def db_setup_post(exchanges):
    con, cur = con_setup()
    
    # create a table for each exchange to store post traded data
    for exchange in exchanges: 
        cur.execute(f'''CREATE TABLE IF NOT EXISTS {exchange}_post
                (row_number INTEGER PRIMARY KEY AUTOINCREMENT,
                id text, 
                timestamp text, 
                ticker text, 
                price real, 
                volume real,
                currency text,
                bid_price real,
                ask_price real,
                price_xetra real,
                price_average real,
                trans_type text,
                matched INTEGER)''')

    con.commit()
    return con  


def db_setup_pre(exchanges):
    con, cur = con_setup()
    
    # create a table for each exchange to store post traded data
    for exchange in exchanges: 
        cur.execute(f'''CREATE TABLE IF NOT EXISTS {exchange}_pre
                (row_number INTEGER PRIMARY KEY AUTOINCREMENT, 
                timestamp text, 
                ticker text,  
                bid_price real,
                ask_price real,
                bid_volume real,
                ask_volume real)''')

    con.commit()
    return con  
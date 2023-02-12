from dotenv import load_dotenv
import os

import psycopg2
import psycopg2.extras
from psycopg2.errors import SerializationFailure

load_dotenv()

conn = psycopg2.connect(os.getenv('COCKROACH_DB_CONNECTION_STRING'))
cur = conn.cursor()


def init_inventory_table():
    
    cur.execute('''DROP TABLE IF EXISTS inventory''')
    cur.execute(" CREATE TABLE inventory (id SERIAL PRIMARY KEY, title STRING, quantity INT, unit_price DECIMAL, size INT);")
    conn.commit()

def init_incomplete_orders_table():
    cur.execute('''DROP TABLE IF EXISTS incomplete_orders''') 
    cur.execute('''
        CREATE TABLE IF NOT EXISTS incomplete_orders (
            id SERIAL PRIMARY KEY,
            order_id STRING,
            title STRING,
            sku STRING,
            ordered INT,
            shipped INT,
            unit_price DECIMAL,
            total_price DECIMAL,
            size INT
        );
    ''')
    conn.commit()


def init_completed_orders_table():
    cur.execute('''DROP TABLE IF EXISTS complete_orders''') 
    cur.execute('''
        CREATE TABLE IF NOT EXISTS complete_orders (
            order_id STRING PRIMARY KEY
        );
    ''')
    conn.commit()


init_inventory_table()
init_incomplete_orders_table()
init_completed_orders_table()
conn.close()

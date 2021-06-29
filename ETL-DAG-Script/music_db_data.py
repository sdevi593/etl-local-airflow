# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy.engine.url import URL
import sqlite3
import mysql.connector

db =(r'C:\Users\62895\Documents\Dataset\Dataset\chinook.db')
db_sqlite =(r'C:\Users\62895\Documents\Dataset\Dataset\database.sqlite')
    
def main():
    db_music_tables = extract_transform_data()
    load_data(db_music_tables, db)

def extract_transform_data():
    #extract chinook.db data
    db_conn = sqlite3.connect(db)

    ##importing tables
    db_tables = pd.read_sql("""SELECT *
                            FROM sqlite_master
                            WHERE type='table'; """, db_conn)

    #extract database.sqlite data

    db_sqlite_conn = sqlite3.connect(db_sqlite)
    ##importing tables
    db_sqlite_tables = pd.read_sql("""SELECT *
                            FROM sqlite_master
                            WHERE type='table'; """, db_sqlite_conn)

    #combine chinook and sqlite data
    db_music_tables = pd.concat([db_tables, db_sqlite_tables])

def load_data(db_music_tables, db):
    #define the file name/URL
    file_db = URL(drivername = 'mysql', host='localhost',
                  database='data_warehouse',
                  query={ 'read_default_file' : '/path/to/.my.cnf'}
                  )
    #connect to database
    engine = sqlalchemy.create_engine(name_or_url=file_db, pool_pre_ping=True)

    file_db_conn = sqlite3.connect(db)
    file_db_cursor = file_db_conn.cursor()

    #export data to sql
    db_music_tables.to_sql(db, engine, index=False, if_exists='replace')
    engine.execute("SELECT * FROM db").fetchall()
    
    #close database
    file_db_conn.close()

if __name__ =='__main__':
   main()

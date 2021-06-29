
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy.engine.url import URL
import sqlite3
import mysql.connector

PATH = r'C:\Users\62895\Documents\Dataset\Dataset\disaster_data.csv'
db = 'disaster_data'

def disaster_data_etl():
    df = extract_transform_data()
    load_data(df, db)

def extract_transform_data():
    #extract Disaster Data.csv
    df = pd.read_csv(PATH, header=0)
    
    #change nan data to none, encode some unicode data
    df = df.astype(object).replace(np.nan, 'None')
    for i in df.columns:
        df[i] =  df.text.str.replace('[^\x00-\x7F]','')
    return df

def load_data(df, db):
    #define the file name/URL
    file_db = URL(drivername = 'mysql', host='localhost',
                  database='data_warehouse',
                  query={ 'read_default_file' : '/path/to/.my.cnf'}
                  )
    #connect to database
    engine = sqlalchemy.create_engine(name_or_url=file_db, pool_pre_ping=True)
    file_db_conn = sqlite3.connect(db)
    file_db_cursor = file_db_conn.cursor()
    
    #export df to sql table
    df.to_sql(db, engine, index=False, if_exists='replace')
    engine.execute("SELECT * FROM disaster_data").fetchall()
    
    #close database
    file_db_conn.close()

if __name__ =='__main__':
    disaster_data_etl()
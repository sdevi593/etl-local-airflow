import pandas as pd
import datetime
import json
import sqlalchemy
from sqlalchemy.engine.url import URL
import sqlite3
import mysql.connector

PATH = r'C:\Users\62895\Documents\Dataset\Dataset\tweet_data.json'
db = 'tweet_data'


def tweet_data_etl():
    df = extract_transform_data()
    load_data(df, db)
    
    

def extract_transform_data():
    #extract tweet_data.json
    df = pd.read_json(PATH, lines=True)
    
    #select some relevant table only, shange date format, decode unicode data
    df = df[['id', 'source', 'text', 'created_at']]
    df['created_at'] = pd.to_datetime(df.created_at).dt.strftime('%m/%d/%Y')
    for i in df.columns:
        df[i] =  df.text.str.replace('[^\x00-\x7F]','')
    return df

def load_data(df,db):
    #define the URL
    file_db = URL(drivername = 'mysql', host='localhost',
              database='data_warehouse',
              query={ 'read_default_file' : '/path/to/.my.cnf'}
              )
    #connect to database
    engine = sqlalchemy.create_engine(name_or_url=file_db, pool_pre_ping=True)
    file_db_conn = sqlite3.connect(db)
    file_db_cursor = file_db_conn.cursor()
    
    #export dataframe to sql table
    df.to_sql(db, engine, index=False, if_exists='replace')
    engine.execute("SELECT * FROM tweet_data").fetchall()
    
    #close database
    file_db_conn.close()
if __name__ =='__main__':
    tweet_data_etl()
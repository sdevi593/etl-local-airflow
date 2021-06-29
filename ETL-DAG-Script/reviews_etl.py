# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy.engine.url import URL
import sqlite3
import mysql.connector
import datetime


PATH1 = r'C:\Users\62895\Documents\Dataset\Dataset\reviews_q1.csv'
PATH2 = r'C:\Users\62895\Documents\Dataset\Dataset\reviews_q2.csv'
PATH3 = r'C:\Users\62895\Documents\Dataset\Dataset\reviews_q3.csv'
PATH4 = r'C:\Users\62895\Documents\Dataset\Dataset\reviews_q4.csv'
db ='reviews'


def reviews_etl():
    df = extract_transform_data()
    load_data(df, db)

def extract_transform_data():
    #extract  and transform reviews data
    df1 = pd.read_csv(PATH1, header=0)
    df2 = pd.read_csv(PATH2, header=0)
    df3 = pd.read_csv(PATH3, header=0)
    df4 = pd.read_csv(PATH4, header=0)
    df = pd.concat([df1, df2, df3, df4])
    df = df.astype(object).replace(np.nan, 'None')
    #df = df.drop(['listing_id'], 1)
    #df['date'] = pd.to_datetime(df.date).dt.strftime('%m/%d/%Y')
    for i in df:
        df[i]=  df[i].astype(str).str.replace('[^\x00-\x7F]','')
    print(df.columns)
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

    #export dataframe to sql table
    df.to_sql(db, engine, index=False, if_exists='replace')
    engine.execute("SELECT * FROM reviews").fetchall()

    #close connection
    file_db_conn.close()


if __name__ =='__main__':
   reviews_etl()
# -*- coding: utf-8 -*-



import pandas as pd
import sqlalchemy
from sqlalchemy.engine.url import URL
import sqlite3
import mysql.connector

PATH = r'C:\Users\62895\Documents\Dataset\Dataset\file_1000.xls'
db = 'file_new_1000'

def file_1000_etl():
    df = extract_transform_data()
    load_data(df, db)

def extract_transform_data():
    #extract file_1000.xls
    df = pd.read_excel(PATH, header=0)

    #transform data : merge, drop, rename, change, reorder columns
    df['Full_Name'] = df['First Name'] + ' ' + df['Last Name']
    df = df.drop(['First Name', 'Last Name', 'First Name.1'], axis=1)
    df = df.rename(columns={'Unnamed: 0' : 'Number'})
    df['Gender'] = df['Gender'].map({'Female' : 'F', 'Male' : 'M'})
    df = df[['Number', 'Full_Name', 'Gender', 'Country', 'Age', 'Id', 'Date' ]]
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
    engine.execute("SELECT * FROM file_new_1000").fetchall()

    #close database
    file_db_conn.close()

if __name__ =='__main__':
    file_1000_etl()
'''
    DAG_ETL Programme
'''



from airflow.models import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from file_1000_etl import file_1000_etl 
from Disaster_data_etl import disaster_data_etl
from tweet_data_etl import tweet_data_etl
from reviews_etl import reviews_etl
from music_db_data import main

def main():
    #define default arguments
    default_args = {
        'owner': 'devis',
        'email' : 'devis@gmail.com',
        'email_on_failure': True,
        'start_date' : days_ago(1),
        'retries' : 2,
        'retry_delay' : timedelta(minutes=1),
    }
    #creating DAG   
    dag = DAG (
        'data_warehouse_dag',
        default_args=default_args,
        description='my first DAG with ETL process!',
        schedule_interval=timedelta(days=1),
    )
    #call the etl function of each file
    run_etl1 = PythonOperator(
        task_id='file_1000_etl',
        python_callable=file_1000_etl,
        dag=dag,
    )
    
    run_etl2 = PythonOperator(
        task_id='Disaster_data_etl',
        python_callable=disaster_data_etl,
        dag=dag,
    )
    

    run_etl3 = PythonOperator(
        task_id='tweet_data_etl',
        python_callable=tweet_data_etl,
        dag=dag,
    )
    
    run_etl4 = PythonOperator(
        task_id='reviews_data_etl',
        python_callable=reviews_etl,
        dag=dag,
    )

    run_etl5 = PythonOperator(
        task_id='music_db_data',
        python_callable=main,
        dag=dag,
    )
    

run_etl1 >> [run_etl2, run_etl3, run_etl4, run_etl5]

if __name__ =='__main__':
    main()
import psycopg2
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

pg_creds = {
    "host": '192.168.0.122'
    , "port": 5432
    , "database": "dshop"
    , "user": "pguser"
    , "password": "secret"
}


def read_pg():
    tables_to_load = ['aisles', 'clients', 'departments', 'orders', 'products']
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        for table in tables_to_load:
            with open(os.path.join('/home/user/data', f'{table}.csv'), 'w') as csv_file:
                cursor.copy_expert(f"COPY (SELECT * FROM {table}) TO STDOUT WITH HEADER CSV", csv_file)



default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 0
}

dag = DAG(
    'load_db_dump_dag',
    description="API DAG #2 for homework 4",
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 25, 20, 30),
    default_args=default_args
)

t1 = PythonOperator(
    task_id='load_db_dump_dag',
    python_callable=read_pg,
    dag=dag
)
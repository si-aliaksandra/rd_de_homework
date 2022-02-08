import json
from config import Config
import requests
from requests.exceptions import RequestException
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import psycopg2
import os
from hdfs import InsecureClient

pg_creds = {
    "host": '127.0.0.1'
    , "port": 5432
    , "database": "dshop"
    , "user": "pguser"
    , "password": "secret"
}

def load_from_api(date_to_load, **kwargs):
    client = InsecureClient('http://127.0.0.1:50070', user='user')
    config = Config("./airflow/dags/config.yaml").get_config()
    url = config["url"]
    hdfs_folder = f'/bronze/out_of_stock/{date_to_load.year}/{date_to_load.month}/'
    client.makedirs(hdfs_folder)

    try:
        request_token = requests.post(url + config['auth']['endpoint'], headers=config['auth']['headers'],
                                      data=json.dumps(config['auth']['payload']))
        token = request_token.json()['access_token']
        print(token)

        headers = {"content-type": "application/json", "authorization": "JWT " + token}
        response = requests.get(url + date_to_load.strftime('%Y-%m-%d'), headers=headers,
                                data=json.dumps(date_to_load.strftime('%Y-%m-%d')))
        response.raise_for_status()
        with client.write(hdfs_folder+'out_of_stock'+date_to_load.strftime('_%Y_%m_%d')+'.json') as json_file:
            json.dump(response.json(), json_file)
    except RequestException:
        print("Connection Error")


def read_pg(table, date_to_load, **kwargs):
    client = InsecureClient('http://127.0.0.1:50070', user='user')
    hdfs_folder = f'/bronze/dshop/{date_to_load.year}/{date_to_load.month}/{date_to_load.day}/'
    client.makedirs(hdfs_folder)
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with client.write(hdfs_folder+f'{table}_{date_to_load.strftime("%Y_%m_%d")}.csv') as csv_file:
            cursor.copy_expert(f"COPY (SELECT * FROM {table}) TO STDOUT WITH HEADER CSV", csv_file)


date_to_load = datetime.now()
tables_to_load = ['aisles', 'clients', 'departments', 'orders', 'products']
db_dump_tasks = []

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 0
}

dag = DAG(
    'hw_5_load_to_hdfs_dag',
    description="API DAG for homework 5",
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 25, 20, 30),
    end_date=datetime(2022, 3, 30, 20, 30),
    default_args=default_args
)

t1 = PythonOperator(
    task_id='load_out_of_stock',
    python_callable=load_from_api,
    op_kwargs={'date_to_load': date_to_load},
    dag=dag
)

for table in tables_to_load:
    db_dump_tasks.append(
        PythonOperator(
            task_id=f'load_db_dump_{table}_dag',
            python_callable=read_pg,
            op_kwargs={'table': table, 'date_to_load': date_to_load},
            dag=dag
        ))

t2 = DummyOperator(
    task_id='process_finished',
    dag=dag
)

t1 >> db_dump_tasks >> t2

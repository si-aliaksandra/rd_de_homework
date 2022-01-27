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

pg_creds = {
    "host": '127.0.0.1'
    , "port": 5432
    , "database": "dshop"
    , "user": "pguser"
    , "password": "secret"
}


def main(**kwargs):
    config = Config("./airflow/dags/config.yaml").get_config()

    url = config["url"]
    path_to_dir = os.path.join('/home/user/data', config['API']['payload']['date'])
    os.makedirs(path_to_dir, exist_ok=True)

    try:
        request_token = requests.post(url + config['auth']['endpoint'], headers=config['auth']['headers'],
                                      data=json.dumps(config['auth']['payload']))
        token = request_token.json()['access_token']
        print(token)

        headers = {"content-type": "application/json", "authorization": "JWT " + token}
        response = requests.get(url + config['API']['endpoint'], headers=headers,
                                data=json.dumps(config['API']["payload"]))
        response.raise_for_status()
        with open(os.path.join(path_to_dir, config['API']['payload']['date']), 'w') as json_file:
            json.dump(response.json(), json_file)
    except RequestException:
        print("Connection Error")


def read_pg(table, **kwargs):
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with open(os.path.join('/home/user/data', f'{table}.csv'), 'w') as csv_file:
            cursor.copy_expert(f"COPY (SELECT * FROM {table}) TO STDOUT WITH HEADER CSV", csv_file)


tables_to_load = ['aisles', 'clients', 'departments', 'orders', 'products']
db_dump_tasks = []

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 0
}

dag = DAG(
    'load_api_out_of_stock_data',
    description="API DAG for homework 4",
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 25, 20, 30),
    end_date=datetime(2022, 1, 30, 20, 30),
    default_args=default_args
)

t1 = PythonOperator(
    task_id='load_data_from_api',
    python_callable=main,
    dag=dag
)

for table in tables_to_load:
    db_dump_tasks.append(
        PythonOperator(
            task_id=f'load_db_dump_{table}_dag',
            python_callable=read_pg,
            op_kwargs={'table': table},
            dag=dag
        ))

t2 = DummyOperator(
    task_id='process finished',
    dag=dag
)

t1 >> db_dump_tasks >> t2

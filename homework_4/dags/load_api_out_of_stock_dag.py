import json
import os
from config import Config
import requests
from requests.exceptions import RequestException
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

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
    default_args=default_args
)

t1 = PythonOperator(
    task_id='load_data_from_api',
    python_callable=main,
    dag=dag
)
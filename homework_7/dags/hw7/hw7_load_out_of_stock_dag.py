'''

Постройте data pipeline c помощью Spark, HDFS, Airflow:

Выгрузите данные из dshop PostgreSQL таблиц в RAW(Bronze).
Выгрузите данные на текущий день из out_of_stock API. Если данных на сегодняшний день нет,
то Airflow должен выдавать ошибку c соответствующем log message в Логах к Airflow Task.
Данные в Bronze должны быть партицированы по дате.
Перенесите данные из Bronze в Silver, попутно очистив их и убрав дубликаты.
Данные в Silver должны храниться в формате Parquet.
Добавьте Airflow логи на каждое действие.
Обработка данных из PostgreSQL и из API должна быть в разных Airflow DAG. Каждый отдельный шаг обработки - отдельный Airflow Task.
'''
import json
from plugins.common.config import Config
from hdfs import InsecureClient
import requests
from requests import exceptions
from datetime import datetime
import logging

def load_from_api():
    date = datetime.now()
    client = InsecureClient('http://127.0.0.1:50070', user='user')
    config = Config("./common/config.yaml").get_config()
    bronze_folder = f'/hw7/bronze/out_of_stock/{date.year}/{date.month}'

    try:
        request_token = requests.post(config["url"] + config['auth']['endpoint'],
                                      headers=config['auth']['headers'],
                                      data=json.dumps(config['auth']['payload']),
                                      timeout=120)
        token = request_token.json()['access_token']

        headers = {"content-type": "application/json", "authorization": "JWT " + token}
        response = requests.get(config["url"] + config['API']['endpoint'] + date.strftime('%Y-%m-%d'),
                                headers=headers,
                                data=json.dumps(date.strftime('%Y-%m-%d')),
                                timeout=120)
        logging.log(response.raise_for_status())
        print(bronze_folder + 'out_of_stock' + date.strftime('_%Y_%m_%d') + '.json')
        with client.write(bronze_folder + 'out_of_stock' + date.strftime('_%Y_%m_%d') + '.json') as json_file:
            json.dump(response.json(), json_file)
    except exceptions.Timeout:
        pass
    except exceptions.HTTPError as e:
        logging.exception(e)
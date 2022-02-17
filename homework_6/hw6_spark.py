from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import psycopg2
from hdfs import InsecureClient
import os
from datetime import datetime

pg_creds = {
    'host': '127.0.0.1',
    'port': '5432',
    'database': 'postgres',
    'user': 'pguser',
    'password': 'secret'
}
client = InsecureClient('127.0.0.1:50070', user='user')

with psycopg.connect() as pg_connection:
    cursor = pg_connection.cursor()
    with client.write('/bronze/film.csv') as csv_file:
        pass

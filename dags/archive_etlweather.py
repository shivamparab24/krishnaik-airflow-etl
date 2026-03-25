import logging

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

from src.weather_pipeline.extract_weather import fetch_weather_data
from src.weather_pipeline.load_weather import load_weather_data
from src.weather_pipeline.tranform_weather import transform_weather_data


LATITUDE = '51.5074'
LONGITUDE = '-0.1278'

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='weather_archive_daily_etl',
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule='@daily',
    catchup=True
) as dag:

    # @task()
    # def extract(execution_date=None):
    #     return fetch_weather_data(
    #         latitude=LATITUDE,
    #         longitude=LONGITUDE,
    #         execution_date=execution_date
    #     )

    @task()
    def extract(**context):
        from datetime import timedelta

        # execution_date = context['logical_date']
        # query_date = execution_date - timedelta(days=1)

        execution_date = context['logical_date']
        query_date = execution_date.date()

        return fetch_weather_data(
            latitude=LATITUDE,
            longitude=LONGITUDE,
            execution_date=query_date
        )

    @task()
    def transform(data):
        return transform_weather_data(data, LATITUDE, LONGITUDE)

    @task()
    def load(records):
        load_weather_data(records)

    data = extract()
    transformed = transform(data)
    load(transformed)
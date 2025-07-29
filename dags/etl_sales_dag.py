from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../scripts")))

from extract import extract
from transform import transform
from validate import validate
from load import load

default_args = {
    'start_date': datetime(2025, 1, 1),
    'retries': 0
}

with DAG(
    dag_id='etl_random_user_dag',
    default_args=default_args,
    catchup=False,
    schedule=None,
    tags=['random_user_etl'],
    description='ETL pipeline fetching data from Random User API',
) as dag:
    def extract_task(**kwargs):
        df = extract()
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())

    def validate_task(**kwargs):
        import pandas as pd
        df_json = kwargs['ti'].xcom_pull(key='raw_data', task_ids='extract')
        df = pd.read_json(df_json)
        validate(df)
        kwargs['ti'].xcom_push(key='validated_data', value=df.to_json())

    def transform_task(**kwargs):
        import pandas as pd
        df_json = kwargs['ti'].xcom_pull(key='validated_data', task_ids='validate')
        df = pd.read_json(df_json)
        df_transformed = transform(df)
        kwargs['ti'].xcom_push(key='transformed_data', value=df_transformed.to_json())

    def load_task(**kwargs):
        import pandas as pd
        df_json = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform')
        df = pd.read_json(df_json)
        load(df)

    extract_op = PythonOperator(task_id='extract', python_callable=extract_task)
    validate_op = PythonOperator(task_id='validate', python_callable=validate_task)
    transform_op = PythonOperator(task_id='transform', python_callable=transform_task)
    load_op = PythonOperator(task_id='load', python_callable=load_task)

    extract_op >> validate_op >> transform_op >> load_op

"""def etl_operation():
    df = extract()
    df_tranformed = transform(df)
    load(df_tranformed)

with DAG('sales_etl_dag',
         start_date=pendulum.datetime(2026, 1, 1),
         #schedule='@daily',
         #default_args={"retries": 2},
         catchup=False) as dag:
    etl_task =  PythonOperator(task_id='etl_operation',python_callable=etl_operation)"""
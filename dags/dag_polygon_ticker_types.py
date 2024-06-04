from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


default_args = {
    'owner': 'kazik',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def extract_ticker_types_task():
    import extract
    e = extract.ClientSync()
    raw_data = e.get_all_ticker_types()
    return extract.write_json(raw_data)   
    

with DAG (
    dag_id='dag_polygon_ticker_types_v01',
    description='fetch all ticker types from polygon.io',
    default_args=default_args,
    start_date=datetime(2024,5,23),
    schedule='@daily'
) as dag:
    extract_ticker_types = PythonOperator(
        task_id='extract_ticker_types',
        python_callable=
    )
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from datetime import datetime, timedelta
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


default_args = {
    'owner': 'kazik',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def extract_ticker_types_task(**kwargs):
    import extract
    e = extract.ClientSync(kwargs["api_key"],
                           kwargs["host"])
    raw_data = e.get_all_ticker_types()
    return extract.write_json(raw_data)


def transform_ticker_types_task(ti):
    import extract
    import transform
    t = transform.Transformers()
    raw_json_file = ti.xcom_pull(task_ids='extract_ticker_types')
    raw_data = extract.read_json(raw_json_file)
    transformed_data = t.transform_ticker_types_response(raw_data)
    directory = f"/opt/airflow/dags/data/T/transform_ticker_types_response/test.xml"
    transformed_data.to_xml(directory)
    return directory


def load_ticker_types(ti):
    import extract
    import load
    import pandas as pd
    data_file_path = ti.xcom_pull(task_ids='transform_ticker_types')
    data = pd.read_xml(data_file_path)
    
    mssql_hook = MsSqlHook(mssql_conn_id="mssql")
    print(mssql_hook.default_conn_name)
    
    return data.describe()
    

with DAG (
    dag_id='dag_polygon_ticker_types_v32',
    description='fetch all ticker types from polygon.io',
    default_args=default_args,
    start_date=datetime(2024,6,3),
    schedule='@daily',
    catchup=False  
) as dag:
    task_1 = PythonOperator(
        task_id="extract_ticker_types",
        python_callable=extract_ticker_types_task,
        op_kwargs={
            "api_key": Variable.get("API_KEY"),
            "host": Variable.get("API_HOST")
        }
    )
    
    task_2 = PythonOperator(
        task_id="transform_ticker_types",
        python_callable=transform_ticker_types_task
    )
    
    task_3 = PythonOperator(
        task_id="load_ticker_types",
        python_callable=load_ticker_types
    )
    
    
    task_1 >> task_2 >> task_3
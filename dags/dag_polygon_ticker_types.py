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


def extract_ticker_types_task(ti, **kwargs):
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
    meta = [
        {"operation": "T"},
        {"name": t.transform_ticker_types_response.__name__},
        transformed_data.to_dict()
    ]
    return extract.write_json(meta)
    

with DAG (
    dag_id='dag_polygon_ticker_types_v21',
    description='fetch all ticker types from polygon.io',
    default_args=default_args,
    start_date=datetime(2024,6,3),
    schedule='@daily',
    catchup=False  
) as dag:
    extract_ticker_types = PythonOperator(
        task_id="extract_ticker_types",
        python_callable=extract_ticker_types_task,
        op_kwargs={
            "api_key": Variable.get("API_KEY"),
            "host": Variable.get("API_HOST")
        }
    )
    
    transform_ticker_types = PythonOperator(
        task_id="transform_ticker_types",
        python_callable=transform_ticker_types_task
    )
    
    
    extract_ticker_types >> transform_ticker_types
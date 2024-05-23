from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import extract
import transform
import load

default_args = {
    'owner': "kazik",
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id="dag_polygon_etl_v02",
    description="dag for polygon api etl",
    start_date=datetime(2024, 5, 19),
    schedule='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='extract',
        python_callable=extract.ClientSync().get_all_ticker_types
    )
    
    # task2 = PythonOperator(
    #     task_id='transfer'
        
    # )
    create_table_mssql_task = MsSqlOperator(
        task_id="create_country_table",
        mssql_conn_id="mssql",
        sql=r"""
        CREATE TABLE Country (
            country_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
            name TEXT,
            continent TEXT
        );
        """,
        dag=dag,
    )
    
    # @dag.task(task_id="insert_mssql_task")
    # def insert_mssql_hook():
    #     mssql_hook = MsSqlHook(mssql_conn_id="mssql")

    #     rows = [
    #         ("Katowice Głowne", "Katowice")
    #     ]

    #     # INSERT INTO trains_db.Stations (StationName, Location) VALUES ('Warszawa Zachodnia', 'Warszawa');

    #     target_fields = ["StationName", "Location"]
    #     mssql_hook.insert_rows(table="trains_db.Stations", rows=rows, target_fields=target_fields)
    
    # task3 = PythonOperator(
    #     task_id='load'
        
    # )
    
    task1 >> create_table_mssql_task
    # >> task2 >> task3
# Please check: https://www.analyticsvidhya.com/blog/2021/11/good-etl-practices-with-apache-airflow/
# Please check: C:\Work\Project\Panasonic SG\Old\Airflow Nico\airflow_wsl-master\airflow\dags\test_from_postgres.py

# Import dependencies
from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator

''' 
DAG definition:
    -schedule: every 10 minutes
    -start: 2023-01-01 00:00:00
'''
with DAG('test_processing', start_date=datetime(2023, 1, 1), 
        schedule_interval='@daily', catchup=False) as dag:

    # Task for get all data from source table
    get_all_data_st = PostgresOperator(
        task_id="get_all_data_st",
        postgres_conn_id="postgres-source",
        sql="SELECT * FROM public.test;"
    )

    # # Task for truncate data in destination table
    # truncate_data_dt = MsSqlOperator(
    #     task_id="truncate_data_dt",
    #     mssql_conn_id="sql-server-destination",
    #     sql="TRUNCATE TABLE dbo.test;"
    # )

    # # Task for insert data to destination table
    # insert_data_dt = MsSqlOperator(
    #     task_id="insert_data_dt",
    #     mssql_conn_id="sql-server-destination",
    #     sql="INSERT INTO dbo.test (id, name) VALUES (1, 'test');"
    # )

    # Task order
    get_all_data_st # >> truncate_data_dt >> insert_data_dt
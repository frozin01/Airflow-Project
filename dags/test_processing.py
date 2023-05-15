# Import dependencies
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd

connStringPSQL = 'postgresql+psycopg2://postgres:admin%40database%402022@192.168.7.102/time_report_management2'
connStringMSSQL = 'mssql+pyodbc://user_mb:user%40mb@192.168.7.102/db_mb'

def exec_query(query, connString):
    try:
        sqlEngine = create_engine(connString)
        dbConn = sqlEngine.connect()
        frame = pd.read_sql(query, dbConn)
        dbConn.close()
        return frame
    except Exception as e:
        print(f'Error at query execution: {e}')
        return

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

if __name__ == '__main__':
    # Run test case here...
    pass
# Import dependencies
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import logging
import requests

# Declare variables
postgres_source_url = "postgresql://postgres:123456@host.docker.internal/source"
postgres_destination_url = "postgresql://postgres:123456@host.docker.internal/destination"
default_args = {
    "owner": "frozin01",
    "depends_on_past": True,
    "retries": 3
}

# Declare Function
def execute_query(query, postgres_url):
    try:
        engine = create_engine(postgres_url)
        with engine.connect() as connection:
            data_frame = pd.read_sql(query, connection)
        return data_frame
    except Exception as e:
        print("Error at query execution: " + e)
        return

def db_integration():
    query = "SELECT * FROM public.test;"
    data = execute_query(query, postgres_source_url)
    data.to_sql(
        name="test", con=postgres_destination_url, schema="public", if_exists="append", index=None, index_label=None
    )

def api_integration():
    response = requests.get('https://catfact.ninja/fact')
    data = response.json()
    data_frame = pd.json_normalize(data)
    print(data_frame)
    data_frame.to_sql(
        name="api_test", con=postgres_destination_url, schema="public", if_exists="append", index=None, index_label=None
    )

def check_destination_db():
    query_source_db = "SELECT COUNT(1) AS total_data FROM public.test;"
    query_api = "SELECT COUNT(1) AS total_data FROM public.api_test;"
    data_source_db = execute_query(query_source_db, postgres_destination_url)
    data_api = execute_query(query_api, postgres_destination_url)
    logging.getLogger().setLevel(logging.DEBUG)
    logging.debug("Total Data: " + data_source_db["total_data"].to_string(index=False))
    logging.debug("Total Data: " + data_api["total_data"].to_string(index=False))

# Declare DAG
with DAG(
    dag_id="test_integration",
    start_date=datetime(2023, 1, 1), 
    schedule_interval="*/10 * * * *",
    catchup=False,
    default_args=default_args
) as dag:

    db_integration = PythonOperator(
        task_id="db_integration",
        python_callable=db_integration
    )

    api_integration = PythonOperator(
        task_id="api_integration",
        python_callable=api_integration
    )

    check_destination_db = PythonOperator(
        task_id="check_destination_db",
        python_callable=check_destination_db
    )

    db_integration >> api_integration >> check_destination_db

# Test the functions
# if __name__ == "__main__":
#     fetch_data()
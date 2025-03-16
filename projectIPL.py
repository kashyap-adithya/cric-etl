from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from tasks.ipl.download import download_and_extract_zip
from tasks.ipl.create_tables import CREATE_TABLE_SQL
from tasks.ipl.load_data import load_json_to_postgres
from tasks.ipl.feature_extartcion import extract_features

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'projectIPL',
    default_args=default_args,
    description='Fetch IPL data and store it in Postgres',
    schedule_interval=timedelta(days=1),
)

download_task = PythonOperator(
    task_id='download_ipl_zip',
    python_callable=download_and_extract_zip,
    op_args=["https://cricsheet.org/downloads/ipl_json.zip", "/tmp/ipl_json_data"],
    dag=dag,
)

create_tables_task = PostgresOperator(
    task_id='create_postgres_tables',
    postgres_conn_id='postgres_default',
    sql=CREATE_TABLE_SQL,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_json_data',
    python_callable=load_json_to_postgres,
    op_args=["/tmp/ipl_json_data"],
    dag=dag,
)

extract_feature = PythonOperator(
    task_id='fearure_extraction',
    python_callable=extract_features,
    dag=dag,
)



download_task >> create_tables_task >> load_data_task >> extract_feature
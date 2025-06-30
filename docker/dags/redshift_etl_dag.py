from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from extract_s3 import download_from_s3
from load_redshift import load_to_redshift

def get_redshift_credentials(conn_id='redshift_capstone'):
    conn = BaseHook.get_connection(conn_id)
    return {
        'redshift_host': conn.host,
        'redshift_port': conn.port,
        'redshift_user': conn.login,
        'redshift_password': conn.password,
        'redshift_db': conn.schema
    }

with DAG(
    dag_id='redshift_etl_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['capstone', 'etl']
) as dag:

    start = EmptyOperator(task_id='start')

    extract = PythonOperator(
        task_id='extract_from_s3',
        python_callable=download_from_s3,
        op_kwargs={
            'bucket_name': 'flight-cost-capstone-jhotchkiss',
            'object_key': 'raw/2023_Q1_clean.csv',
            'download_path': '/opt/airflow/data/2023_Q1_clean.csv'
        }
    )

    def load_with_conn(**kwargs):
        creds = get_redshift_credentials()
        load_to_redshift(
            redshift_host=creds['redshift_host'],
            redshift_port=creds['redshift_port'],
            redshift_user=creds['redshift_user'],
            redshift_password=creds['redshift_password'],
            redshift_db=creds['redshift_db'],
            table_name='flight_fares',
            csv_path='/opt/airflow/data/2023_Q1_clean.csv'
        )

    load = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_with_conn
    )

    end = EmptyOperator(task_id='end')

    start >> extract >> load >> end
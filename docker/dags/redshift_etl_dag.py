from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import psycopg2

def get_redshift_credentials(conn_id='redshift_capstone'):
    conn = BaseHook.get_connection(conn_id)
    return {
        'host': conn.host,
        'port': conn.port,
        'user': conn.login,
        'password': conn.password,
        'dbname': conn.schema
    }

def copy_to_redshift(**kwargs):
    creds = get_redshift_credentials()
    iam_role = 'arn:aws:iam::900546257653:role/RedshiftS3AccessRole'
    s3_paths = [
        's3://flight-cost-capstone-jhotchkiss/raw/year=2023/quarter=Q1/2023_Q1_final.csv',
        's3://flight-cost-capstone-jhotchkiss/raw/year=2023/quarter=Q2/2023_Q2_final.csv',
        's3://flight-cost-capstone-jhotchkiss/raw/year=2023/quarter=Q3/2023_Q3_final.csv',
        's3://flight-cost-capstone-jhotchkiss/raw/year=2023/quarter=Q4/2023_Q4_final.csv',
    ]
    
    conn = psycopg2.connect(**creds)
    cur = conn.cursor()

    for path in s3_paths:
        copy_sql = f"""
        COPY flight_fares (year, quarter, origin, dest, reporting_carrier, passengers, market_fare, market_distance)
        FROM '{path}'
        IAM_ROLE '{iam_role}'
        CSV
        IGNOREHEADER 1;
        """
        cur.execute(copy_sql)
        conn.commit()
        print(f"Loaded: {path}")

    cur.close()
    conn.close()

with DAG(
    dag_id='redshift_copy_from_s3_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['capstone', 'redshift', 's3']
) as dag:

    start = EmptyOperator(task_id='start')

    load = PythonOperator(
        task_id='copy_csvs_to_redshift',
        python_callable=copy_to_redshift
    )

    end = EmptyOperator(task_id='end')

    start >> load >> end
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import sys

sys.path.append('/opt/airflow/scripts')
from data_generator import generate_voucher_data

def ingest_discounts():
    # Generate 3 voucher
    data = generate_voucher_data(count=3)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    for row in data:
        sql = """
            INSERT INTO vouchers (voucher_code, voucher_name, discount_type, discount_value, valid_until, created_date) 
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP) 
            ON CONFLICT (voucher_code) DO NOTHING
        """
        cursor.execute(sql, (
            row['voucher_code'], 
            row['voucher_name'], 
            row['discount_type'], 
            row['discount_value'], 
            row['valid_until']
        ))
    
    conn.commit()
    cursor.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG('1_ingest_discounts', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:
    
    task_ingest_discounts = PythonOperator(
        task_id='process_discounts',
        python_callable=ingest_discounts
    )
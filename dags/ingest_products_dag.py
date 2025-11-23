from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import random
import sys

sys.path.append('/opt/airflow/scripts')
from data_generator import generate_product_data

def ingest_products():
    start_id = random.randint(1, 1000)
    data = generate_product_data(start_id=start_id, count=10)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    for row in data:
        sql = """
            INSERT INTO products (product_id, product_name, category, price, created_date) 
            VALUES (%s, %s, %s, %s, %s) 
            ON CONFLICT (product_id) DO NOTHING
        """
        cursor.execute(sql, (row['product_id'], row['product_name'], row['category'], row['price'], row['created_date']))
    
    conn.commit()
    cursor.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG('1_ingest_products', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:
    
    task_ingest_products = PythonOperator(
        task_id='process_products',
        python_callable=ingest_products
    )
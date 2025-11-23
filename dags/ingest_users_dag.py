from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import random
import sys

# Import logic dari scripts
sys.path.append('/opt/airflow/scripts')
from data_generator import generate_user_data

def ingest_users():
    # Random start ID
    start_id = random.randint(1, 2000)
    data = generate_user_data(start_id=start_id, count=20)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    for row in data:
        # Insert ignore duplicate
        sql = """
            INSERT INTO users (user_id, name, email, created_date) 
            VALUES (%s, %s, %s, %s) 
            ON CONFLICT (user_id) DO NOTHING
        """
        cursor.execute(sql, (row['user_id'], row['name'], row['email'], row['created_date']))
    
    conn.commit()
    cursor.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG('1_ingest_users', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:
    
    task_ingest_users = PythonOperator(
        task_id='process_users',
        python_callable=ingest_users
    )
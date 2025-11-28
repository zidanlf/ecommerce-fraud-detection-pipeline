import sys
import logging
import random
import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Import generator script
sys.path.append('/opt/airflow/scripts')
from data_generator import generate_user_data

# Configure logging
logger = logging.getLogger("airflow.task")

def ingest_users(**kwargs):
    """
    Ingest user data into Postgres.
    Skips execution if schedule latency exceeds 2 minutes.
    """
    # 1. Check Schedule Latency
    execution_date = kwargs['data_interval_end']
    current_time = pendulum.now("UTC")
    latency_minutes = (current_time - execution_date).in_minutes()

    logger.info(f"Schedule: {execution_date} | Current: {current_time} | Latency: {latency_minutes} min")

    if latency_minutes > 2:
        logger.warning("Latency threshold exceeded (2 min). Skipping data ingestion.")
        return "Skipped"

    # 2. Execute Ingestion
    start_id = random.randint(1, 2000)
    data = generate_user_data(start_id=start_id, count=20)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    sql = """
        INSERT INTO users (user_id, name, email, created_date) 
        VALUES (%s, %s, %s, %s) 
        ON CONFLICT (user_id) DO NOTHING
    """

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for row in data:
                cursor.execute(sql, (
                    row['user_id'], 
                    row['name'], 
                    row['email'], 
                    row['created_date']
                ))
            conn.commit()
    
    logger.info(f"Transaction committed. Inserted {len(data)} users.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    '1_ingest_users', 
    default_args=default_args, 
    schedule_interval='@hourly', 
    catchup=False
) as dag:
    
    task_ingest_users = PythonOperator(
        task_id='process_users',
        python_callable=ingest_users,
        provide_context=True
    )
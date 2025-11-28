import sys
import logging
import random
import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.append('/opt/airflow/scripts')
from data_generator import generate_product_data

logger = logging.getLogger("airflow.task")

def ingest_products(**kwargs):
    """
    Ingest product data into Postgres.
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
    start_id = random.randint(1, 1000)
    data = generate_product_data(start_id=start_id, count=10)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    sql = """
        INSERT INTO products (product_id, product_name, category, price, created_date) 
        VALUES (%s, %s, %s, %s, %s) 
        ON CONFLICT (product_id) DO NOTHING
    """

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for row in data:
                cursor.execute(sql, (
                    row['product_id'], 
                    row['product_name'], 
                    row['category'], 
                    row['price'], 
                    row['created_date']
                ))
            conn.commit()
    
    logger.info(f"Transaction committed. Inserted {len(data)} products.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    '1_ingest_products', 
    default_args=default_args, 
    schedule_interval='@hourly', 
    catchup=False
) as dag:
    
    task_ingest_products = PythonOperator(
        task_id='process_products',
        python_callable=ingest_products,
        provide_context=True
    )
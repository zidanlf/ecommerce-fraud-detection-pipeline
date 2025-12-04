import sys
import logging
import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Import generator script
sys.path.append('/opt/airflow/scripts')
from data_generator import generate_voucher_data

# Configure logging
logger = logging.getLogger("airflow.task")

def ingest_discounts(**kwargs):
    """
    Ingest voucher data into Postgres.
    Execution is skipped if the schedule latency exceeds 2 minutes.
    """
    # Check Schedule Latency
    execution_date = kwargs['data_interval_end']
    current_time = pendulum.now("UTC")
    latency_minutes = (current_time - execution_date).in_minutes()

    logger.info(f"Schedule: {execution_date} | Current: {current_time} | Latency: {latency_minutes} min")

    if latency_minutes > 2:
        logger.warning("Latency threshold exceeded (2 min). Skipping data ingestion.")
        return "Skipped"

    # Execute Data Ingestion
    data = generate_voucher_data(count=3)
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    sql = """
        INSERT INTO vouchers (voucher_code, voucher_name, discount_type, discount_value, valid_until, created_date) 
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP) 
        ON CONFLICT (voucher_code) DO NOTHING
    """

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for row in data:
                cursor.execute(sql, (
                    row['voucher_code'], 
                    row['voucher_name'], 
                    row['discount_type'], 
                    row['discount_value'], 
                    row['valid_until']
                ))
            conn.commit()
    
    logger.info(f"Transaction committed. Rows inserted: {len(data)}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    '1_ingest_discounts', 
    default_args=default_args, 
    schedule_interval='@hourly', 
    catchup=False
) as dag:
    
    task_ingest = PythonOperator(
        task_id='process_discounts',
        python_callable=ingest_discounts,
        provide_context=True
    )
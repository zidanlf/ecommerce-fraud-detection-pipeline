import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# --- CONFIGURATION ---
PROJECT_ID = 'finpro-purwadhika'
DATASET_ID = "zidan_finpro"
DISCORD_WEBHOOK = os.getenv('DISCORD_WEBHOOK_URL')

logger = logging.getLogger("airflow.task")

# --- ALERTING ---
def send_discord_alert(context, status_type):
    if not DISCORD_WEBHOOK:
        return

    ti = context.get('task_instance')
    color = 15158332 if status_type == "FAILED" else 16776960
    
    payload = {
        "username": "Airflow Monitor",
        "embeds": [{
            "title": f"DAG {status_type}",
            "color": color,
            "fields": [
                {"name": "DAG", "value": ti.dag_id, "inline": True},
                {"name": "Task", "value": ti.task_id, "inline": True},
                {"name": "Date", "value": context.get('ds'), "inline": True},
                {"name": "Error", "value": str(context.get('exception'))[:300]}
            ]
        }]
    }
    
    try:
        requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"Discord alert failed: {e}")

# --- SIMPLE ETL ---
def load_to_bigquery(table_name, **kwargs):
    
    # 1. EXTRACT FROM POSTGRES
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    execution_date = kwargs['ds']
    
    sql = f"""
        SELECT * FROM {table_name} 
        WHERE DATE(created_date) = '{execution_date}'
    """
    
    logger.info(f"Loading {table_name} for date: {execution_date}")
    df = pg_hook.get_pandas_df(sql)

    if df.empty:
        logger.warning(f"No data for {table_name} on {execution_date}")
        return "No data"

    logger.info(f"Found {len(df)} rows")

    # 2. TRANSFORM
    if 'created_date' in df.columns:
        df['created_date'] = pd.to_datetime(df['created_date'])

    # 3. LOAD TO BIGQUERY (SIMPLE APPEND)
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    # Create table if not exists
    try:
        bq_client.get_table(table_id)
    except NotFound:
        logger.info(f"Creating table: {table_id}")
        schema = [bigquery.SchemaField(col, "STRING") for col in df.columns]
        table = bigquery.Table(table_id, schema=schema)
        
        # Add partitioning
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_date"
        )
        bq_client.create_table(table)
    
    # Load data (APPEND mode)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )
    
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    
    logger.info(f"✓ Loaded {len(df)} rows to {table_id}")
    return f"Success: {len(df)} rows"

# --- DAG DEFINITION ---
default_args = {
    'owner': 'zidan',
    'start_date': datetime(2025, 11, 24),
    'retries': 0,
    'on_failure_callback': lambda context: send_discord_alert(context, "FAILED")
}

with DAG(
    '2_ingest_to_bigquery',
    default_args=default_args,
    description='Simple Postgres to BigQuery ETL',
    schedule_interval='40 8 * * *',
    catchup=True,
    max_active_runs=1,
    tags=['etl']
) as dag:

    # Create tasks
    load_users = PythonOperator(
        task_id='load_users',
        python_callable=load_to_bigquery,
        op_kwargs={'table_name': 'users'}
    )
    
    load_products = PythonOperator(
        task_id='load_products',
        python_callable=load_to_bigquery,
        op_kwargs={'table_name': 'products'}
    )
    
    load_vouchers = PythonOperator(
        task_id='load_vouchers',
        python_callable=load_to_bigquery,
        op_kwargs={'table_name': 'vouchers'}
    )
    
    load_orders = PythonOperator(
        task_id='load_orders',
        python_callable=load_to_bigquery,
        op_kwargs={'table_name': 'orders'}
    )

    # Dependencies
    [load_users, load_products, load_vouchers] >> load_orders
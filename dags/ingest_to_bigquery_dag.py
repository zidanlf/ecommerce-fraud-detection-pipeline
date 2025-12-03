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
PROJECT_ID = 'jcdeah-006'
DATASET_ID = "zidan_finpro"
DISCORD_WEBHOOK = os.getenv('DISCORD_WEBHOOK_URL')

logger = logging.getLogger("airflow.task")

# --- ALERTING ---
def send_discord_alert(context, status_type):
    if not DISCORD_WEBHOOK: return
    ti = context.get('task_instance')
    try:
        requests.post(DISCORD_WEBHOOK, json={
            "username": "Airflow",
            "content": f"DAG {ti.dag_id} - {ti.task_id}: {status_type}\nError: {str(context.get('exception'))[:200]}"
        })
    except: pass

# --- ETL FUNCTION ---
def load_to_bigquery(table_name, **kwargs):
    
    # 1. EXTRACT
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    execution_date = kwargs['ds'] 
    
    # Ambil data sesuai tanggal
    sql = f"SELECT * FROM {table_name} WHERE DATE(created_date) = '{execution_date}'"
    logger.info(f"Loading {table_name} for date: {execution_date}")
    df = pg_hook.get_pandas_df(sql)

    if df.empty:
        logger.warning(f"No data for {table_name}")
        return "No data"

    # --- 2. TRANSFORM (FIX TIPE DATA) ---
    # List kolom tanggal yang harus jadi TIMESTAMP
    target_date_cols = ['created_date', 'valid_until', 'registered_date']
    
    for col in target_date_cols:
        if col in df.columns:
            # Ubah ke datetime
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # PENTING: Paksa ke UTC Timezone agar dianggap TIMESTAMP oleh BigQuery
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize('UTC')
            else:
                df[col] = df[col].dt.tz_convert('UTC')

    # Pastikan kolom numeric bersih
    numeric_cols = ['amount', 'price', 'quantity', 'discount_value']
    for col in numeric_cols:
        if col in df.columns:
             df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # --- 3. PREPARE SCHEMA (SEBELUM LOAD) ---
    # Kita definisikan schema secara eksplisit agar tidak salah tebak
    bq_schema = []
    for col, dtype in df.dtypes.items():
        # Mapping tipe data
        if pd.api.types.is_datetime64_any_dtype(dtype): 
            bq_type = "TIMESTAMP" # Timezone aware
        elif pd.api.types.is_integer_dtype(dtype): 
            bq_type = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype): 
            bq_type = "FLOAT"
        else: 
            bq_type = "STRING"
        
        bq_schema.append(bigquery.SchemaField(col, bq_type))

    # --- 4. BIGQUERY CONNECTION ---
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    # Cek/Buat Tabel (Idempotent)
    try:
        bq_client.get_table(table_id)
    except NotFound:
        logger.info(f"Creating table: {table_id}")
        table = bigquery.Table(table_id, schema=bq_schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_date"
        )
        bq_client.create_table(table)

    # --- 5. LOAD (PARTITION OVERWRITE) ---
    partition_suffix = execution_date.replace('-', '')
    target_table_id = f"{table_id}${partition_suffix}"
    
    job_config = bigquery.LoadJobConfig(
        # PENTING: Masukkan schema yang sudah kita buat tadi ke sini
        schema=bq_schema, 
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_date"
        )
    )
    
    logger.info(f"Uploading to partition: {target_table_id}")
    job = bq_client.load_table_from_dataframe(df, target_table_id, job_config=job_config)
    job.result()
    
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
    description='Postgres to BigQuery ETL',
    schedule_interval='10 3 * * *',
    catchup=False,
    max_active_runs=1
) as dag:

    # Task Generator
    tables = ['users', 'products', 'vouchers', 'orders']
    tasks = {}

    for t in tables:
        tasks[t] = PythonOperator(
            task_id=f'load_{t}',
            python_callable=load_to_bigquery,
            op_kwargs={'table_name': t}
        )

    # Dependencies
    [tasks['users'], tasks['products'], tasks['vouchers']] >> tasks['orders']
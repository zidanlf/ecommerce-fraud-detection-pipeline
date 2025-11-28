import os
import logging
import requests
import pandas as pd
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# --- CONFIGURATION ---
PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'jcdeah-006') 
DATASET_ID = "zidan_finpro"
DISCORD_WEBHOOK = os.getenv('DISCORD_WEBHOOK_URL')

PK_MAPPING = {
    'users': 'user_id',
    'products': 'product_id',
    'orders': 'order_id',
    'vouchers': 'voucher_code'
}

logger = logging.getLogger("airflow.task")

# --- ALERTING UTILITIES ---
def send_discord_alert(context, status_type):
    """Generic function to send alerts (Failure/Retry)"""
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
                {"name": "Try", "value": f"{ti.try_number}", "inline": True},
                {"name": "Execution Date", "value": context.get('ds'), "inline": True},
                {"name": "Exception", "value": str(context.get('exception'))[:300]}
            ],
            "timestamp": datetime.utcnow().isoformat()
        }]
    }
    
    try:
        requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"Failed to send Discord alert: {e}")

# --- CORE ETL LOGIC ---
def etl_postgres_to_bq(table_name, **kwargs):
    
    # 1. LATENCY CHECK (Skip if delayed > 2 mins)
    expected_run = kwargs['data_interval_end']
    current_time = pendulum.now("UTC")
    latency_minutes = (current_time - expected_run).in_minutes()

    logger.info(f"Schedule: {expected_run} | Current: {current_time} | Latency: {latency_minutes} min")

    if latency_minutes > 2:
        logger.warning("Latency threshold exceeded (2 min). Skipping ETL process to maintain schedule integrity.")
        return "Skipped (Late Run)"

    # 2. INITIALIZE CONNECTIONS
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    bq_client = bigquery.Client(project=PROJECT_ID)
    execution_date = kwargs['ds']
    primary_key = PK_MAPPING.get(table_name)

    logger.info(f"Starting ETL for {table_name} on {execution_date}")

    # 3. EXTRACT (Postgres)
    sql = f"SELECT * FROM {table_name} WHERE DATE(created_date) = '{execution_date}'"
    df = pg_hook.get_pandas_df(sql)

    if df.empty:
        logger.warning(f"No data found for {table_name} on {execution_date}. Skipping.")
        return

    # 4. TRANSFORM (Pandas)
    if 'created_date' in df.columns:
        df['created_date'] = pd.to_datetime(df['created_date'])

    # 5. LOAD TO STAGING (BigQuery)
    staging_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}_staging"
    target_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bq_client.load_table_from_dataframe(df, staging_id, job_config=job_config)
    job.result()
    logger.info(f"Loaded {len(df)} rows to staging: {staging_id}")

    # 6. ENSURE TARGET TABLE EXISTS
    try:
        bq_client.get_table(target_id)
    except NotFound:
        logger.info(f"Target table not found. Creating partitioned table: {target_id}")
        schema = bq_client.get_table(staging_id).schema
        table = bigquery.Table(target_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="created_date"
        )
        bq_client.create_table(table)

    # 7. MERGE (UPSERT STRATEGY)
    schema = bq_client.get_table(staging_id).schema
    cols = [field.name for field in schema]
    update_clause = ", ".join([f"T.{c}=S.{c}" for c in cols if c != primary_key])
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"S.{c}" for c in cols])

    merge_query = f"""
        MERGE `{target_id}` T
        USING `{staging_id}` S
        ON T.{primary_key} = S.{primary_key}
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    
    bq_client.query(merge_query).result()
    logger.info(f"Merge completed successfully for {table_name}")

    # 8. CLEANUP
    bq_client.delete_table(staging_id)
    logger.info("Staging table cleaned up.")

# --- DAG DEFINITION ---
default_args = {
    'owner': 'zidan',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': lambda context: send_discord_alert(context, "FAILED"),
    'on_retry_callback': lambda context: send_discord_alert(context, "RETRY")
}

with DAG(
    '2_ingest_to_bigquery',
    default_args=default_args,
    description='Incremental load Postgres to BigQuery with Merge Strategy',
    schedule_interval='0 8 * * *',
    catchup=False,
    tags=['etl', 'production']
) as dag:

    tasks = {}
    for table in PK_MAPPING.keys():
        tasks[table] = PythonOperator(
            task_id=f'load_{table}',
            python_callable=etl_postgres_to_bq,
            op_kwargs={'table_name': table},
            provide_context=True
        )

    [tasks['users'], tasks['products'], tasks['vouchers']] >> tasks['orders']
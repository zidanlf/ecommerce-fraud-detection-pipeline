from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas as pd
import requests
import os

# --- KONFIGURASI ---
PROJECT_ID = "finpro-purwadhika"
DATASET_ID = "zidan_finpro"
DISCORD_WEBHOOK = os.getenv('DISCORD_WEBHOOK_URL')

# --- ALERTING FUNCTION ---
def on_failure_callback(context):
    """Kirim notifikasi ke Discord saat DAG Gagal"""
    task_instance = context.get('task_instance')
    task_name = task_instance.task_id
    dag_name = task_instance.dag_id
    error_msg = str(context.get('exception'))
    
    payload = {
        "username": "Airflow Bot",
        "embeds": [{
            "title": "❌ DAG FAILED",
            "color": 15158332, # Merah
            "fields": [
                {"name": "DAG", "value": dag_name, "inline": True},
                {"name": "Task", "value": task_name, "inline": True},
                {"name": "Error", "value": error_msg[:200]} # Potong biar gak kepanjangan
            ]
        }]
    }
    try:
        requests.post(DISCORD_WEBHOOK, json=payload)
    except:
        pass

# --- CORE ETL FUNCTION ---
def load_table_to_bq(table_name, **kwargs):
    # 1. Setup Koneksi
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    bq_client = bigquery.Client()
    
    # 2. Logic Filter H-1 (Kemarin)
    # Execution Date di Airflow itu biasanya H-1 dari waktu jalan sebenernya.
    # Kita ambil tanggal eksekusi data.
    execution_date = kwargs['ds'] 
    
    print(f"\n[INFO] Processing Table: {table_name} for Date: {execution_date}")
    
    # 3. Query Postgres
    sql = f"""
        SELECT * FROM {table_name}
        WHERE DATE(created_date) = '{execution_date}'
    """
    
    # Khusus tabel dimensi (users/products/vouchers), mungkin kita mau ambil semua (Full Load)
    # atau incremental. Sesuai soal: "Data pada setiap tabel di filter per hari (H-1)"
    # Jadi kita pakai filter di atas.
    
    df = pg_hook.get_pandas_df(sql)
    
    if df.empty:
        print(f"[WARN] No data found for {table_name} on {execution_date}. Skipping.")
        return

    # 4. Data Type Fixing (Penting untuk BigQuery)
    # Pastikan created_date jadi datetime
    if 'created_date' in df.columns:
        df['created_date'] = pd.to_datetime(df['created_date'])

    # 5. Load to BigQuery
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        # Schema Partitioning (Sesuai Soal)
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_date"  # Partisi berdasarkan kolom ini
        ),
        write_disposition="WRITE_APPEND", # Incremental Load (Append)
    )

    job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    
    job.result() # Tunggu sampai selesai
    
    print(f"[SUCCESS] Loaded {len(df)} rows to {table_id}")

# --- DAG DEFINITION ---
default_args = {
    'owner': 'zidan',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback # Pasang Alert Discord
}

with DAG(
    '2_ingest_to_bigquery',
    default_args=default_args,
    schedule_interval='0 1 * * *', # Jalan jam 1 pagi setiap hari
    catchup=False
) as dag:

    # Task 1: Load Users
    t_users = PythonOperator(
        task_id='load_users',
        python_callable=load_table_to_bq,
        op_kwargs={'table_name': 'users'},
        provide_context=True
    )

    # Task 2: Load Products
    t_products = PythonOperator(
        task_id='load_products',
        python_callable=load_table_to_bq,
        op_kwargs={'table_name': 'products'},
        provide_context=True
    )

    # Task 3: Load Orders (Streaming Result)
    t_orders = PythonOperator(
        task_id='load_orders',
        python_callable=load_table_to_bq,
        op_kwargs={'table_name': 'orders'},
        provide_context=True
    )
    
    # Task 4: Load Vouchers
    t_vouchers = PythonOperator(
        task_id='load_vouchers',
        python_callable=load_table_to_bq,
        op_kwargs={'table_name': 'vouchers'},
        provide_context=True
    )

    # Bisa jalan paralel
    [t_users, t_products, t_orders, t_vouchers]
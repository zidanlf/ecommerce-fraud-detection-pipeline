from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Konfigurasi Default
default_args = {
    'owner': 'zidan',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definisi DAG
with DAG(
    '3_transform_fraud_analytics',
    default_args=default_args,
    description='Menjalankan transformasi DBT untuk Fraud Analytics',
    schedule_interval='0 9 * * *',  # Jalankan jam 09:00 (Setelah Ingest BigQuery selesai)
    catchup=False,
    tags=['dbt', 'analytics']
) as dag:

    # Task 1: DBT Debug (Opsional - untuk memastikan koneksi aman)
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command='cd /opt/airflow/fraud_analytics && dbt debug --profiles-dir .'
    )

    # Task 2: DBT Run (Membuat Tabel/View)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/fraud_analytics && dbt run --profiles-dir .'
    )

    # Task 3: DBT Test (Menjalankan tes data quality jika ada)
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/fraud_analytics && dbt test --profiles-dir .'
    )

    # Alur Eksekusi
    dbt_debug >> dbt_run >> dbt_test
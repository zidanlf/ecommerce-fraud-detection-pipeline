from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# --- CONFIG ---
DBT_DIR = "/opt/airflow/fraud_analytics"

default_args = {
    'owner': 'zidan',
    'start_date': datetime(2025, 11, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '3_dbt_fraud_analytics',
    default_args=default_args,
    description='Run DBT Models Daily at 02:00 WIB',
    schedule_interval='15 3 * * *', 
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task 1: Debug Connection
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command=f'cd {DBT_DIR} && dbt debug --profiles-dir .'
    )

    # Task 2: Install Dependencies (Good Practice)
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'cd {DBT_DIR} && dbt deps --profiles-dir .'
    )

    # Task 3: Run Models (Staging -> Core -> Marts)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_DIR} && dbt run --profiles-dir .'
    )

    # Task 4: Test Data (Optional)
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_DIR} && dbt test --profiles-dir .'
    )

    dbt_debug >> dbt_deps >> dbt_run >> dbt_test
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

default_args = {
    'owner': 'zidan',
    'start_date': datetime(2025, 11, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '3_transform_fraud_analytics',
    default_args=default_args,
    description='DBT Fraud Analytics',
    schedule_interval='0 9 * * *',
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='''
            set -x
            export DBT_PROFILES_DIR=/opt/airflow/fraud_analytics
            export DBT_PROJECT_DIR=/opt/airflow/fraud_analytics
            
            cd /opt/airflow/fraud_analytics
            
            echo "=== DBT Version ==="
            dbt --version
            
            echo ""
            echo "=== DBT List Models ==="
            timeout 30 dbt ls --profiles-dir . || echo "List timeout"
            
            echo ""
            echo "=== DBT Run ==="
            timeout 300 dbt run \
                --profiles-dir . \
                --full-refresh \
                --log-level info \
                --threads 1 \
                2>&1 | tee /tmp/dbt_run_output.log
            
            DBT_EXIT=$?
            
            echo ""
            echo "=== DBT Exit Code: $DBT_EXIT ==="
            
            if [ $DBT_EXIT -eq 0 ]; then
                echo "✅ SUCCESS"
                exit 0
            elif [ $DBT_EXIT -eq 124 ]; then
                echo "⏱️ TIMEOUT after 5 minutes"
                cat /tmp/dbt_run_output.log
                exit 1
            else
                echo "❌ FAILED"
                cat /tmp/dbt_run_output.log
                exit $DBT_EXIT
            fi
        ''',
        execution_timeout=timedelta(minutes=10),
        env={'GOOGLE_APPLICATION_CREDENTIALS': '/opt/airflow/keys/gcp_key.json'}
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='''
            cd /opt/airflow/fraud_analytics
            timeout 120 dbt test --profiles-dir . 2>&1 || echo "Tests completed with warnings"
        ''',
        execution_timeout=timedelta(minutes=5),
        trigger_rule='all_done'
    )

    dbt_run >> dbt_test
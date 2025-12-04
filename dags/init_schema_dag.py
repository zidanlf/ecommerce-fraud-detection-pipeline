from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG('0_init_schema', default_args=default_args, schedule_interval='@once', catchup=False) as dag:

    # Tabel Users
    create_users = PostgresOperator(
        task_id='create_users',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                created_date TIMESTAMP
            );
        """
    )

    # Tabel Products
    create_products = PostgresOperator(
        task_id='create_products',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS products (
                product_id VARCHAR(50) PRIMARY KEY,
                product_name VARCHAR(100),
                category VARCHAR(50),
                price INT,
                created_date TIMESTAMP
            );
        """
    )
    
    # Tabel Vouchers
    create_vouchers = PostgresOperator(
        task_id='create_vouchers',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS vouchers (
                voucher_code VARCHAR(50) PRIMARY KEY,
                voucher_name VARCHAR(100),
                discount_type VARCHAR(20),
                discount_value INT,
                valid_until DATE,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    # Tabel Orders
    create_orders = PostgresOperator(
        task_id='create_orders',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS orders (
                order_id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(50),
                product_id VARCHAR(50),
                quantity INT,
                amount INT,
                voucher_code VARCHAR(50),
                country VARCHAR(50),
                created_date TIMESTAMP,
                status VARCHAR(20)
            );
        """
    )

    [create_users, create_products, create_vouchers] >> create_orders
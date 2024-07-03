import os
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'transfer_data_dag',
    default_args=default_args,
    description='Transfer updated data from NDS to DDS',
    schedule_interval='@daily',
)

def connect_to_nds():
    pg_hook = PostgresHook(postgres_conn_id='nds_postgres')
    return pg_hook.get_conn()

def connect_to_dds():
    pg_hook = PostgresHook(postgres_conn_id='dds_postgres')
    return pg_hook.get_conn()

def extract_updated_data(**kwargs):
    nds_conn = connect_to_nds()
    nds_cur = nds_conn.cursor()

    # Извлечение обновленных данных из NDS
    nds_cur.execute("""
        SELECT * FROM Sales WHERE updated_at > (
            SELECT COALESCE(MAX(updated_at), '1970-01-01')
            FROM Sales
        )
    """)
    updated_sales = nds_cur.fetchall()

    nds_cur.close()
    nds_conn.close()

    # Поместить данные в контекст задачи для использования в последующих задачах
    kwargs['ti'].xcom_push(key='updated_sales', value=updated_sales)

def load_data_to_dds(**kwargs):
    updated_sales = kwargs['ti'].xcom_pull(key='updated_sales', task_ids='extract_data')
    dds_conn = connect_to_dds()
    dds_cur = dds_conn.cursor()

    for sale in updated_sales:
        dds_cur.execute("""
            INSERT INTO fact_sales (invoice_id, branch_id, city_id, customer_id, product_line_id, unit_price, quantity, tax_5_percent, total, date_id, time_id, payment_id, cost_of_goods_sold, gross_margin_percentage, gross_income, rating, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (invoice_id) DO UPDATE
            SET branch_id = EXCLUDED.branch_id, city_id = EXCLUDED.city_id, customer_id = EXCLUDED.customer_id, product_line_id = EXCLUDED.product_line_id, unit_price = EXCLUDED.unit_price, quantity = EXCLUDED.quantity, tax_5_percent = EXCLUDED.tax_5_percent, total = EXCLUDED.total, date_id = EXCLUDED.date_id, time_id = EXCLUDED.time_id, payment_id = EXCLUDED.payment_id, cost_of_goods_sold = EXCLUDED.cost_of_goods_sold, gross_margin_percentage = EXCLUDED.gross_margin_percentage, gross_income = EXCLUDED.gross_income, rating = EXCLUDED.rating, updated_at = EXCLUDED.updated_at
        """, sale)

    dds_conn.commit()
    dds_cur.close()
    dds_conn.close()

wait_for_nds_load = ExternalTaskSensor(
    task_id='wait_for_nds_load',
    external_dag_id='process_data_dag',  # ID вашего DAG для загрузки данных в NDS
    external_task_id='process_data',
    allowed_states=['success'],
    failed_states=['failed'],
    mode='poke',
    poke_interval=30,
    timeout=600,
    dag=dag,
)

extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_updated_data,
    provide_context=True,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_dds,
    provide_context=True,
    dag=dag,
)

wait_for_nds_load >> extract_data_task >> load_data_task

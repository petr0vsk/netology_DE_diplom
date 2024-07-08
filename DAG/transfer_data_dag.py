import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['petr0vskjy.aleksander@gmail.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Определение DAG
dag = DAG(
    'transfer_data_dag',
    default_args=default_args,
    description='Transfer data from NDS to DDS',
    schedule_interval=None,
)

# Функция для подключения к базе данных
def connect_to_db(conn_id):
    logging.info(f"Connecting to the database with conn_id: {conn_id}")
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    conn = pg_hook.get_conn()
    logging.info("Connection established.")
    return conn

# Функция для логирования статуса загрузки
def log_load_status(conn, load_id, status, message=None):
    try:
        cur = conn.cursor()
        logging.info(f"Inserting load status for load_id: {load_id}, status: {status}, message: {message}")
        cur.execute("""
            INSERT INTO load_status (load_id, status_time, status, message)
            VALUES (%s, %s, %s, %s)
        """, (load_id, datetime.now(), status, message))
        conn.commit()
        cur.close()
        logging.info("Load status inserted successfully.")
    except Exception as e:
        logging.error(f"Error inserting load status: {str(e)}")
        raise

# Функция для начала загрузки
def start_load(dag_id, task_id):
    try:
        conn = connect_to_db('postgres_dds')
        cur = conn.cursor()
        logging.info(f"Inserting load history for DAG: {dag_id}, task: {task_id}")
        cur.execute("""
            INSERT INTO load_history (dag_id, task_id, start_time, status)
            VALUES (%s, %s, %s, %s) RETURNING load_id
        """, (dag_id, task_id, datetime.now(), 'in_progress'))
        load_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Load history started with load_id: {load_id}")
        return load_id
    except Exception as e:
        logging.error(f"Error starting load: {str(e)}")
        raise

# Функция для завершения загрузки
def end_load(load_id, status, row_count=None, error_message=None):
    try:
        conn = connect_to_db('postgres_dds')
        cur = conn.cursor()
        logging.info(f"Updating load history for load_id: {load_id}, status: {status}, row_count: {row_count}, error_message: {error_message}")
        cur.execute("""
            UPDATE load_history
            SET end_time = %s, status = %s, row_count = %s, error_message = %s
            WHERE load_id = %s
        """, (datetime.now(), status, row_count, error_message, load_id))
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Load history updated successfully.")
    except Exception as e:
        logging.error(f"Error ending load: {str(e)}")
        raise

# Функция для получения новых данных из базы данных NDS
def extract_data():
    conn = connect_to_db('postgres_nds')
    query = """
        SELECT s.*, 
               b.branch_name, 
               c.city_name, 
               cu.customer_type, cu.gender, 
               pl.product_line_name, 
               p.payment_type
        FROM Sales s
        JOIN Branch b ON s.branch_id = b.branch_id
        JOIN City c ON s.city_id = c.city_id
        JOIN Customer cu ON s.customer_id = cu.customer_id
        JOIN ProductLine pl ON s.product_line_id = pl.product_line_id
        JOIN Payment p ON s.payment_id = p.payment_id
        WHERE s.is_transferred = FALSE;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    logging.info(f"Extracted {len(df)} rows from NDS.")
    return df

# Функция для вставки данных в базу данных DDS
def load_data_to_dds(df, load_id):
    conn = connect_to_db('postgres_dds')
    cur = conn.cursor()
    logging.info(f"Loading data to DDS. Data shape: {df.shape}")

    # Вставка уникальных значений в таблицы измерений
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO dim_branch (branch_name) VALUES (%s)
            ON CONFLICT (branch_name) DO NOTHING;
        """, (row['branch_name'],))
        cur.execute("""
            INSERT INTO dim_city (city_name) VALUES (%s)
            ON CONFLICT (city_name) DO NOTHING;
        """, (row['city_name'],))
        cur.execute("""
            INSERT INTO dim_customer (customer_type, gender) VALUES (%s, %s)
            ON CONFLICT (customer_type, gender) DO NOTHING;
        """, (row['customer_type'], row['gender']))
        cur.execute("""
            INSERT INTO dim_product_line (product_line_name) VALUES (%s)
            ON CONFLICT (product_line_name) DO NOTHING;
        """, (row['product_line_name'],))
        cur.execute("""
            INSERT INTO dim_payment (payment_type) VALUES (%s)
            ON CONFLICT (payment_type) DO NOTHING;
        """, (row['payment_type'],))
        cur.execute("""
            INSERT INTO dim_date (date, year, quarter, month, day, day_of_week)
            VALUES (%s, EXTRACT(YEAR FROM %s), EXTRACT(QUARTER FROM %s), EXTRACT(MONTH FROM %s), EXTRACT(DAY FROM %s), TO_CHAR(%s, 'Day'))
            ON CONFLICT (date) DO NOTHING;
        """, (row['date'], row['date'], row['date'], row['date'], row['date'], row['date']))
        cur.execute("""
            INSERT INTO dim_time (time, hour, minute, second)
            VALUES (%s, EXTRACT(HOUR FROM %s), EXTRACT(MINUTE FROM %s), EXTRACT(SECOND FROM %s))
            ON CONFLICT (time) DO NOTHING;
        """, (row['time'], row['time'], row['time'], row['time']))

    # Вставка данных в таблицу фактов
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO fact_sales (
                invoice_id, branch_id, city_id, customer_id, product_line_id, unit_price, quantity, 
                tax_5_percent, total, date_id, time_id, payment_id, cost_of_goods_sold, 
                gross_margin_percentage, gross_income, rating
            )
            VALUES (
                %s,
                (SELECT branch_id FROM dim_branch WHERE branch_name = %s),
                (SELECT city_id FROM dim_city WHERE city_name = %s),
                (SELECT customer_id FROM dim_customer WHERE customer_type = %s AND gender = %s),
                (SELECT product_line_id FROM dim_product_line WHERE product_line_name = %s),
                %s, %s, %s, %s,
                (SELECT date_id FROM dim_date WHERE date = %s),
                (SELECT time_id FROM dim_time WHERE time = %s),
                (SELECT payment_id FROM dim_payment WHERE payment_type = %s),
                %s, %s, %s, %s
            )
            ON CONFLICT (invoice_id) DO NOTHING;
        """, (
            row['invoice_id'], row['branch_name'], row['city_name'], row['customer_type'], row['gender'], row['product_line_name'],
            row['unit_price'], row['quantity'], row['tax_5_percent'], row['total'],
            row['date'], row['time'], row['payment_type'], row['cost_of_goods_sold'], row['gross_margin_percentage'], row['gross_income'], row['rating']
        ))

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Data loaded to DDS successfully.")

    # Обновление поля is_transferred в NDS
    conn_nds = connect_to_db('postgres_nds')
    cur_nds = conn_nds.cursor()
    invoice_ids = tuple(df['invoice_id'].tolist())
    cur_nds.execute("""
        UPDATE Sales
        SET is_transferred = TRUE
        WHERE invoice_id IN %s;
    """, (invoice_ids,))
    conn_nds.commit()
    cur_nds.close()
    conn_nds.close()
    logging.info("NDS Sales table updated successfully.")

# Основная функция выполнения процесса
def main(**kwargs):
    dag_id = kwargs['dag'].dag_id
    task_id = kwargs['task'].task_id
    load_id = start_load(dag_id, task_id)
    conn = connect_to_db('postgres_dds')

    try:
        log_load_status(conn, load_id, 'processing', 'Starting main function')
        logging.info("Starting main function")
        df = extract_data()
        if not df.empty:
            load_data_to_dds(df, load_id)
            row_count = len(df)
        else:
            logging.info("No new data to transfer.")
            row_count = 0

        log_load_status(conn, load_id, 'completed', 'Main function completed successfully')
        end_load(load_id, 'success', row_count)
    except Exception as e:
        log_load_status(conn, load_id, 'failed', str(e))
        end_load(load_id, 'failed', error_message=str(e))
        raise
    finally:
        conn.close()

# Определение задачи для обработки данных
process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=main,
    provide_context=True,
    dag=dag,
)

process_data_task

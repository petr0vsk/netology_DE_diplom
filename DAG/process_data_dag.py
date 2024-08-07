import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

# Пути к входной и архивной директориям
input_path = "/home/petr0vsk/WorkSQL/Diplom/input"
tar_path = "/home/petr0vsk/WorkSQL/Diplom/tar"

# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['petr0vskjy.aleksander@gmail.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# Определение DAG
dag = DAG(
    'process_data_dag',
    default_args=default_args,
    description='Process data and load to PostgreSQL',
    schedule_interval=None,
)

# Функция для подключения к базе данных
def connect_to_db(conn_id='postgres_nds'):
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

# Функция для создания таблиц в базе данных
def create_tables():
    commands = (
        """
        CREATE TABLE IF NOT EXISTS Branch (
            branch_id SERIAL PRIMARY KEY,
            branch_name VARCHAR(10) UNIQUE NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS City (
            city_id SERIAL PRIMARY KEY,
            city_name VARCHAR(50) UNIQUE NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Customer (
            customer_id SERIAL PRIMARY KEY,
            customer_type VARCHAR(10) NOT NULL,
            gender VARCHAR(10) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ProductLine (
            product_line_id SERIAL PRIMARY KEY,
            product_line_name VARCHAR(50) UNIQUE NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Payment (
            payment_id SERIAL PRIMARY KEY,
            payment_type VARCHAR(20) UNIQUE NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Sales (
            sale_id SERIAL PRIMARY KEY,
            invoice_id VARCHAR(20) UNIQUE NOT NULL,
            branch_id INT REFERENCES Branch(branch_id),
            city_id INT REFERENCES City(city_id),
            customer_id INT REFERENCES Customer(customer_id),
            product_line_id INT REFERENCES ProductLine(product_line_id),
            unit_price NUMERIC(10, 2) NOT NULL,
            quantity INT NOT NULL,
            tax_5_percent NUMERIC(10, 2) NOT NULL,
            total NUMERIC(10, 2) NOT NULL,
            date DATE NOT NULL,
            time TIME NOT NULL,
            payment_id INT REFERENCES Payment(payment_id),
            cost_of_goods_sold NUMERIC(10, 2) NOT NULL,
            gross_margin_percentage NUMERIC(5, 2) NOT NULL,
            gross_income NUMERIC(10, 2) NOT NULL,
            rating NUMERIC(3, 1) NOT NULL,
            is_transferred BOOLEAN DEFAULT FALSE
        )
        """
    )

    conn = connect_to_db()
    cur = conn.cursor()
    for command in commands:
        logging.info(f"Executing command: {command}")
        cur.execute(command)
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Tables created successfully.")

# Функция для загрузки данных в базу данных
def load_data_to_db(df):
    conn = connect_to_db()
    cur = conn.cursor()

    logging.info(f"Loading data to the database. Data shape: {df.shape}")
    
    # Уникальные значения для вставки в соответствующие таблицы
    branch_unique = df[['branch']].drop_duplicates()
    city_unique = df[['city']].drop_duplicates()
    customer_unique = df[['customer_type', 'gender']].drop_duplicates()
    product_line_unique = df[['product_line']].drop_duplicates()
    payment_unique = df[['payment']].drop_duplicates()

    # Вставка уникальных значений в таблицы Branch, City, Customer, ProductLine, Payment
    for branch in branch_unique['branch']:
        cur.execute("""
            INSERT INTO Branch (branch_name) VALUES (%s) 
            ON CONFLICT (branch_name) DO NOTHING
        """, (branch,))
    for city in city_unique['city']:
        cur.execute("""
            INSERT INTO City (city_name) VALUES (%s) 
            ON CONFLICT (city_name) DO NOTHING
        """, (city,))
    for _, row in customer_unique.iterrows():
        cur.execute("""
            INSERT INTO Customer (customer_type, gender) VALUES (%s, %s) 
            ON CONFLICT DO NOTHING
        """, (row['customer_type'], row['gender']))
    for product_line in product_line_unique['product_line']:
        cur.execute("""
            INSERT INTO ProductLine (product_line_name) VALUES (%s) 
            ON CONFLICT (product_line_name) DO NOTHING
        """, (product_line,))
    for payment in payment_unique['payment']:
        cur.execute("""
            INSERT INTO Payment (payment_type) VALUES (%s) 
            ON CONFLICT (payment_type) DO NOTHING
        """, (payment,))

    conn.commit()
    logging.info("Inserted unique data into Branch, City, Customer, ProductLine, Payment tables.")
    
    # Вставка данных в таблицу Sales
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO Sales (
                invoice_id, branch_id, city_id, customer_id, product_line_id, unit_price, quantity, 
                tax_5_percent, total, date, time, payment_id, cost_of_goods_sold, 
                gross_margin_percentage, gross_income, rating, is_transferred
            )
            VALUES (
                %s,
                (SELECT branch_id FROM Branch WHERE branch_name = %s LIMIT 1),
                (SELECT city_id FROM City WHERE city_name = %s LIMIT 1),
                (SELECT customer_id FROM Customer WHERE customer_type = %s AND gender = %s LIMIT 1),
                (SELECT product_line_id FROM ProductLine WHERE product_line_name = %s LIMIT 1),
                %s, %s, %s, %s, %s, %s, 
                (SELECT payment_id FROM Payment WHERE payment_type = %s LIMIT 1),
                %s, %s, %s, %s, FALSE
            )
            ON CONFLICT (invoice_id) DO NOTHING
        """, (
            row['invoice_id'], row['branch'], row['city'], row['customer_type'], row['gender'], row['product_line'],
            row['unit_price'], row['quantity'], row['tax_5_percent'], row['total'], row['date'], row['time'],
            row['payment'], row['cost_of_goods_sold'], row['gross_margin_percentage'], row['gross_income'], row['rating']
        ))

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Data committed to Sales table.")

# Функция для обработки данных из CSV файла
def process_data(file_path):
    logging.info(f"Processing file: {file_path}")
    df = pd.read_csv(file_path)
    logging.info(f"Initial dataframe shape: {df.shape}")
    
    # Удаление пустых и дублированных строк
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    logging.info(f"Cleaned dataframe shape: {df.shape}")
    
    # Удаление BOM-символов и переименование столбцов
    df.columns = df.columns.str.replace('ï»¿', '')  # Удалить BOM-символы
    df.rename(columns={
        'Invoice ID': 'invoice_id',
        'Branch': 'branch',
        'City': 'city',
        'Customer type': 'customer_type',
        'Gender': 'gender',
        'Product line': 'product_line',
        'Unit price': 'unit_price',
        'Quantity': 'quantity',
        'Tax 5%': 'tax_5_percent',
        'Total': 'total',
        'Date': 'date',
        'Time': 'time',
        'Payment': 'payment',
        'cogs': 'cost_of_goods_sold',
        'gross margin percentage': 'gross_margin_percentage',
        'gross income': 'gross_income',
        'Rating': 'rating'
    }, inplace=True)
    df['date'] = pd.to_datetime(df['date'])
    df['time'] = pd.to_datetime(df['time']).dt.time

    logging.info(f"Processed dataframe columns: {df.columns}")
    return df

# Основная функция выполнения процесса
def main(**kwargs):
    dag_id = kwargs['dag'].dag_id
    task_id = kwargs['task'].task_id
    load_id = start_load(dag_id, task_id)
    kwargs['load_id'] = load_id
    conn = connect_to_db('postgres_dds')

    try:
        log_load_status(conn, load_id, 'processing', 'Starting main function')
        logging.info("Starting main function")
        input_files = [f for f in os.listdir(input_path) if f.endswith('.csv')]
        logging.info(f"Input files: {input_files}")
        create_tables()
        row_count = 0
        for file in input_files:
            full_file_path = os.path.join(input_path, file)
            df = process_data(full_file_path)
            load_data_to_db(df)
            row_count += len(df)

            base_name = os.path.basename(full_file_path)
            new_name = f"{os.path.splitext(base_name)[0]}_tar.csv"
            os.rename(full_file_path, os.path.join(tar_path, new_name))
            logging.info(f"Moved file from {full_file_path} to {os.path.join(tar_path, new_name)}")
            
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

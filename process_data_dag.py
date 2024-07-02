
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

input_path = "/home/petr0vsk/WorkSQL/Diplom/input"
tar_path = "/home/petr0vsk/WorkSQL/Diplom/tar"

def connect_to_db():
    logging.info("Connecting to the database...")
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    logging.info("Connection established.")
    return conn

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
            rating NUMERIC(3, 1) NOT NULL
        )
        """
    )

    conn = connect_to_db()
    cur = conn.cursor()
    for command in commands:
        cur.execute(command)
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Tables created successfully.")

def load_data_to_db(df):
    conn = connect_to_db()
    cur = conn.cursor()

    logging.info(f"Loading data to the database. Data shape: {df.shape}")
    
    branch_unique = df[['branch']].drop_duplicates()
    city_unique = df[['city']].drop_duplicates()
    customer_unique = df[['customer_type', 'gender']].drop_duplicates()
    product_line_unique = df[['product_line']].drop_duplicates()
    payment_unique = df[['payment']].drop_duplicates()

    for branch in branch_unique['branch']:
        cur.execute("INSERT INTO Branch (branch_name) VALUES (%s) ON CONFLICT (branch_name) DO NOTHING", (branch,))
    for city in city_unique['city']:
        cur.execute("INSERT INTO City (city_name) VALUES (%s) ON CONFLICT (city_name) DO NOTHING", (city,))
    for _, row in customer_unique.iterrows():
        cur.execute("INSERT INTO Customer (customer_type, gender) VALUES (%s, %s) ON CONFLICT DO NOTHING", (row['customer_type'], row['gender']))
    for product_line in product_line_unique['product_line']:
        cur.execute("INSERT INTO ProductLine (product_line_name) VALUES (%s) ON CONFLICT (product_line_name) DO NOTHING", (product_line,))
    for payment in payment_unique['payment']:
        cur.execute("INSERT INTO Payment (payment_type) VALUES (%s) ON CONFLICT (payment_type) DO NOTHING", (payment,))

    conn.commit()
    logging.info("Inserted unique data into Branch, City, Customer, ProductLine, Payment tables.")

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO Sales (invoice_id, branch_id, city_id, customer_id, product_line_id, unit_price, quantity, tax_5_percent, total, date, time, payment_id, cost_of_goods_sold, gross_margin_percentage, gross_income, rating)
            VALUES (
                %s,
                (SELECT branch_id FROM Branch WHERE branch_name = %s LIMIT 1),
                (SELECT city_id FROM City WHERE city_name = %s LIMIT 1),
                (SELECT customer_id FROM Customer WHERE customer_type = %s AND gender = %s LIMIT 1),
                (SELECT product_line_id FROM ProductLine WHERE product_line_name = %s LIMIT 1),
                %s, %s, %s, %s, %s, %s, 
                (SELECT payment_id FROM Payment WHERE payment_type = %s LIMIT 1),
                %s, %s, %s, %s
            )
            ON CONFLICT (invoice_id) DO NOTHING
        """, (
            row['invoice_id'], row['branch'], row['city'], row['customer_type'], row['gender'], row['product_line'],
            row['unit_price'], row['quantity'], row['tax_5_percent'], row['total'], row['date'], row['time'],
            row['payment'],
            row['cost_of_goods_sold'], row['gross_margin_percentage'], row['gross_income'], row['rating']
        ))

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Data committed to Sales table.")

def process_data(file_path):
    logging.info(f"Processing file: {file_path}")
    df = pd.read_csv(file_path)
    logging.info(f"Initial dataframe shape: {df.shape}")

    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    logging.info(f"Cleaned dataframe shape: {df.shape}")

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

def main(**kwargs):
    logging.info("Starting main function")
    input_files = [f for f in os.listdir(input_path) if f.endswith('.csv')]
    logging.info(f"Input files: {input_files}")
    create_tables()
    for file in input_files:
        full_file_path = os.path.join(input_path, file)
        df = process_data(full_file_path)
        load_data_to_db(df)

        base_name = os.path.basename(full_file_path)
        new_name = f"{os.path.splitext(base_name)[0]}_tar.csv"
        os.rename(full_file_path, os.path.join(tar_path, new_name))
        logging.info(f"Moved file from {full_file_path} to {os.path.join(tar_path, new_name)}")

default_args = {
    'owner': 'airflow', 
    'start_date': days_ago(1),
    'email': ['petr0vskjy.aleksander@gmail.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'process_data_dag',
    default_args=default_args,
    description='Process data and load to PostgreSQL',
    schedule_interval='@daily',
)

wait_for_fetch_data = ExternalTaskSensor(
    task_id='wait_for_fetch_data',
    external_dag_id='fetch_data_dag',
    external_task_id='fetch_data',
    allowed_states=['success'],
    failed_states=['failed'],
    mode='poke',
    poke_interval=30,
    timeout=300,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=main,
    dag=dag,
)

wait_for_fetch_data >> process_data_task

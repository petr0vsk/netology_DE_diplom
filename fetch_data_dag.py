import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.utils.email import send_email
import logging

API_URL = 'https://api.mockaroo.com/api/07cd64d0'
API_KEY = 'ff0eaa50'
NUM_RECORDS = 1000
CSV_FILE_PATH = '/home/petr0vsk/WorkSQL/Diplom/input/fake_data.csv'

def fetch_data(**context):
    logging.info("Starting fetch_data task")
    try:
        response = requests.get(f"{API_URL}?count={NUM_RECORDS}&key={API_KEY}")
        response.raise_for_status()
        with open(CSV_FILE_PATH, 'wb') as file:  # Use 'wb' to write bytes
            file.write(response.content)
        logging.info("Data fetched and saved successfully")
    except Exception as e:
        send_email(
            to='admin@example.com',
            subject='Fetch Data DAG Failed',
            html_content=f'Error: {str(e)}'
        )
        logging.error(f"Error occurred: {str(e)}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_data_dag',
    default_args=default_args,
    description='Fetch data from Mockaroo and save to /mnt/input',
    schedule_interval='@daily',
)

fetch_data_task = PythonOperator(
    task_id='fetch_data',
    provide_context=True,
    python_callable=fetch_data,
    dag=dag,
)

fetch_data_task

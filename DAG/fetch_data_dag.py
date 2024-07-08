import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from airflow.utils.email import send_email
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

# URL API для получения данных
API_URL = 'https://api.mockaroo.com/api/07cd64d0'
# API ключ для доступа к Mockaroo
API_KEY = 'ff0eaa50'
# Количество записей для получения
NUM_RECORDS = 1000
# Путь к файлу для сохранения данных
CSV_FILE_PATH = '/home/petr0vsk/WorkSQL/Diplom/input/fake_data.csv'

# Функция для подключения к базе данных
def connect_to_db(conn_id='postgres_dds'):
    # Создание объекта PostgresHook с указанным соединением
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    # Получение подключения к базе данных
    return pg_hook.get_conn()

# Функция для логирования статуса загрузки
def log_load_status(conn, load_id, status, message=None):
    cur = conn.cursor()
    # Вставка записи о статусе загрузки в таблицу load_status
    cur.execute("""
        INSERT INTO load_status (load_id, status_time, status, message)
        VALUES (%s, %s, %s, %s)
    """, (load_id, datetime.now(), status, message))
    conn.commit()
    cur.close()

# Функция для начала загрузки
def start_load(dag_id, task_id):
    # Подключение к базе данных и открытие курсора
    conn = connect_to_db()
    cur = conn.cursor()
    # Вставка записи о начале загрузки в таблицу load_history и получение идентификатора загрузки
    cur.execute("""
        INSERT INTO load_history (dag_id, task_id, start_time, status)
        VALUES (%s, %s, %s, %s) RETURNING load_id
    """, (dag_id, task_id, datetime.now(), 'in_progress'))
    load_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    # Возврат соединения и идентификатора загрузки
    return conn, load_id

# Функция для завершения загрузки
def end_load(conn, load_id, status, row_count=None, error_message=None):
    cur = conn.cursor()
    # Обновление записи о загрузке в таблице load_history
    cur.execute("""
        UPDATE load_history
        SET end_time = %s, status = %s, row_count = %s, error_message = %s
        WHERE load_id = %s
    """, (datetime.now(), status, row_count, error_message, load_id))
    conn.commit()
    cur.close()
    conn.close()

# Основная функция для получения данных из API и сохранения их в файл
def fetch_data(**context):
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    # Начало загрузки и получение идентификатора загрузки
    conn, load_id = start_load(dag_id, task_id)

    try:
        log_load_status(conn, load_id, 'fetching', 'Starting fetch_data task')
        logging.info("Starting fetch_data task")
        
        # Запрос данных из API
        response = requests.get(f"{API_URL}?count={NUM_RECORDS}&key={API_KEY}")
        response.raise_for_status()
        
        # Сохранение полученных данных в файл
        with open(CSV_FILE_PATH, 'wb') as file:
            file.write(response.content)
        
        log_load_status(conn, load_id, 'completed', 'Data fetched and saved successfully')
        logging.info("Data fetched and saved successfully")
        # Завершение загрузки
        end_load(conn, load_id, 'success', row_count=NUM_RECORDS)
    except Exception as e:
        log_load_status(conn, load_id, 'failed', str(e))
        end_load(conn, load_id, 'failed', error_message=str(e))
        # Отправка email при ошибке
        send_email(
            to='admin@example.com',
            subject='Fetch Data DAG Failed',
            html_content=f'Error: {str(e)}'
        )
        logging.error(f"Error occurred: {str(e)}")
        raise

# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
dag = DAG(
    'fetch_data_dag',
    default_args=default_args,
    description='Fetch data from Mockaroo and save to /mnt/input',
    schedule_interval=None,
)

# Определение задачи для получения данных
fetch_data_task = PythonOperator(
    task_id='fetch_data',
    provide_context=True,
    python_callable=fetch_data,
    dag=dag,
)

fetch_data_task

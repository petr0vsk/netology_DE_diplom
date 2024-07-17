from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email': ['petr0vskjy.aleksander@gmail.com'],
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL DAG to trigger other DAGs sequentially',
    schedule_interval='0 2 * * *',  # Запуск каждый день в 2:00 ночи
    catchup=False,
)

# Запуск fetch_data_dag
trigger_fetch_data_dag = TriggerDagRunOperator(
    task_id='trigger_fetch_data_dag',
    trigger_dag_id='fetch_data_dag',
    dag=dag,
)

# Ожидание завершения fetch_data_dag
wait_for_fetch_data_dag = ExternalTaskSensor(
    task_id='wait_for_fetch_data_dag',
    external_dag_id='fetch_data_dag',
    external_task_id=None,
    mode='reschedule',
    timeout=60*60,  # ждем до 1 часа
    dag=dag,
)

# Запуск process_data_dag
trigger_process_data_dag = TriggerDagRunOperator(
    task_id='trigger_process_data_dag',
    trigger_dag_id='process_data_dag',
    dag=dag,
)

# Ожидание завершения process_data_dag
wait_for_process_data_dag = ExternalTaskSensor(
    task_id='wait_for_process_data_dag',
    external_dag_id='process_data_dag',
    external_task_id=None,
    mode='reschedule',
    timeout=60*60,  # ждем до 1 часа
    dag=dag,
)

# Запуск transfer_data_dag
trigger_transfer_data_dag = TriggerDagRunOperator(
    task_id='trigger_transfer_data_dag',
    trigger_dag_id='transfer_data_dag',
    dag=dag,
)

# Ожидание завершения transfer_data_dag
wait_for_transfer_data_dag = ExternalTaskSensor(
    task_id='wait_for_transfer_data_dag',
    external_dag_id='transfer_data_dag',
    external_task_id=None,
    mode='reschedule',
    timeout=60*60,  # ждем до 1 часа
    dag=dag,
)

# Уведомление об ошибке
error_notification = EmailOperator(
    task_id='error_notification',
    to='petr0vskjy.aleksander@gmail.com',
    subject='ETL DAG Failed',
    html_content='One of the DAGs in the ETL process failed after 3 attempts.',
    trigger_rule='one_failed',  # Выполняется, если хотя бы одна из предыдущих задач завершилась с ошибкой
    dag=dag,
)

# Определение порядка выполнения задач
trigger_fetch_data_dag >> wait_for_fetch_data_dag >> trigger_process_data_dag >> wait_for_process_data_dag >> trigger_transfer_data_dag >> wait_for_transfer_data_dag

# Добавляем уведомление об ошибке ко всем сенсорам
wait_for_fetch_data_dag >> error_notification
wait_for_process_data_dag >> error_notification
wait_for_transfer_data_dag >> error_notification

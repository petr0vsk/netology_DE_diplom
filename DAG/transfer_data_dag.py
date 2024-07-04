from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, date, time

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 4),
    'retries': 3,
}

dag = DAG(
    'transfer_data_dag',
    default_args=default_args,
    description='Transfer data from NDS to DDS',
    schedule_interval=None,
)

def extract_updated_data(**kwargs):
    nds_hook = PostgresHook(postgres_conn_id='nds_postgres')
    nds_conn = nds_hook.get_conn()
    nds_cursor = nds_conn.cursor()
    
    nds_cursor.execute("""
        SELECT * FROM Sales WHERE is_transferred = FALSE
    """)
    sales = nds_cursor.fetchall()
    
    print(f"Extracted {len(sales)} rows from NDS.")
    
    serialized_sales = []
    for sale in sales:
        serialized_sale = tuple(
            str(item) if isinstance(item, (datetime, date, time)) else item
            for item in sale
        )
        serialized_sales.append(serialized_sale)
    
    kwargs['ti'].xcom_push(key='updated_sales', value=serialized_sales)
    nds_cursor.close()

def load_dim_branch():
    nds_hook = PostgresHook(postgres_conn_id='nds_postgres')
    dds_hook = PostgresHook(postgres_conn_id='dds_postgres')
    
    nds_conn = nds_hook.get_conn()
    nds_cursor = nds_conn.cursor()
    nds_cursor.execute("SELECT DISTINCT branch_id, branch_name FROM Branch")
    branches = nds_cursor.fetchall()
    
    dds_conn = dds_hook.get_conn()
    dds_cursor = dds_conn.cursor()
    insert_query = """
        INSERT INTO dim_branch (branch_id, branch_name)
        VALUES (%s, %s)
        ON CONFLICT (branch_id) DO NOTHING
    """
    dds_cursor.executemany(insert_query, branches)
    dds_conn.commit()
    
    nds_cursor.close()
    dds_cursor.close()

def load_dim_city():
    nds_hook = PostgresHook(postgres_conn_id='nds_postgres')
    dds_hook = PostgresHook(postgres_conn_id='dds_postgres')
    
    nds_conn = nds_hook.get_conn()
    nds_cursor = nds_conn.cursor()
    nds_cursor.execute("SELECT DISTINCT city_id, city_name FROM City")
    cities = nds_cursor.fetchall()
    
    dds_conn = dds_hook.get_conn()
    dds_cursor = dds_conn.cursor()
    insert_query = """
        INSERT INTO dim_city (city_id, city_name)
        VALUES (%s, %s)
        ON CONFLICT (city_id) DO NOTHING
    """
    dds_cursor.executemany(insert_query, cities)
    dds_conn.commit()
    
    nds_cursor.close()
    dds_cursor.close()

def load_dim_customer():
    nds_hook = PostgresHook(postgres_conn_id='nds_postgres')
    dds_hook = PostgresHook(postgres_conn_id='dds_postgres')
    
    nds_conn = nds_hook.get_conn()
    nds_cursor = nds_conn.cursor()
    nds_cursor.execute("SELECT DISTINCT customer_id, customer_type, gender FROM Customer")
    customers = nds_cursor.fetchall()
    
    dds_conn = dds_hook.get_conn()
    dds_cursor = dds_conn.cursor()
    insert_query = """
        INSERT INTO dim_customer (customer_id, customer_type, gender)
        VALUES (%s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING
    """
    dds_cursor.executemany(insert_query, customers)
    dds_conn.commit()
    
    nds_cursor.close()
    dds_cursor.close()

def load_dim_product_line():
    nds_hook = PostgresHook(postgres_conn_id='nds_postgres')
    dds_hook = PostgresHook(postgres_conn_id='dds_postgres')
    
    nds_conn = nds_hook.get_conn()
    nds_cursor = nds_conn.cursor()
    nds_cursor.execute("SELECT DISTINCT product_line_id, product_line_name FROM ProductLine")
    product_lines = nds_cursor.fetchall()
    
    dds_conn = dds_hook.get_conn()
    dds_cursor = dds_conn.cursor()
    insert_query = """
        INSERT INTO dim_product_line (product_line_id, product_line_name)
        VALUES (%s, %s)
        ON CONFLICT (product_line_id) DO NOTHING
    """
    dds_cursor.executemany(insert_query, product_lines)
    dds_conn.commit()
    
    nds_cursor.close()
    dds_cursor.close()

def load_dim_payment():
    nds_hook = PostgresHook(postgres_conn_id='nds_postgres')
    dds_hook = PostgresHook(postgres_conn_id='dds_postgres')
    
    nds_conn = nds_hook.get_conn()
    nds_cursor = nds_conn.cursor()
    nds_cursor.execute("SELECT DISTINCT payment_id, payment_type FROM Payment")
    payments = nds_cursor.fetchall()
    
    dds_conn = dds_hook.get_conn()
    dds_cursor = dds_conn.cursor()
    insert_query = """
        INSERT INTO dim_payment (payment_id, payment_type)
        VALUES (%s, %s)
        ON CONFLICT (payment_id) DO NOTHING
    """
    dds_cursor.executemany(insert_query, payments)
    dds_conn.commit()
    
    nds_cursor.close()
    dds_cursor.close()

def load_dim_date():
    nds_hook = PostgresHook(postgres_conn_id='nds_postgres')
    dds_hook = PostgresHook(postgres_conn_id='dds_postgres')
    
    nds_conn = nds_hook.get_conn()
    nds_cursor = nds_conn.cursor()
    nds_cursor.execute("SELECT DISTINCT date FROM Sales")
    dates = nds_cursor.fetchall()
    
    dds_conn = dds_hook.get_conn()
    dds_cursor = dds_conn.cursor()
    
    for date in dates:
        year = date[0].year
        quarter = (date[0].month - 1) // 3 + 1
        month = date[0].month
        day = date[0].day
        day_of_week = date[0].strftime('%A')
        
        insert_query = """
            INSERT INTO dim_date (date, year, quarter, month, day, day_of_week)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING
        """
        dds_cursor.execute(insert_query, (date[0], year, quarter, month, day, day_of_week))
    
    dds_conn.commit()
    nds_cursor.close()
    dds_cursor.close()

def load_dim_time():
    nds_hook = PostgresHook(postgres_conn_id='nds_postgres')
    dds_hook = PostgresHook(postgres_conn_id='dds_postgres')
    
    nds_conn = nds_hook.get_conn()
    nds_cursor = nds_conn.cursor()
    nds_cursor.execute("SELECT DISTINCT time FROM Sales")
    times = nds_cursor.fetchall()
    
    dds_conn = dds_hook.get_conn()
    dds_cursor = dds_conn.cursor()
    
    for time in times:
        hour = time[0].hour
        minute = time[0].minute
        second = time[0].second
        
        insert_query = """
            INSERT INTO dim_time (time, hour, minute, second)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (time) DO NOTHING
        """
        dds_cursor.execute(insert_query, (time[0], hour, minute, second))
    
    dds_conn.commit()
    nds_cursor.close()
    dds_cursor.close()

def load_fact_sales(**kwargs):
    sales = kwargs['ti'].xcom_pull(task_ids='extract_data', key='updated_sales')
    
    if not sales:
        print("No sales data to load.")
        return
    
    print(f"Sales data to load: {sales[:5]}")  # Печать первых 5 записей для отладки

    dds_hook = PostgresHook(postgres_conn_id='dds_postgres')
    dds_conn = dds_hook.get_conn()
    dds_cursor = dds_conn.cursor()

    # Получение идентификаторов для дат и времени
    date_map = {}
    time_map = {}

    dds_cursor.execute("SELECT date, date_id FROM dim_date")
    for row in dds_cursor.fetchall():
        date_map[row[0].isoformat()] = row[1]
    
    dds_cursor.execute("SELECT time, time_id FROM dim_time")
    for row in dds_cursor.fetchall():
        time_map[row[0].isoformat()] = row[1]

    insert_query = """
        INSERT INTO fact_sales (
            sale_id, invoice_id, branch_id, city_id, customer_id, product_line_id, unit_price,
            quantity, tax_5_percent, total, date_id, time_id, payment_id, cost_of_goods_sold,
            gross_margin_percentage, gross_income, rating
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Преобразование данных в нужный формат для executemany
    formatted_sales = []
    for sale in sales:
        date_str = sale[10]
        time_str = sale[11]
        date_id = date_map.get(date_str)
        time_id = time_map.get(time_str)
        
        if date_id is None or time_id is None:
            print(f"Skipping sale {sale[0]} due to missing date or time mapping.")
            continue
        
        formatted_sales.append((
            sale[0], sale[1], sale[2], sale[3], sale[4], sale[5], sale[6], sale[7],
            sale[8], sale[9], date_id, time_id, sale[12], sale[13], sale[14],
            sale[15], sale[16]
        ))

    try:
        dds_cursor.executemany(insert_query, formatted_sales)
        dds_conn.commit()
        print(f"Successfully loaded {len(formatted_sales)} sales records into fact_sales.")
    except Exception as e:
        print(f"Error loading sales data: {e}")
        raise
    finally:
        dds_cursor.close()

    # Обновление флага is_transferred в NDS
    nds_hook = PostgresHook(postgres_conn_id='nds_postgres')
    nds_conn = nds_hook.get_conn()
    nds_cursor = nds_conn.cursor()
    
    update_query = """
        UPDATE Sales
        SET is_transferred = TRUE
        WHERE sale_id = %s
    """
    
    sale_ids = [sale[0] for sale in sales]
    
    try:
        nds_cursor.executemany(update_query, [(sale_id,) for sale_id in sale_ids])
        nds_conn.commit()
        print(f"Successfully updated is_transferred flag for {len(sale_ids)} records in NDS.")
    except Exception as e:
        print(f"Error updating is_transferred flag in NDS: {e}")
        raise
    finally:
        nds_cursor.close()



load_dim_branch_task = PythonOperator(
    task_id='load_dim_branch',
    python_callable=load_dim_branch,
    dag=dag,
)

load_dim_city_task = PythonOperator(
    task_id='load_dim_city',
    python_callable=load_dim_city,
    dag=dag,
)

load_dim_customer_task = PythonOperator(
    task_id='load_dim_customer',
    python_callable=load_dim_customer,
    dag=dag,
)

load_dim_product_line_task = PythonOperator(
    task_id='load_dim_product_line',
    python_callable=load_dim_product_line,
    dag=dag,
)

load_dim_payment_task = PythonOperator(
    task_id='load_dim_payment',
    python_callable=load_dim_payment,
    dag=dag,
)

load_dim_date_task = PythonOperator(
    task_id='load_dim_date',
    python_callable=load_dim_date,
    dag=dag,
)

load_dim_time_task = PythonOperator(
    task_id='load_dim_time',
    python_callable=load_dim_time,
    dag=dag,
)

# Задача для загрузки данных в таблицу фактов
load_fact_sales_task = PythonOperator(
    task_id='load_fact_sales',
    python_callable=load_fact_sales,
    provide_context=True,
    dag=dag,
)

# Задача для извлечения данных из NDS
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_updated_data,
    provide_context=True,
    dag=dag,
)

# Последовательность выполнения задач
extract_data_task >> [
    load_dim_branch_task,
    load_dim_city_task,
    load_dim_customer_task,
    load_dim_product_line_task,
    load_dim_payment_task,
    load_dim_date_task,
    load_dim_time_task
] >> load_fact_sales_task

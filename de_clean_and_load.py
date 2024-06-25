import pandas as pd
import psycopg2
import os
import glob
from dotenv import load_dotenv

# Загрузка переменных окружения из файла .env
load_dotenv()

# Параметры подключения к базе данных PostgreSQL из переменных окружения
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

# Функция для подключения к базе данных PostgreSQL
def connect_to_db():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn

# Остальная часть кода без изменений...

# Функция для создания таблиц
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

    conn = None
    try:
        conn = connect_to_db()
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

# Функция для загрузки данных в таблицы
def load_data_to_db(df):
    conn = connect_to_db()
    cur = conn.cursor()

    # Вставка данных в таблицы
    for _, row in df.iterrows():
        # Вставка данных в таблицу Branch
        cur.execute("INSERT INTO Branch (branch_name) VALUES (%s) ON CONFLICT (branch_name) DO NOTHING", (row['branch'],))
        
        # Вставка данных в таблицу City
        cur.execute("INSERT INTO City (city_name) VALUES (%s) ON CONFLICT (city_name) DO NOTHING", (row['city'],))
        
        # Вставка данных в таблицу Customer
        cur.execute("INSERT INTO Customer (customer_type, gender) VALUES (%s, %s) ON CONFLICT DO NOTHING", (row['customer_type'], row['gender']))
        
        # Вставка данных в таблицу ProductLine
        cur.execute("INSERT INTO ProductLine (product_line_name) VALUES (%s) ON CONFLICT (product_line_name) DO NOTHING", (row['product_line'],))
        
        # Вставка данных в таблицу Payment
        cur.execute("INSERT INTO Payment (payment_type) VALUES (%s) ON CONFLICT (payment_type) DO NOTHING", (row['payment'],))
        
        # Вставка данных в таблицу Sales
        cur.execute("""
            INSERT INTO Sales (invoice_id, branch_id, city_id, customer_id, product_line_id, unit_price, quantity, tax_5_percent, total, date, time, payment_id, cost_of_goods_sold, gross_margin_percentage, gross_income, rating)
            VALUES (
                %s,
                (SELECT branch_id FROM Branch WHERE branch_name = %s),
                (SELECT city_id FROM City WHERE city_name = %s),
                (SELECT customer_id FROM Customer WHERE customer_type = %s AND gender = %s),
                (SELECT product_line_id FROM ProductLine WHERE product_line_name = %s),
                %s, %s, %s, %s, %s, %s, 
                (SELECT payment_id FROM Payment WHERE payment_type = %s),
                %s, %s, %s, %s
            )
        """, (
            row['invoice_id'], row['branch'], row['city'], row['customer_type'], row['gender'], row['product_line'],
            row['unit_price'], row['quantity'], row['tax_5_percent'], row['total'], row['date'], row['time'], row['payment'],
            row['cost_of_goods_sold'], row['gross_margin_percentage'], row['gross_income'], row['rating']
        ))

    conn.commit()
    cur.close()
    conn.close()

# Функция для обработки данных
def process_data(file_path):
    df = pd.read_csv(file_path)
    
    # Первичная очистка данных
    df.dropna(inplace=True)  # Удаление строк с пропущенными значениями
    df.drop_duplicates(inplace=True)  # Удаление дубликатов
    
    # Преобразование столбцов и типов данных
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
    
    return df

# Основная функция
def main():
    input_path = '/mnt/input/'
    tar_path = '/mnt/tar/'
    
    # Создание таблиц
    create_tables()
    
    # Обработка и загрузка данных
    for file in glob.glob(os.path.join(input_path, '*.csv')):
        df = process_data(file)
        load_data_to_db(df)
        
        # Перемещение обработанного файла
        base_name = os.path.basename(file)
        new_name = f"{os.path.splitext(base_name)[0]}_tar.csv"
        os.rename(file, os.path.join(tar_path, new_name))

if __name__ == '__main__':
    main()

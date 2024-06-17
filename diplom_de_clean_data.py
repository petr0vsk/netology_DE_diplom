
# Подготовка данных к загрузке в PostgreSQL

import pandas as pd
from sqlalchemy import create_engine

# Загрузка данных
file_path = '/mnt/data/supermarket_sales - Sheet1.csv'
data = pd.read_csv(file_path)

# Предварительная очистка данных

# Проверка и удаление пропущенных значений
data.dropna(inplace=True)

# Преобразование столбца 'Date' в формат datetime
data['Date'] = pd.to_datetime(data['Date'], format='%m/%d/%Y')

# Преобразование столбца 'Time' в формат времени
data['Time'] = pd.to_datetime(data['Time'], format='%H:%M').dt.time
# Переименование столбцов для соответствия стандартам базы данных
data.rename(columns={
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

# Удаление дубликатов, если такие есть
data.drop_duplicates(inplace=True)

# Подключение к базе данных PostgreSQL
engine = create_engine('postgresql://username:password@host:port/database')

# Загрузка данных в таблицу 'supermarket_sales'
data.to_sql('supermarket_sales', engine, if_exists='replace', index=False)

# Проверка данных после загрузки (опционально)
# loaded_data = pd.read_sql('SELECT * FROM supermarket_sales', engine)
# print(loaded_data.head())

import ace_tools as tools; tools.display_dataframe_to_user(name="Cleaned Supermarket Sales Data", dataframe=data)

# Сохранение очищенных данных в новый CSV файл
data.to_csv('/mnt/data/supermarket_sales_cleaned.csv', index=False)


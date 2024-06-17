### Код на Python для загрузки данных в PostgreSQL

```python
import pandas as pd
from sqlalchemy import create_engine

# Загрузка очищенных данных из CSV файла
file_path = '/mnt/data/supermarket_sales_cleaned.csv'
data = pd.read_csv(file_path)

# Подключение к базе данных PostgreSQL
engine = create_engine('postgresql://username:password@host:port/database')

# Вставка уникальных значений в таблицы Branch, City, Customer, ProductLine, Payment
data[['branch']].drop_duplicates().to_sql('Branch', engine, if_exists='append', index=False)
data[['city']].drop_duplicates().to_sql('City', engine, if_exists='append', index=False)
data[['customer_type', 'gender']].drop_duplicates().to_sql('Customer', engine, if_exists='append', index=False)
data[['product_line']].drop_duplicates().to_sql('ProductLine', engine, if_exists='append', index=False)
data[['payment']].drop_duplicates().to_sql('Payment', engine, if_exists='append', index=False)

# Получение ID для вставки в таблицу Sales
branch_df = pd.read_sql('SELECT branch_id, branch_name FROM Branch', engine)
city_df = pd.read_sql('SELECT city_id, city_name FROM City', engine)
customer_df = pd.read_sql('SELECT customer_id, customer_type, gender FROM Customer', engine)
product_line_df = pd.read_sql('SELECT product_line_id, product_line_name FROM ProductLine', engine)
payment_df = pd.read_sql('SELECT payment_id, payment_type FROM Payment', engine)

# Объединение данных для получения ID
data = data.merge(branch_df, left_on='branch', right_on='branch_name') \
           .merge(city_df, left_on='city', right_on='city_name') \
           .merge(customer_df, on=['customer_type', 'gender']) \
           .merge(product_line_df, left_on='product_line', right_on='product_line_name') \
           .merge(payment_df, left_on='payment', right_on='payment_type')

# Создание финальной таблицы Sales
sales_data = data[['invoice_id', 'branch_id', 'city_id', 'customer_id', 'product_line_id', 'unit_price',
                   'quantity', 'tax_5_percent', 'total', 'date', 'time', 'payment_id', 
                   'cost_of_goods_sold', 'gross_margin_percentage', 'gross_income', 'rating']]

# Вставка данных в таблицу Sales
sales_data.to_sql('Sales', engine, if_exists='append', index=False)
```
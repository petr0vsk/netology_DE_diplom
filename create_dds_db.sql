-- Создание схемы базы данных DDS
-- Создание таблицы измерений филиалов
CREATE TABLE IF NOT EXISTS dim_branch (
    branch_id SERIAL PRIMARY KEY,
    branch_name VARCHAR(10) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы измерений городов
CREATE TABLE IF NOT EXISTS dim_city (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(50) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы измерений клиентов
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_type VARCHAR(10) NOT NULL,
    gender VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы измерений продуктовых линеек
CREATE TABLE IF NOT EXISTS dim_product_line (
    product_line_id SERIAL PRIMARY KEY,
    product_line_name VARCHAR(50) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы измерений способов оплаты
CREATE TABLE IF NOT EXISTS dim_payment (
    payment_id SERIAL PRIMARY KEY,
    payment_type VARCHAR(20) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы измерений дат
CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы измерений времени
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    time TIME UNIQUE NOT NULL,
    hour INT NOT NULL,
    minute INT NOT NULL,
    second INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы фактов продаж
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id SERIAL PRIMARY KEY,
    invoice_id VARCHAR(20) UNIQUE NOT NULL,
    branch_id INT REFERENCES dim_branch(branch_id),
    city_id INT REFERENCES dim_city(city_id),
    customer_id INT REFERENCES dim_customer(customer_id),
    product_line_id INT REFERENCES dim_product_line(product_line_id),
    unit_price NUMERIC(10, 2) NOT NULL,
    quantity INT NOT NULL,
    tax_5_percent NUMERIC(10, 2) NOT NULL,
    total NUMERIC(10, 2) NOT NULL,
    date_id INT REFERENCES dim_date(date_id),
    time_id INT REFERENCES dim_time(time_id),
    payment_id INT REFERENCES dim_payment(payment_id),
    cost_of_goods_sold NUMERIC(10, 2) NOT NULL,
    gross_margin_percentage NUMERIC(5, 2) NOT NULL,
    gross_income NUMERIC(10, 2) NOT NULL,
    rating NUMERIC(3, 1) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

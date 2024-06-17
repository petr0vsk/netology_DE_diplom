-- Схема нормализованных данных (NDS)

-- Для нормализации данных и создания удобной схемы базы данных, можно выделить следующие таблицы:

-- 1. **Branch:** Информация о филиалах
-- 2. **City:** Информация о городах
-- 3. **Customer:** Информация о клиентах
-- 4. **ProductLine:** Информация о линиях продуктов
-- 5. **Payment:** Информация о способах оплаты
-- 6. **Sales:** Информация о продажах (основная таблица, содержащая ссылки на другие таблицы)

--  SQL код для создания таблиц в базе данных PostgreSQL

-- Создание таблицы Branch
CREATE TABLE Branch (
    branch_id SERIAL PRIMARY KEY,
    branch_name VARCHAR(10) UNIQUE NOT NULL
);

-- Создание таблицы City
CREATE TABLE City (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(50) UNIQUE NOT NULL
);

-- Создание таблицы Customer
CREATE TABLE Customer (
    customer_id SERIAL PRIMARY KEY,
    customer_type VARCHAR(10) NOT NULL,
    gender VARCHAR(10) NOT NULL
);

-- Создание таблицы ProductLine
CREATE TABLE ProductLine (
    product_line_id SERIAL PRIMARY KEY,
    product_line_name VARCHAR(50) UNIQUE NOT NULL
);

-- Создание таблицы Payment
CREATE TABLE Payment (
    payment_id SERIAL PRIMARY KEY,
    payment_type VARCHAR(20) UNIQUE NOT NULL
);

-- Создание таблицы Sales
CREATE TABLE Sales (
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
);
```

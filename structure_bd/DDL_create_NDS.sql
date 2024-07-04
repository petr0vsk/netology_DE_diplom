-- Таблица для хранения информации о филиалах
CREATE TABLE IF NOT EXISTS Branch (
    branch_id SERIAL PRIMARY KEY,  -- Уникальный идентификатор филиала
    branch_name VARCHAR(10) UNIQUE NOT NULL  -- Название филиала
);

-- Таблица для хранения информации о городах
CREATE TABLE IF NOT EXISTS City (
    city_id SERIAL PRIMARY KEY,  -- Уникальный идентификатор города
    city_name VARCHAR(50) UNIQUE NOT NULL  -- Название города
);

-- Таблица для хранения информации о клиентах
CREATE TABLE IF NOT EXISTS Customer (
    customer_id SERIAL PRIMARY KEY,  -- Уникальный идентификатор клиента
    customer_type VARCHAR(10) NOT NULL,  -- Тип клиента (например, 'VIP', 'Regular')
    gender VARCHAR(10) NOT NULL  -- Пол клиента
);

-- Таблица для хранения информации о продуктовых линейках
CREATE TABLE IF NOT EXISTS ProductLine (
    product_line_id SERIAL PRIMARY KEY,  -- Уникальный идентификатор продуктовой линейки
    product_line_name VARCHAR(50) UNIQUE NOT NULL  -- Название продуктовой линейки
);

-- Таблица для хранения информации о типах платежей
CREATE TABLE IF NOT EXISTS Payment (
    payment_id SERIAL PRIMARY KEY,  -- Уникальный идентификатор платежа
    payment_type VARCHAR(20) UNIQUE NOT NULL  -- Тип платежа (например, 'Credit Card', 'Cash')
);

-- Таблица для хранения информации о продажах
CREATE TABLE IF NOT EXISTS Sales (
    sale_id SERIAL PRIMARY KEY,  -- Уникальный идентификатор продажи
    invoice_id VARCHAR(20) UNIQUE NOT NULL,  -- Уникальный идентификатор счета-фактуры
    branch_id INT REFERENCES Branch(branch_id),  -- Ссылка на филиал
    city_id INT REFERENCES City(city_id),  -- Ссылка на город
    customer_id INT REFERENCES Customer(customer_id),  -- Ссылка на клиента
    product_line_id INT REFERENCES ProductLine(product_line_id),  -- Ссылка на продуктовую линейку
    unit_price NUMERIC(10, 2) NOT NULL,  -- Цена за единицу товара
    quantity INT NOT NULL,  -- Количество товаров
    tax_5_percent NUMERIC(10, 2) NOT NULL,  -- Налог 5%
    total NUMERIC(10, 2) NOT NULL,  -- Общая стоимость
    date DATE NOT NULL,  -- Дата продажи
    time TIME NOT NULL,  -- Время продажи
    payment_id INT REFERENCES Payment(payment_id),  -- Ссылка на тип платежа
    cost_of_goods_sold NUMERIC(10, 2) NOT NULL,  -- Себестоимость проданных товаров
    gross_margin_percentage NUMERIC(5, 2) NOT NULL,  -- Процент валовой прибыли
    gross_income NUMERIC(10, 2) NOT NULL,  -- Валовой доход
    rating NUMERIC(3, 1) NOT NULL,  -- Рейтинг
    is_transferred BOOLEAN DEFAULT FALSE  -- Флаг, указывающий, была ли передана запись
);

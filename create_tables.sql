-- Redshift Star Schema for E-Commerce Analytics

CREATE SCHEMA IF NOT EXISTS ecommerce_analytics;

CREATE TABLE IF NOT EXISTS ecommerce_analytics.dim_customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(200),
    region VARCHAR(50),
    segment VARCHAR(50),
    signup_date DATE
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS ecommerce_analytics.dim_products (
    product_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(200),
    category VARCHAR(50),
    price DECIMAL(10,2),
    brand VARCHAR(50)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS ecommerce_analytics.dim_dates (
    date_id INTEGER PRIMARY KEY,
    date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    quarter INTEGER,
    day_of_week INTEGER
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS ecommerce_analytics.fact_sales (
    order_id VARCHAR(20),
    customer_id VARCHAR(20),
    product_id VARCHAR(20),
    date_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    discount DECIMAL(5,2) DEFAULT 0,
    status VARCHAR(20),
    order_year INTEGER,
    order_month INTEGER
)
DISTKEY(customer_id)
SORTKEY(order_year, order_month);

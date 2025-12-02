-- ============================================
-- INITIALIZE POSTGRESQL SOURCE DATABASE
-- ============================================

\c source_sales;

DROP SCHEMA IF EXISTS sales CASCADE;
CREATE SCHEMA sales;

-- ============================================
-- MASTER DATA
-- ============================================
CREATE TABLE sales.customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(120),
    city VARCHAR(80),
    country VARCHAR(80),
    created_at DATE DEFAULT CURRENT_DATE
);

INSERT INTO sales.customers (customer_id, first_name, last_name, email, city, country, created_at) VALUES
    (1, 'Alice', 'Nguyen', 'alice.nguyen@example.com', 'Seattle', 'USA', '2024-01-04'),
    (2, 'Bruno', 'Costa', 'bruno.costa@example.com', 'Lisbon', 'Portugal', '2024-02-12'),
    (3, 'Carmen', 'Diaz', 'carmen.diaz@example.com', 'Madrid', 'Spain', '2024-03-08'),
    (4, 'Deepak', 'Iyer', 'deepak.iyer@example.com', 'Bangalore', 'India', '2024-01-22'),
    (5, 'Elena', 'Fischer', 'elena.fischer@example.com', 'Munich', 'Germany', '2024-04-18');

CREATE TABLE sales.products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    unit_price NUMERIC(10,2),
    active BOOLEAN DEFAULT TRUE
);

INSERT INTO sales.products (product_id, product_name, category, unit_price, active) VALUES
    (101, 'Wireless Keyboard', 'Peripherals', 49.99, TRUE),
    (102, 'Ergonomic Mouse', 'Peripherals', 39.99, TRUE),
    (103, '4K Monitor', 'Displays', 329.00, TRUE),
    (104, 'USB-C Docking Station', 'Accessories', 189.00, TRUE),
    (105, 'Noise Cancelling Headphones', 'Audio', 249.00, TRUE);

-- ============================================
-- FACT SOURCE TABLES
-- ============================================
CREATE TABLE sales.orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES sales.customers(customer_id),
    order_date DATE NOT NULL,
    status VARCHAR(20) DEFAULT 'COMPLETED',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sales.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES sales.orders(order_id),
    product_id INTEGER REFERENCES sales.products(product_id),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL
);

INSERT INTO sales.orders (order_id, customer_id, order_date, status) VALUES
    (5001, 1, '2024-11-20', 'COMPLETED'),
    (5002, 1, '2024-11-21', 'COMPLETED'),
    (5003, 2, '2024-11-21', 'COMPLETED'),
    (5004, 3, '2024-11-22', 'COMPLETED'),
    (5005, 4, '2024-11-22', 'COMPLETED'),
    (5006, 5, '2024-11-23', 'COMPLETED');

INSERT INTO sales.order_items (order_id, product_id, quantity, unit_price) VALUES
    (5001, 101, 2, 49.99),
    (5001, 104, 1, 189.00),
    (5002, 105, 1, 249.00),
    (5003, 103, 1, 329.00),
    (5003, 102, 2, 39.99),
    (5004, 101, 1, 49.99),
    (5004, 102, 1, 39.99),
    (5004, 105, 1, 249.00),
    (5005, 104, 2, 189.00),
    (5006, 103, 1, 329.00),
    (5006, 101, 1, 49.99);

CREATE OR REPLACE VIEW sales.daily_sales_mv AS
SELECT 
    o.order_date,
    o.customer_id,
    SUM(oi.quantity) as total_quantity,
    SUM(oi.quantity * oi.unit_price) as total_amount
FROM sales.orders o
JOIN sales.order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_date, o.customer_id;

\echo '============================================'
\echo 'SOURCE DATABASE INITIALIZED'
\echo '============================================'

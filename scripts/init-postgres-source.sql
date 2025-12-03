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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO sales.customers (customer_id, first_name, last_name, email, city, country, created_at, updated_at) VALUES
    (1, 'Alice', 'Nguyen', 'alice.nguyen@example.com', 'Seattle', 'USA', '2024-01-04 10:00:00', '2024-01-04 10:00:00'),
    (2, 'Bruno', 'Costa', 'bruno.costa@example.com', 'Lisbon', 'Portugal', '2024-02-12 11:30:00', '2024-02-12 11:30:00'),
    (3, 'Carmen', 'Diaz', 'carmen.diaz@example.com', 'Madrid', 'Spain', '2024-03-08 09:15:00', '2024-03-08 09:15:00'),
    (4, 'Deepak', 'Iyer', 'deepak.iyer@example.com', 'Bangalore', 'India', '2024-01-22 14:00:00', '2024-01-22 14:00:00'),
    (5, 'Elena', 'Fischer', 'elena.fischer@example.com', 'Munich', 'Germany', '2024-04-18 16:45:00', '2024-04-18 16:45:00');

CREATE TABLE sales.products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    unit_price NUMERIC(10,2),
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO sales.products (product_id, product_name, category, unit_price, active, created_at, updated_at) VALUES
    (101, 'Wireless Keyboard', 'Peripherals', 49.99, TRUE, '2024-01-01 08:00:00', '2024-01-01 08:00:00'),
    (102, 'Ergonomic Mouse', 'Peripherals', 39.99, TRUE, '2024-01-01 08:00:00', '2024-01-01 08:00:00'),
    (103, '4K Monitor', 'Displays', 329.00, TRUE, '2024-01-01 08:00:00', '2024-01-01 08:00:00'),
    (104, 'USB-C Docking Station', 'Accessories', 189.00, TRUE, '2024-01-01 08:00:00', '2024-01-01 08:00:00'),
    (105, 'Noise Cancelling Headphones', 'Audio', 249.00, TRUE, '2024-01-01 08:00:00', '2024-01-01 08:00:00');

-- ============================================
-- FACT SOURCE TABLES
-- ============================================
CREATE TABLE sales.orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES sales.customers(customer_id),
    order_date DATE NOT NULL,
    status VARCHAR(20) DEFAULT 'COMPLETED',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sales.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES sales.orders(order_id),
    product_id INTEGER REFERENCES sales.products(product_id),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO sales.orders (order_id, customer_id, order_date, status, created_at, updated_at) VALUES
    (5001, 1, '2024-11-20', 'COMPLETED', '2024-11-20 10:30:00', '2024-11-20 10:30:00'),
    (5002, 1, '2024-11-21', 'COMPLETED', '2024-11-21 11:00:00', '2024-11-21 11:00:00'),
    (5003, 2, '2024-11-21', 'COMPLETED', '2024-11-21 14:15:00', '2024-11-21 14:15:00'),
    (5004, 3, '2024-11-22', 'COMPLETED', '2024-11-22 09:45:00', '2024-11-22 09:45:00'),
    (5005, 4, '2024-11-22', 'COMPLETED', '2024-11-22 16:30:00', '2024-11-22 16:30:00'),
    (5006, 5, '2024-11-23', 'COMPLETED', '2024-11-23 12:00:00', '2024-11-23 12:00:00');

INSERT INTO sales.order_items (order_id, product_id, quantity, unit_price, created_at, updated_at) VALUES
    (5001, 101, 2, 49.99, '2024-11-20 10:30:00', '2024-11-20 10:30:00'),
    (5001, 104, 1, 189.00, '2024-11-20 10:30:00', '2024-11-20 10:30:00'),
    (5002, 105, 1, 249.00, '2024-11-21 11:00:00', '2024-11-21 11:00:00'),
    (5003, 103, 1, 329.00, '2024-11-21 14:15:00', '2024-11-21 14:15:00'),
    (5003, 102, 2, 39.99, '2024-11-21 14:15:00', '2024-11-21 14:15:00'),
    (5004, 101, 1, 49.99, '2024-11-22 09:45:00', '2024-11-22 09:45:00'),
    (5004, 102, 1, 39.99, '2024-11-22 09:45:00', '2024-11-22 09:45:00'),
    (5004, 105, 1, 249.00, '2024-11-22 09:45:00', '2024-11-22 09:45:00'),
    (5005, 104, 2, 189.00, '2024-11-22 16:30:00', '2024-11-22 16:30:00'),
    (5006, 103, 1, 329.00, '2024-11-23 12:00:00', '2024-11-23 12:00:00'),
    (5006, 101, 1, 49.99, '2024-11-23 12:00:00', '2024-11-23 12:00:00');

-- ============================================
-- TRIGGERS FOR AUTO-UPDATING updated_at
-- ============================================
CREATE OR REPLACE FUNCTION sales.update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON sales.customers
    FOR EACH ROW EXECUTE FUNCTION sales.update_updated_at();

CREATE TRIGGER trg_products_updated_at
    BEFORE UPDATE ON sales.products
    FOR EACH ROW EXECUTE FUNCTION sales.update_updated_at();

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON sales.orders
    FOR EACH ROW EXECUTE FUNCTION sales.update_updated_at();

CREATE TRIGGER trg_order_items_updated_at
    BEFORE UPDATE ON sales.order_items
    FOR EACH ROW EXECUTE FUNCTION sales.update_updated_at();

-- ============================================
-- INDEXES FOR LSET/CET QUERIES
-- ============================================
CREATE INDEX idx_customers_updated_at ON sales.customers(updated_at);
CREATE INDEX idx_products_updated_at ON sales.products(updated_at);
CREATE INDEX idx_orders_updated_at ON sales.orders(updated_at);
CREATE INDEX idx_order_items_updated_at ON sales.order_items(updated_at);

-- ============================================
-- VIEW
-- ============================================
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

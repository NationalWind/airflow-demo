-- ============================================
-- INITIALIZE POSTGRESQL TARGET DATABASE
-- ============================================

-- Connect to analytics_db (will be created by Docker)
\c analytics_db;

-- ============================================
-- SCHEMA: dwh (Data Warehouse)
-- ============================================
CREATE SCHEMA IF NOT EXISTS dwh;

-- ============================================
-- TABLE: dwh.dim_customers (Dimension)
-- ============================================
DROP TABLE IF EXISTS dwh.dim_customers CASCADE;
CREATE TABLE dwh.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),
    country VARCHAR(50),
    created_date DATE,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_customers_id ON dwh.dim_customers(customer_id);

-- ============================================
-- TABLE: dwh.dim_products (Dimension)
-- ============================================
DROP TABLE IF EXISTS dwh.dim_products CASCADE;
CREATE TABLE dwh.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(100),
    category VARCHAR(50),
    unit_price NUMERIC(10,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_products_id ON dwh.dim_products(product_id);

-- ============================================
-- TABLE: dwh.fact_sales (Fact Table)
-- ============================================
DROP TABLE IF EXISTS dwh.fact_sales CASCADE;
CREATE TABLE dwh.fact_sales (
    sale_key SERIAL PRIMARY KEY,
    sale_id INTEGER NOT NULL,
    customer_key INTEGER REFERENCES dwh.dim_customers(customer_key),
    product_key INTEGER REFERENCES dwh.dim_products(product_key),
    sale_date DATE NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    total_amount NUMERIC(10,2) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_sales_date ON dwh.fact_sales(sale_date);
CREATE INDEX idx_fact_sales_customer ON dwh.fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON dwh.fact_sales(product_key);

-- ============================================
-- TABLE: dwh.sales_summary_daily (Aggregated Fact)
-- ============================================
DROP TABLE IF EXISTS dwh.sales_summary_daily CASCADE;
CREATE TABLE dwh.sales_summary_daily (
    summary_date DATE PRIMARY KEY,
    unique_customers INTEGER,
    total_orders INTEGER,
    total_items_sold INTEGER,
    total_revenue NUMERIC(12,2),
    avg_order_value NUMERIC(10,2),
    min_order_value NUMERIC(10,2),
    max_order_value NUMERIC(10,2),
    top_product_id INTEGER,
    top_product_name VARCHAR(100),
    top_product_revenue NUMERIC(12,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- TABLE: etl.etl_log (ETL Monitoring)
-- ============================================
CREATE SCHEMA IF NOT EXISTS etl;

DROP TABLE IF EXISTS etl.etl_log CASCADE;
CREATE TABLE etl.etl_log (
    log_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    task_id VARCHAR(100),
    execution_date DATE,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20),
    rows_extracted INTEGER,
    rows_loaded INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_etl_log_dag_task ON etl.etl_log(dag_id, task_id, execution_date);

-- ============================================
-- VIEW: dwh.vw_sales_analysis
-- ============================================
CREATE OR REPLACE VIEW dwh.vw_sales_analysis AS
SELECT 
    fs.sale_date,
    dc.customer_name,
    dc.city,
    dc.country,
    dp.product_name,
    dp.category,
    fs.quantity,
    fs.unit_price,
    fs.total_amount,
    fs.loaded_at
FROM dwh.fact_sales fs
INNER JOIN dwh.dim_customers dc ON fs.customer_key = dc.customer_key
INNER JOIN dwh.dim_products dp ON fs.product_key = dp.product_key
ORDER BY fs.sale_date DESC;

-- ============================================
-- VIEW: dwh.vw_top_products_by_revenue
-- ============================================
CREATE OR REPLACE VIEW dwh.vw_top_products_by_revenue AS
SELECT 
    dp.product_name,
    dp.category,
    COUNT(*) as total_orders,
    SUM(fs.quantity) as total_quantity_sold,
    SUM(fs.total_amount) as total_revenue,
    AVG(fs.total_amount) as avg_order_value
FROM dwh.fact_sales fs
INNER JOIN dwh.dim_products dp ON fs.product_key = dp.product_key
GROUP BY dp.product_name, dp.category
ORDER BY total_revenue DESC;

-- ============================================
-- VIEW: dwh.vw_customer_lifetime_value
-- ============================================
CREATE OR REPLACE VIEW dwh.vw_customer_lifetime_value AS
SELECT 
    dc.customer_name,
    dc.city,
    dc.country,
    COUNT(DISTINCT fs.sale_date) as purchase_days,
    COUNT(*) as total_orders,
    SUM(fs.quantity) as total_items_purchased,
    SUM(fs.total_amount) as lifetime_value,
    AVG(fs.total_amount) as avg_order_value,
    MAX(fs.sale_date) as last_purchase_date
FROM dwh.fact_sales fs
INNER JOIN dwh.dim_customers dc ON fs.customer_key = dc.customer_key
GROUP BY dc.customer_name, dc.city, dc.country
ORDER BY lifetime_value DESC;

-- ============================================
-- FUNCTION: Update ETL Log
-- ============================================
CREATE OR REPLACE FUNCTION etl.log_etl_execution(
    p_dag_id VARCHAR(100),
    p_task_id VARCHAR(100),
    p_execution_date DATE,
    p_status VARCHAR(20),
    p_rows_extracted INTEGER DEFAULT NULL,
    p_rows_loaded INTEGER DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    v_log_id INTEGER;
BEGIN
    INSERT INTO etl.etl_log (
        dag_id, task_id, execution_date, 
        start_time, status, 
        rows_extracted, rows_loaded, error_message
    )
    VALUES (
        p_dag_id, p_task_id, p_execution_date,
        CURRENT_TIMESTAMP, p_status,
        p_rows_extracted, p_rows_loaded, p_error_message
    )
    RETURNING log_id INTO v_log_id;
    
    RETURN v_log_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- FUNCTION: Update ETL Log Status
-- ============================================
CREATE OR REPLACE FUNCTION etl.update_etl_log(
    p_log_id INTEGER,
    p_status VARCHAR(20),
    p_rows_loaded INTEGER DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    UPDATE etl.etl_log
    SET 
        end_time = CURRENT_TIMESTAMP,
        status = p_status,
        rows_loaded = COALESCE(p_rows_loaded, rows_loaded),
        error_message = p_error_message
    WHERE log_id = p_log_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- VERIFICATION
-- ============================================
\echo '============================================'
\echo 'DATABASE INITIALIZATION COMPLETED'
\echo '============================================'

\echo 'Tables created:'
SELECT schemaname, tablename 
FROM pg_tables 
WHERE schemaname IN ('dwh', 'etl')
ORDER BY schemaname, tablename;

\echo '============================================'
\echo 'Views created:'
SELECT schemaname, viewname 
FROM pg_views 
WHERE schemaname = 'dwh'
ORDER BY viewname;

\echo '============================================'
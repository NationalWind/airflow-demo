-- ============================================
-- INITIALIZE TARGET DATABASE (STG, NDS, DDS)
-- ============================================

\c analytics;

-- ============================================
-- STAGING LAYER (STG)
-- ============================================

-- STG: CRM Schema
DROP SCHEMA IF EXISTS stg_crm CASCADE;
CREATE SCHEMA stg_crm;

CREATE TABLE stg_crm.customers (
    customer_id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(120),
    phone VARCHAR(20),
    date_of_birth DATE,
    city VARCHAR(80),
    state VARCHAR(50),
    country VARCHAR(80),
    postal_code VARCHAR(20),
    customer_segment VARCHAR(20),
    loyalty_tier VARCHAR(20),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    stg_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    stg_source_system VARCHAR(50) DEFAULT 'crm'
);

CREATE TABLE stg_crm.customer_interactions (
    interaction_id INTEGER,
    customer_id INTEGER,
    interaction_type VARCHAR(50),
    interaction_date TIMESTAMP,
    channel VARCHAR(30),
    notes TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    stg_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    stg_source_system VARCHAR(50) DEFAULT 'crm'
);

-- STG: ERP Schema
DROP SCHEMA IF EXISTS stg_erp CASCADE;
CREATE SCHEMA stg_erp;

CREATE TABLE stg_erp.products (
    product_id INTEGER,
    sku VARCHAR(50),
    product_name VARCHAR(100),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    brand VARCHAR(50),
    unit_price NUMERIC(10,2),
    cost_price NUMERIC(10,2),
    weight_kg NUMERIC(8,2),
    dimensions VARCHAR(50),
    active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    stg_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    stg_source_system VARCHAR(50) DEFAULT 'erp'
);

CREATE TABLE stg_erp.orders (
    order_id INTEGER,
    order_number VARCHAR(50),
    customer_email VARCHAR(120),
    order_date TIMESTAMP,
    order_status VARCHAR(20),
    payment_method VARCHAR(30),
    payment_status VARCHAR(20),
    shipping_address TEXT,
    total_amount NUMERIC(12,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    stg_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    stg_source_system VARCHAR(50) DEFAULT 'erp'
);

CREATE TABLE stg_erp.order_items (
    order_item_id INTEGER,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    discount_percent NUMERIC(5,2),
    line_total NUMERIC(12,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    stg_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    stg_source_system VARCHAR(50) DEFAULT 'erp'
);

-- STG: WEB Schema
DROP SCHEMA IF EXISTS stg_web CASCADE;
CREATE SCHEMA stg_web;

CREATE TABLE stg_web.page_views (
    page_view_id INTEGER,
    session_id VARCHAR(100),
    user_email VARCHAR(120),
    page_url TEXT,
    page_title VARCHAR(200),
    referrer_url TEXT,
    device_type VARCHAR(20),
    browser VARCHAR(50),
    country VARCHAR(80),
    view_timestamp TIMESTAMP,
    time_on_page_seconds INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    stg_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    stg_source_system VARCHAR(50) DEFAULT 'web'
);

CREATE TABLE stg_web.events (
    event_id INTEGER,
    session_id VARCHAR(100),
    user_email VARCHAR(120),
    event_type VARCHAR(50),
    event_category VARCHAR(50),
    event_action VARCHAR(100),
    event_label VARCHAR(200),
    event_value NUMERIC(10,2),
    event_timestamp TIMESTAMP,
    page_url TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    stg_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    stg_source_system VARCHAR(50) DEFAULT 'web'
);

-- ============================================
-- NORMALIZED DATA STORE (NDS) - 3NF
-- ============================================
DROP SCHEMA IF EXISTS nds CASCADE;
CREATE SCHEMA nds;

CREATE TABLE nds.customers (
    customer_sk SERIAL PRIMARY KEY,
    customer_nk VARCHAR(100) NOT NULL UNIQUE,
    source_system VARCHAR(50) NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(120),
    phone VARCHAR(20),
    date_of_birth DATE,
    city VARCHAR(80),
    state VARCHAR(50),
    country VARCHAR(80),
    postal_code VARCHAR(20),
    customer_segment VARCHAR(20),
    loyalty_tier VARCHAR(20),
    source_created_at TIMESTAMP,
    source_updated_at TIMESTAMP,
    nds_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nds_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE nds.products (
    product_sk SERIAL PRIMARY KEY,
    product_nk VARCHAR(100) NOT NULL UNIQUE,
    source_system VARCHAR(50) NOT NULL,
    sku VARCHAR(50),
    product_name VARCHAR(100),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    brand VARCHAR(50),
    unit_price NUMERIC(10,2),
    cost_price NUMERIC(10,2),
    weight_kg NUMERIC(8,2),
    active BOOLEAN,
    source_created_at TIMESTAMP,
    source_updated_at TIMESTAMP,
    nds_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nds_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE nds.orders (
    order_sk SERIAL PRIMARY KEY,
    order_nk VARCHAR(100) NOT NULL UNIQUE,
    customer_nk VARCHAR(100) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    order_number VARCHAR(50),
    customer_email VARCHAR(120),
    order_date TIMESTAMP,
    order_status VARCHAR(20),
    payment_method VARCHAR(30),
    payment_status VARCHAR(20),
    shipping_address TEXT,
    total_amount NUMERIC(12,2),
    source_created_at TIMESTAMP,
    source_updated_at TIMESTAMP,
    nds_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nds_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE nds.order_items (
    order_item_sk SERIAL PRIMARY KEY,
    order_item_nk VARCHAR(100) NOT NULL UNIQUE,
    order_nk VARCHAR(100) NOT NULL,
    product_nk VARCHAR(100) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    discount_percent NUMERIC(5,2),
    line_total NUMERIC(12,2),
    source_created_at TIMESTAMP,
    source_updated_at TIMESTAMP,
    nds_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nds_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- DIMENSIONAL DATA STORE (DDS) - STAR SCHEMA
-- ============================================
DROP SCHEMA IF EXISTS dds CASCADE;
CREATE SCHEMA dds;

CREATE TABLE dds.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_nk VARCHAR(100) NOT NULL,
    source_system VARCHAR(50),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(120),
    phone VARCHAR(20),
    date_of_birth DATE,
    city VARCHAR(80),
    state VARCHAR(50),
    country VARCHAR(80),
    postal_code VARCHAR(20),
    customer_segment VARCHAR(20),
    loyalty_tier VARCHAR(20),
    source_created_at TIMESTAMP,
    source_updated_at TIMESTAMP,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    hash_key VARCHAR(64),
    dds_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_nk, valid_from)
);

CREATE TABLE dds.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_nk VARCHAR(100) NOT NULL,
    source_system VARCHAR(50),
    sku VARCHAR(50),
    product_name VARCHAR(100),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    brand VARCHAR(50),
    unit_price NUMERIC(10,2),
    cost_price NUMERIC(10,2),
    weight_kg NUMERIC(8,2),
    active BOOLEAN,
    source_created_at TIMESTAMP,
    source_updated_at TIMESTAMP,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    hash_key VARCHAR(64),
    dds_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_nk, valid_from)
);

CREATE TABLE dds.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    quarter_name VARCHAR(10),
    month INTEGER,
    month_name VARCHAR(20),
    week INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

-- Pre-populate dim_date with 10 years of data (2020-2030)
INSERT INTO dds.dim_date (
    date_key,
    full_date,
    year,
    quarter,
    quarter_name,
    month,
    month_name,
    week,
    day_of_month,
    day_of_week,
    day_name,
    is_weekend,
    is_holiday,
    fiscal_year,
    fiscal_quarter
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
    d::DATE AS full_date,
    EXTRACT(YEAR FROM d)::INTEGER AS year,
    EXTRACT(QUARTER FROM d)::INTEGER AS quarter,
    'Q' || EXTRACT(QUARTER FROM d)::TEXT AS quarter_name,
    EXTRACT(MONTH FROM d)::INTEGER AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(WEEK FROM d)::INTEGER AS week,
    EXTRACT(DAY FROM d)::INTEGER AS day_of_month,
    EXTRACT(DOW FROM d)::INTEGER AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday,
    EXTRACT(YEAR FROM d)::INTEGER AS fiscal_year,
    EXTRACT(QUARTER FROM d)::INTEGER AS fiscal_quarter
FROM generate_series(
    DATE '2020-01-01',
    DATE '2030-12-31',
    '1 day'::INTERVAL
) d;

CREATE TABLE dds.fact_sales (
    sale_key SERIAL PRIMARY KEY,
    sale_nk VARCHAR(100) UNIQUE NOT NULL,
    customer_key INTEGER REFERENCES dds.dim_customers(customer_key),
    product_key INTEGER REFERENCES dds.dim_products(product_key),
    date_key INTEGER REFERENCES dds.dim_date(date_key),
    order_number VARCHAR(50),
    source_system VARCHAR(50),
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    discount_amount NUMERIC(10,2),
    line_total NUMERIC(12,2),
    cost_amount NUMERIC(12,2),
    profit_amount NUMERIC(12,2),
    source_created_at TIMESTAMP,
    source_updated_at TIMESTAMP,
    dds_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- INDEXES FOR NDS
-- ============================================
CREATE INDEX idx_nds_customers_email ON nds.customers(email);
CREATE INDEX idx_nds_orders_customer_nk ON nds.orders(customer_nk);
CREATE INDEX idx_nds_order_items_order_nk ON nds.order_items(order_nk);
CREATE INDEX idx_nds_order_items_product_nk ON nds.order_items(product_nk);

-- ============================================
-- INDEXES FOR DDS
-- ============================================
CREATE INDEX idx_dds_customers_nk ON dds.dim_customers(customer_nk);
CREATE INDEX idx_dds_customers_current ON dds.dim_customers(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dds_customers_email ON dds.dim_customers(email);
CREATE INDEX idx_dds_products_nk ON dds.dim_products(product_nk);
CREATE INDEX idx_dds_products_current ON dds.dim_products(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dds_products_category ON dds.dim_products(category);
CREATE INDEX idx_fact_sales_customer_key ON dds.fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product_key ON dds.fact_sales(product_key);
CREATE INDEX idx_fact_sales_date_key ON dds.fact_sales(date_key);
CREATE INDEX idx_fact_sales_order_number ON dds.fact_sales(order_number);

\echo '============================================'
\echo 'TARGET DATABASE INITIALIZED (STG, NDS, DDS)'
\echo '============================================'

-- ============================================
-- INITIALIZE ERP SOURCE DATABASE
-- ============================================

\c erp_db;

DROP SCHEMA IF EXISTS erp CASCADE;
CREATE SCHEMA erp;

-- ============================================
-- ERP: PRODUCTS
-- ============================================
CREATE TABLE erp.products (
    product_id SERIAL PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    subcategory VARCHAR(50),
    brand VARCHAR(50),
    unit_price NUMERIC(10,2) NOT NULL,
    cost_price NUMERIC(10,2),
    weight_kg NUMERIC(8,2),
    dimensions VARCHAR(50),
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- ERP: INVENTORY
-- ============================================
CREATE TABLE erp.inventory (
    inventory_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES erp.products(product_id),
    warehouse_location VARCHAR(50),
    quantity_on_hand INTEGER,
    quantity_reserved INTEGER,
    reorder_point INTEGER,
    last_restock_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- ERP: ORDERS
-- ============================================
CREATE TABLE erp.orders (
    order_id SERIAL PRIMARY KEY,
    order_number VARCHAR(50) UNIQUE NOT NULL,
    customer_email VARCHAR(120) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    order_status VARCHAR(20) DEFAULT 'PENDING',
    payment_method VARCHAR(30),
    payment_status VARCHAR(20),
    shipping_address TEXT,
    total_amount NUMERIC(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- ERP: ORDER ITEMS
-- ============================================
CREATE TABLE erp.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES erp.orders(order_id),
    product_id INTEGER REFERENCES erp.products(product_id),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    discount_percent NUMERIC(5,2) DEFAULT 0,
    line_total NUMERIC(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- TRIGGERS FOR AUTO-UPDATING updated_at
-- ============================================
CREATE OR REPLACE FUNCTION erp.update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_products_updated_at
    BEFORE UPDATE ON erp.products
    FOR EACH ROW EXECUTE FUNCTION erp.update_updated_at();

CREATE TRIGGER trg_inventory_updated_at
    BEFORE UPDATE ON erp.inventory
    FOR EACH ROW EXECUTE FUNCTION erp.update_updated_at();

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON erp.orders
    FOR EACH ROW EXECUTE FUNCTION erp.update_updated_at();

CREATE TRIGGER trg_order_items_updated_at
    BEFORE UPDATE ON erp.order_items
    FOR EACH ROW EXECUTE FUNCTION erp.update_updated_at();

-- ============================================
-- INDEXES
-- ============================================
CREATE INDEX idx_products_sku ON erp.products(sku);
CREATE INDEX idx_products_category ON erp.products(category);
CREATE INDEX idx_products_updated_at ON erp.products(updated_at);
CREATE INDEX idx_inventory_product_id ON erp.inventory(product_id);
CREATE INDEX idx_inventory_updated_at ON erp.inventory(updated_at);
CREATE INDEX idx_orders_customer_email ON erp.orders(customer_email);
CREATE INDEX idx_orders_order_date ON erp.orders(order_date);
CREATE INDEX idx_orders_updated_at ON erp.orders(updated_at);
CREATE INDEX idx_order_items_order_id ON erp.order_items(order_id);
CREATE INDEX idx_order_items_product_id ON erp.order_items(product_id);
CREATE INDEX idx_order_items_updated_at ON erp.order_items(updated_at);

-- ============================================
-- SAMPLE DATA
-- ============================================
INSERT INTO erp.products (product_id, sku, product_name, category, subcategory, brand, unit_price, cost_price, weight_kg, active, created_at, updated_at) VALUES
    (101, 'PERI-KB-001', 'Wireless Keyboard', 'Peripherals', 'Input Devices', 'TechPro', 49.99, 25.00, 0.65, TRUE, '2024-01-01 08:00:00', '2024-01-01 08:00:00'),
    (102, 'PERI-MS-001', 'Ergonomic Mouse', 'Peripherals', 'Input Devices', 'TechPro', 39.99, 18.00, 0.15, TRUE, '2024-01-01 08:00:00', '2024-01-01 08:00:00'),
    (103, 'DISP-MN-001', '4K Monitor 27"', 'Displays', 'Monitors', 'ViewMaster', 329.00, 200.00, 5.50, TRUE, '2024-01-01 08:00:00', '2024-01-01 08:00:00'),
    (104, 'ACCS-DK-001', 'USB-C Docking Station', 'Accessories', 'Connectivity', 'PortHub', 189.00, 95.00, 0.80, TRUE, '2024-01-01 08:00:00', '2024-01-01 08:00:00'),
    (105, 'AUDI-HP-001', 'Noise Cancelling Headphones', 'Audio', 'Headphones', 'SoundWave', 249.00, 130.00, 0.35, TRUE, '2024-01-01 08:00:00', '2024-01-01 08:00:00');

INSERT INTO erp.inventory (product_id, warehouse_location, quantity_on_hand, quantity_reserved, reorder_point, last_restock_date) VALUES
    (101, 'WH-US-01', 500, 50, 100, '2024-11-01'),
    (102, 'WH-US-01', 750, 30, 150, '2024-11-01'),
    (103, 'WH-US-02', 200, 20, 50, '2024-11-05'),
    (104, 'WH-EU-01', 300, 25, 75, '2024-11-03'),
    (105, 'WH-EU-01', 450, 40, 100, '2024-11-02');

INSERT INTO erp.orders (order_id, order_number, customer_email, order_date, order_status, payment_method, payment_status, total_amount, created_at, updated_at) VALUES
    (5001, 'ORD-2024-001', 'alice.nguyen@example.com', '2024-11-20 10:30:00', 'COMPLETED', 'Credit Card', 'PAID', 288.98, '2024-11-20 10:30:00', '2024-11-20 10:30:00'),
    (5002, 'ORD-2024-002', 'alice.nguyen@example.com', '2024-11-21 11:00:00', 'COMPLETED', 'PayPal', 'PAID', 249.00, '2024-11-21 11:00:00', '2024-11-21 11:00:00'),
    (5003, 'ORD-2024-003', 'bruno.costa@example.com', '2024-11-21 14:15:00', 'COMPLETED', 'Credit Card', 'PAID', 408.98, '2024-11-21 14:15:00', '2024-11-21 14:15:00'),
    (5004, 'ORD-2024-004', 'carmen.diaz@example.com', '2024-11-22 09:45:00', 'COMPLETED', 'Credit Card', 'PAID', 338.98, '2024-11-22 09:45:00', '2024-11-22 09:45:00'),
    (5005, 'ORD-2024-005', 'deepak.iyer@example.com', '2024-11-22 16:30:00', 'PROCESSING', 'Bank Transfer', 'PENDING', 378.00, '2024-11-22 16:30:00', '2024-11-22 16:30:00'),
    (5006, 'ORD-2024-006', 'elena.fischer@example.com', '2024-11-23 12:00:00', 'COMPLETED', 'Credit Card', 'PAID', 378.99, '2024-11-23 12:00:00', '2024-11-23 12:00:00');

INSERT INTO erp.order_items (order_id, product_id, quantity, unit_price, discount_percent, line_total, created_at, updated_at) VALUES
    (5001, 101, 2, 49.99, 0, 99.98, '2024-11-20 10:30:00', '2024-11-20 10:30:00'),
    (5001, 104, 1, 189.00, 0, 189.00, '2024-11-20 10:30:00', '2024-11-20 10:30:00'),
    (5002, 105, 1, 249.00, 0, 249.00, '2024-11-21 11:00:00', '2024-11-21 11:00:00'),
    (5003, 103, 1, 329.00, 0, 329.00, '2024-11-21 14:15:00', '2024-11-21 14:15:00'),
    (5003, 102, 2, 39.99, 0, 79.98, '2024-11-21 14:15:00', '2024-11-21 14:15:00'),
    (5004, 101, 1, 49.99, 0, 49.99, '2024-11-22 09:45:00', '2024-11-22 09:45:00'),
    (5004, 102, 1, 39.99, 0, 39.99, '2024-11-22 09:45:00', '2024-11-22 09:45:00'),
    (5004, 105, 1, 249.00, 0, 249.00, '2024-11-22 09:45:00', '2024-11-22 09:45:00'),
    (5005, 104, 2, 189.00, 0, 378.00, '2024-11-22 16:30:00', '2024-11-22 16:30:00'),
    (5006, 103, 1, 329.00, 0, 329.00, '2024-11-23 12:00:00', '2024-11-23 12:00:00'),
    (5006, 101, 1, 49.99, 0, 49.99, '2024-11-23 12:00:00', '2024-11-23 12:00:00');

\echo '============================================'
\echo 'ERP SOURCE DATABASE INITIALIZED'
\echo '============================================'

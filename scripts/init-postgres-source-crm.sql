-- ============================================
-- INITIALIZE CRM SOURCE DATABASE
-- ============================================

\c crm_db;

DROP SCHEMA IF EXISTS crm CASCADE;
CREATE SCHEMA crm;

-- ============================================
-- CRM: CUSTOMERS
-- ============================================
CREATE TABLE crm.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(120) UNIQUE NOT NULL,
    phone VARCHAR(20),
    date_of_birth DATE,
    city VARCHAR(80),
    state VARCHAR(50),
    country VARCHAR(80),
    postal_code VARCHAR(20),
    customer_segment VARCHAR(20),
    loyalty_tier VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- CRM: CUSTOMER INTERACTIONS
-- ============================================
CREATE TABLE crm.customer_interactions (
    interaction_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES crm.customers(customer_id),
    interaction_type VARCHAR(50),
    interaction_date TIMESTAMP,
    channel VARCHAR(30),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- TRIGGERS FOR AUTO-UPDATING updated_at
-- ============================================
CREATE OR REPLACE FUNCTION crm.update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON crm.customers
    FOR EACH ROW EXECUTE FUNCTION crm.update_updated_at();

CREATE TRIGGER trg_customer_interactions_updated_at
    BEFORE UPDATE ON crm.customer_interactions
    FOR EACH ROW EXECUTE FUNCTION crm.update_updated_at();

-- ============================================
-- INDEXES
-- ============================================
CREATE INDEX idx_customers_email ON crm.customers(email);
CREATE INDEX idx_customers_updated_at ON crm.customers(updated_at);
CREATE INDEX idx_customer_interactions_customer_id ON crm.customer_interactions(customer_id);
CREATE INDEX idx_customer_interactions_updated_at ON crm.customer_interactions(updated_at);

-- ============================================
-- SAMPLE DATA
-- ============================================
INSERT INTO crm.customers (customer_id, first_name, last_name, email, phone, date_of_birth, city, state, country, postal_code, customer_segment, loyalty_tier, created_at, updated_at) VALUES
    (1, 'Alice', 'Nguyen', 'alice.nguyen@example.com', '+1-206-555-0101', '1985-03-15', 'Seattle', 'WA', 'USA', '98101', 'Premium', 'Gold', '2024-01-04 10:00:00', '2024-01-04 10:00:00'),
    (2, 'Bruno', 'Costa', 'bruno.costa@example.com', '+351-21-555-0102', '1990-07-22', 'Lisbon', 'Lisbon', 'Portugal', '1000-001', 'Standard', 'Silver', '2024-02-12 11:30:00', '2024-02-12 11:30:00'),
    (3, 'Carmen', 'Diaz', 'carmen.diaz@example.com', '+34-91-555-0103', '1988-11-08', 'Madrid', 'Madrid', 'Spain', '28001', 'Premium', 'Platinum', '2024-03-08 09:15:00', '2024-03-08 09:15:00'),
    (4, 'Deepak', 'Iyer', 'deepak.iyer@example.com', '+91-80-555-0104', '1992-05-18', 'Bangalore', 'Karnataka', 'India', '560001', 'Standard', 'Bronze', '2024-01-22 14:00:00', '2024-01-22 14:00:00'),
    (5, 'Elena', 'Fischer', 'elena.fischer@example.com', '+49-89-555-0105', '1987-09-25', 'Munich', 'Bavaria', 'Germany', '80331', 'Premium', 'Gold', '2024-04-18 16:45:00', '2024-04-18 16:45:00');

INSERT INTO crm.customer_interactions (customer_id, interaction_type, interaction_date, channel, notes, created_at, updated_at) VALUES
    (1, 'Support Call', '2024-11-15 10:30:00', 'Phone', 'Product inquiry', '2024-11-15 10:30:00', '2024-11-15 10:30:00'),
    (2, 'Email', '2024-11-18 14:20:00', 'Email', 'Order status inquiry', '2024-11-18 14:20:00', '2024-11-18 14:20:00'),
    (3, 'Chat', '2024-11-20 16:45:00', 'Web Chat', 'Technical support', '2024-11-20 16:45:00', '2024-11-20 16:45:00');

\echo '============================================'
\echo 'CRM SOURCE DATABASE INITIALIZED'
\echo '============================================'

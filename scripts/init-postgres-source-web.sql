-- ============================================
-- INITIALIZE WEB ANALYTICS SOURCE DATABASE
-- ============================================

\c web_db;

DROP SCHEMA IF EXISTS web CASCADE;
CREATE SCHEMA web;

-- ============================================
-- WEB: PAGE VIEWS
-- ============================================
CREATE TABLE web.page_views (
    page_view_id SERIAL PRIMARY KEY,
    session_id VARCHAR(100) NOT NULL,
    user_email VARCHAR(120),
    page_url TEXT NOT NULL,
    page_title VARCHAR(200),
    referrer_url TEXT,
    device_type VARCHAR(20),
    browser VARCHAR(50),
    country VARCHAR(80),
    view_timestamp TIMESTAMP NOT NULL,
    time_on_page_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- WEB: EVENTS
-- ============================================
CREATE TABLE web.events (
    event_id SERIAL PRIMARY KEY,
    session_id VARCHAR(100) NOT NULL,
    user_email VARCHAR(120),
    event_type VARCHAR(50) NOT NULL,
    event_category VARCHAR(50),
    event_action VARCHAR(100),
    event_label VARCHAR(200),
    event_value NUMERIC(10,2),
    event_timestamp TIMESTAMP NOT NULL,
    page_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- WEB: USER SESSIONS
-- ============================================
CREATE TABLE web.user_sessions (
    session_id VARCHAR(100) PRIMARY KEY,
    user_email VARCHAR(120),
    session_start TIMESTAMP NOT NULL,
    session_end TIMESTAMP,
    session_duration_seconds INTEGER,
    pages_viewed INTEGER,
    device_type VARCHAR(20),
    browser VARCHAR(50),
    os VARCHAR(50),
    country VARCHAR(80),
    city VARCHAR(80),
    campaign_source VARCHAR(100),
    campaign_medium VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- TRIGGERS FOR AUTO-UPDATING updated_at
-- ============================================
CREATE OR REPLACE FUNCTION web.update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_page_views_updated_at
    BEFORE UPDATE ON web.page_views
    FOR EACH ROW EXECUTE FUNCTION web.update_updated_at();

CREATE TRIGGER trg_events_updated_at
    BEFORE UPDATE ON web.events
    FOR EACH ROW EXECUTE FUNCTION web.update_updated_at();

CREATE TRIGGER trg_user_sessions_updated_at
    BEFORE UPDATE ON web.user_sessions
    FOR EACH ROW EXECUTE FUNCTION web.update_updated_at();

-- ============================================
-- INDEXES
-- ============================================
CREATE INDEX idx_page_views_session_id ON web.page_views(session_id);
CREATE INDEX idx_page_views_user_email ON web.page_views(user_email);
CREATE INDEX idx_page_views_timestamp ON web.page_views(view_timestamp);
CREATE INDEX idx_page_views_updated_at ON web.page_views(updated_at);
CREATE INDEX idx_events_session_id ON web.events(session_id);
CREATE INDEX idx_events_user_email ON web.events(user_email);
CREATE INDEX idx_events_type ON web.events(event_type);
CREATE INDEX idx_events_timestamp ON web.events(event_timestamp);
CREATE INDEX idx_events_updated_at ON web.events(updated_at);
CREATE INDEX idx_user_sessions_email ON web.user_sessions(user_email);
CREATE INDEX idx_user_sessions_start ON web.user_sessions(session_start);
CREATE INDEX idx_user_sessions_updated_at ON web.user_sessions(updated_at);

-- ============================================
-- SAMPLE DATA
-- ============================================
INSERT INTO web.user_sessions (session_id, user_email, session_start, session_end, session_duration_seconds, pages_viewed, device_type, browser, os, country, city, campaign_source, campaign_medium, created_at, updated_at) VALUES
    ('sess_001', 'alice.nguyen@example.com', '2024-11-20 09:15:00', '2024-11-20 09:32:00', 1020, 5, 'Desktop', 'Chrome', 'Windows', 'USA', 'Seattle', 'Google', 'CPC', '2024-11-20 09:15:00', '2024-11-20 09:15:00'),
    ('sess_002', 'bruno.costa@example.com', '2024-11-21 13:45:00', '2024-11-21 14:10:00', 1500, 8, 'Mobile', 'Safari', 'iOS', 'Portugal', 'Lisbon', 'Facebook', 'Social', '2024-11-21 13:45:00', '2024-11-21 13:45:00'),
    ('sess_003', 'carmen.diaz@example.com', '2024-11-22 08:30:00', '2024-11-22 09:05:00', 2100, 12, 'Desktop', 'Firefox', 'macOS', 'Spain', 'Madrid', 'Direct', 'None', '2024-11-22 08:30:00', '2024-11-22 08:30:00'),
    ('sess_004', 'deepak.iyer@example.com', '2024-11-22 15:00:00', '2024-11-22 15:45:00', 2700, 15, 'Tablet', 'Chrome', 'Android', 'India', 'Bangalore', 'Email', 'Newsletter', '2024-11-22 15:00:00', '2024-11-22 15:00:00'),
    ('sess_005', 'elena.fischer@example.com', '2024-11-23 11:20:00', '2024-11-23 11:50:00', 1800, 10, 'Desktop', 'Edge', 'Windows', 'Germany', 'Munich', 'Google', 'Organic', '2024-11-23 11:20:00', '2024-11-23 11:20:00');

INSERT INTO web.page_views (session_id, user_email, page_url, page_title, referrer_url, device_type, browser, country, view_timestamp, time_on_page_seconds, created_at, updated_at) VALUES
    ('sess_001', 'alice.nguyen@example.com', '/products/keyboards', 'Wireless Keyboards', 'https://google.com', 'Desktop', 'Chrome', 'USA', '2024-11-20 09:15:00', 180, '2024-11-20 09:15:00', '2024-11-20 09:15:00'),
    ('sess_001', 'alice.nguyen@example.com', '/products/keyboards/wireless-keyboard', 'Wireless Keyboard Pro', '/products/keyboards', 'Desktop', 'Chrome', 'USA', '2024-11-20 09:18:00', 240, '2024-11-20 09:18:00', '2024-11-20 09:18:00'),
    ('sess_001', 'alice.nguyen@example.com', '/cart', 'Shopping Cart', '/products/keyboards/wireless-keyboard', 'Desktop', 'Chrome', 'USA', '2024-11-20 09:22:00', 120, '2024-11-20 09:22:00', '2024-11-20 09:22:00'),
    ('sess_002', 'bruno.costa@example.com', '/products/monitors', '4K Monitors', 'https://facebook.com', 'Mobile', 'Safari', 'Portugal', '2024-11-21 13:45:00', 200, '2024-11-21 13:45:00', '2024-11-21 13:45:00'),
    ('sess_003', 'carmen.diaz@example.com', '/products', 'All Products', NULL, 'Desktop', 'Firefox', 'Spain', '2024-11-22 08:30:00', 150, '2024-11-22 08:30:00', '2024-11-22 08:30:00');

INSERT INTO web.events (session_id, user_email, event_type, event_category, event_action, event_label, event_value, event_timestamp, page_url, created_at, updated_at) VALUES
    ('sess_001', 'alice.nguyen@example.com', 'click', 'Product', 'Add to Cart', 'Wireless Keyboard', 49.99, '2024-11-20 09:22:15', '/products/keyboards/wireless-keyboard', '2024-11-20 09:22:15', '2024-11-20 09:22:15'),
    ('sess_001', 'alice.nguyen@example.com', 'click', 'Product', 'Add to Cart', 'USB-C Dock', 189.00, '2024-11-20 09:25:30', '/products/accessories/usbc-dock', '2024-11-20 09:25:30', '2024-11-20 09:25:30'),
    ('sess_001', 'alice.nguyen@example.com', 'purchase', 'Ecommerce', 'Checkout Complete', 'Order #5001', 288.98, '2024-11-20 09:30:00', '/checkout/success', '2024-11-20 09:30:00', '2024-11-20 09:30:00'),
    ('sess_002', 'bruno.costa@example.com', 'click', 'Product', 'View Product', '4K Monitor', 329.00, '2024-11-21 13:50:00', '/products/monitors/4k-monitor', '2024-11-21 13:50:00', '2024-11-21 13:50:00'),
    ('sess_003', 'carmen.diaz@example.com', 'search', 'Navigation', 'Product Search', 'headphones', NULL, '2024-11-22 08:35:00', '/search', '2024-11-22 08:35:00', '2024-11-22 08:35:00');

\echo '============================================'
\echo 'WEB ANALYTICS SOURCE DATABASE INITIALIZED'
\echo '============================================'

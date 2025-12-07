-- ============================================
-- INITIALIZE METADATA DATABASE
-- ============================================

\c metadata_db;

-- ============================================
-- METADATA SCHEMA FOR ETL CONTROL
-- ============================================
DROP SCHEMA IF EXISTS metadata CASCADE;
CREATE SCHEMA metadata;

-- ============================================
-- ETL WATERMARK TABLE
-- Tracks LSET/CET for incremental loads
-- ============================================
CREATE TABLE metadata.etl_watermark (
    watermark_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    target_layer VARCHAR(20) NOT NULL,
    target_table VARCHAR(100) NOT NULL,

    lset TIMESTAMP,
    cet TIMESTAMP,

    last_run_start TIMESTAMP,
    last_run_end TIMESTAMP,
    last_run_status VARCHAR(20),

    rows_extracted INTEGER DEFAULT 0,
    rows_loaded INTEGER DEFAULT 0,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(dag_id, source_system, source_table, target_layer)
);

-- ============================================
-- ETL RUN LOG TABLE
-- Detailed run history for each pipeline execution
-- ============================================
CREATE TABLE metadata.etl_run_log (
    run_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    dag_run_id VARCHAR(200) NOT NULL,
    task_id VARCHAR(100),
    source_system VARCHAR(50),
    source_table VARCHAR(100),
    target_layer VARCHAR(20),
    target_table VARCHAR(100),

    execution_date DATE,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    status VARCHAR(20),

    lset_used TIMESTAMP,
    cet_used TIMESTAMP,

    rows_extracted INTEGER DEFAULT 0,
    rows_loaded INTEGER DEFAULT 0,
    rows_updated INTEGER DEFAULT 0,
    rows_deleted INTEGER DEFAULT 0,

    error_message TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- DATA QUALITY CHECKS TABLE
-- Track data quality validations
-- ============================================
CREATE TABLE metadata.data_quality_checks (
    check_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    check_name VARCHAR(100) NOT NULL,
    check_type VARCHAR(50),
    target_schema VARCHAR(50),
    target_table VARCHAR(100),
    check_query TEXT,
    expected_result VARCHAR(500),
    actual_result VARCHAR(500),
    status VARCHAR(20),
    severity VARCHAR(20),
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- INDEXES
-- ============================================
CREATE INDEX idx_watermark_lookup
    ON metadata.etl_watermark(dag_id, source_system, source_table, target_layer);

CREATE INDEX idx_watermark_status
    ON metadata.etl_watermark(last_run_status, updated_at);

CREATE INDEX idx_run_log_dag
    ON metadata.etl_run_log(dag_id, execution_date);

CREATE INDEX idx_run_log_status
    ON metadata.etl_run_log(status, start_time);

CREATE INDEX idx_quality_checks_dag
    ON metadata.data_quality_checks(dag_id, check_timestamp);

-- ============================================
-- HELPER FUNCTIONS
-- ============================================

-- Function to get watermark (LSET/CET)
CREATE OR REPLACE FUNCTION metadata.get_watermark(
    p_dag_id VARCHAR(100),
    p_source_system VARCHAR(50),
    p_source_table VARCHAR(100),
    p_target_layer VARCHAR(20)
)
RETURNS TABLE (
    lset TIMESTAMP,
    cet TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        w.lset,
        CURRENT_TIMESTAMP AS cet
    FROM metadata.etl_watermark w
    WHERE w.dag_id = p_dag_id
      AND w.source_system = p_source_system
      AND w.source_table = p_source_table
      AND w.target_layer = p_target_layer
      AND w.last_run_status = 'SUCCESS';

    -- If no previous successful run, return NULL for LSET
    IF NOT FOUND THEN
        RETURN QUERY SELECT NULL::TIMESTAMP AS lset, CURRENT_TIMESTAMP AS cet;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to update watermark
CREATE OR REPLACE FUNCTION metadata.update_watermark(
    p_dag_id VARCHAR(100),
    p_source_system VARCHAR(50),
    p_source_table VARCHAR(100),
    p_target_layer VARCHAR(20),
    p_target_table VARCHAR(100),
    p_lset TIMESTAMP,
    p_cet TIMESTAMP,
    p_status VARCHAR(20),
    p_rows_extracted INTEGER DEFAULT 0,
    p_rows_loaded INTEGER DEFAULT 0
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO metadata.etl_watermark (
        dag_id, source_system, source_table, target_layer, target_table,
        lset, cet, last_run_start, last_run_end, last_run_status,
        rows_extracted, rows_loaded, updated_at
    )
    VALUES (
        p_dag_id, p_source_system, p_source_table, p_target_layer, p_target_table,
        p_lset, p_cet, p_lset, p_cet, p_status,
        p_rows_extracted, p_rows_loaded, CURRENT_TIMESTAMP
    )
    ON CONFLICT (dag_id, source_system, source_table, target_layer)
    DO UPDATE SET
        lset = EXCLUDED.lset,
        cet = EXCLUDED.cet,
        last_run_start = EXCLUDED.last_run_start,
        last_run_end = EXCLUDED.last_run_end,
        last_run_status = EXCLUDED.last_run_status,
        rows_extracted = EXCLUDED.rows_extracted,
        rows_loaded = EXCLUDED.rows_loaded,
        updated_at = CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql;

-- Function to log ETL run
CREATE OR REPLACE FUNCTION metadata.log_etl_run(
    p_dag_id VARCHAR(100),
    p_dag_run_id VARCHAR(200),
    p_task_id VARCHAR(100),
    p_source_system VARCHAR(50),
    p_source_table VARCHAR(100),
    p_target_layer VARCHAR(20),
    p_target_table VARCHAR(100),
    p_execution_date DATE,
    p_status VARCHAR(20),
    p_lset TIMESTAMP DEFAULT NULL,
    p_cet TIMESTAMP DEFAULT NULL,
    p_rows_extracted INTEGER DEFAULT 0,
    p_rows_loaded INTEGER DEFAULT 0,
    p_error_message TEXT DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    v_run_id INTEGER;
BEGIN
    INSERT INTO metadata.etl_run_log (
        dag_id, dag_run_id, task_id, source_system, source_table,
        target_layer, target_table, execution_date, start_time, status,
        lset_used, cet_used, rows_extracted, rows_loaded, error_message
    )
    VALUES (
        p_dag_id, p_dag_run_id, p_task_id, p_source_system, p_source_table,
        p_target_layer, p_target_table, p_execution_date, CURRENT_TIMESTAMP, p_status,
        p_lset, p_cet, p_rows_extracted, p_rows_loaded, p_error_message
    )
    RETURNING run_id INTO v_run_id;

    RETURN v_run_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- INITIALIZE WATERMARK TABLE
-- Set initial LSET to 1900-01-01 for all source tables
-- This ensures first run extracts all historical data
-- ============================================

-- Delete any existing watermarks (for clean initialization)
DELETE FROM metadata.etl_watermark;

-- Source to Staging watermarks
INSERT INTO metadata.etl_watermark (
    dag_id, source_system, source_table, target_layer, target_table,
    lset, cet, last_run_status
) VALUES
    ('source_to_staging', 'crm', 'customers', 'stg', 'stg_crm.customers', '1900-01-01', '1900-01-01', 'SUCCESS'),
    ('source_to_staging', 'erp', 'products', 'stg', 'stg_erp.products', '1900-01-01', '1900-01-01', 'SUCCESS'),
    ('source_to_staging', 'erp', 'orders', 'stg', 'stg_erp.orders', '1900-01-01', '1900-01-01', 'SUCCESS'),
    ('source_to_staging', 'erp', 'order_items', 'stg', 'stg_erp.order_items', '1900-01-01', '1900-01-01', 'SUCCESS'),
    ('source_to_staging', 'web', 'page_views', 'stg', 'stg_web.page_views', '1900-01-01', '1900-01-01', 'SUCCESS'),
    ('source_to_staging', 'web', 'events', 'stg', 'stg_web.events', '1900-01-01', '1900-01-01', 'SUCCESS'),

    -- Staging to NDS watermarks
    ('staging_to_nds', 'crm', 'customers', 'nds', 'nds.customers', '1900-01-01', '1900-01-01', 'SUCCESS'),
    ('staging_to_nds', 'erp', 'products', 'nds', 'nds.products', '1900-01-01', '1900-01-01', 'SUCCESS'),
    ('staging_to_nds', 'erp', 'orders', 'nds', 'nds.orders', '1900-01-01', '1900-01-01', 'SUCCESS'),
    ('staging_to_nds', 'erp', 'order_items', 'nds', 'nds.order_items', '1900-01-01', '1900-01-01', 'SUCCESS'),

    -- NDS to DDS watermarks
    ('nds_to_dds', 'nds', 'customers', 'dds', 'dds.dim_customers', '1900-01-01', '1900-01-01', 'SUCCESS'),
    ('nds_to_dds', 'nds', 'products', 'dds', 'dds.dim_products', '1900-01-01', '1900-01-01', 'SUCCESS'),
    ('nds_to_dds', 'system', 'date_dimension', 'dds', 'dds.dim_date', '1900-01-01', '1900-01-01', 'SUCCESS'),
    ('nds_to_dds', 'nds', 'order_items', 'dds', 'dds.fact_sales', '1900-01-01', '1900-01-01', 'SUCCESS');

\echo '============================================'
\echo 'METADATA DATABASE INITIALIZED'
\echo 'Initial watermarks set to 1900-01-01'
\echo 'This ensures first run extracts all historical data'
\echo '============================================'

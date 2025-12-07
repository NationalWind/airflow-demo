# Enterprise Data Warehouse Pipeline with Apache Airflow

A production-grade, 3-layer ETL pipeline demonstrating best practices for data warehousing with Apache Airflow 3.0.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         SOURCE SYSTEMS                          │
├──────────────────┬──────────────────┬──────────────────────────┤
│  CRM Database    │  ERP Database    │  Web Analytics Database  │
│  (Port: 5434)    │  (Port: 5436)    │  (Port: 5437)           │
│  - Customers     │  - Products      │  - Page Views           │
│  - Interactions  │  - Orders        │  - Events               │
│                  │  - Order Items   │  - User Sessions        │
│                  │  - Inventory     │                         │
└──────────────────┴──────────────────┴──────────────────────────┘
                              ↓
              ┌──────────────────────────────────┐
              │  DAG 1: source_to_staging        │
              │  Schedule: Every 15 minutes      │
              │  Pattern: Incremental (LSET/CET) │
              │  Load: Full Truncate/Load        │
              └──────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      STAGING LAYER (STG)                        │
│                         (Port: 5433)                            │
├──────────────────┬──────────────────┬──────────────────────────┤
│  stg_crm         │  stg_erp         │  stg_web                │
│  - customers     │  - products      │  - page_views           │
│                  │  - orders        │  - events               │
│                  │  - order_items   │                         │
└──────────────────┴──────────────────┴──────────────────────────┘
                              ↓
              ┌──────────────────────────────────┐
              │  DAG 2: staging_to_nds           │
              │  Schedule: Every 30 minutes      │
              │  Pattern: SCD Type 2             │
              │  Change Detection: Hash-based    │
              └──────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│            NORMALIZED DATA STORE (NDS) - 3NF                    │
│                         (Port: 5433)                            │
├─────────────────────────────────────────────────────────────────┤
│  nds.customers    │  nds.products   │  nds.orders              │
│  nds.order_items  │                                            │
│  Features: valid_from, valid_to, is_current, hash_key          │
└─────────────────────────────────────────────────────────────────┘
                              ↓
              ┌──────────────────────────────────┐
              │  DAG 3: nds_to_dds               │
              │  Schedule: Every hour            │
              │  Pattern: Star Schema            │
              │  Load: Dimension Upserts + Facts │
              └──────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│         DIMENSIONAL DATA STORE (DDS) - Star Schema              │
│                         (Port: 5433)                            │
├─────────────────────────────────────────────────────────────────┤
│  Dimensions:                                                    │
│  - dim_customers  - dim_products  - dim_date                   │
│                                                                 │
│  Facts:                                                         │
│  - fact_sales                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Key Features

### 1. **Event-Driven Execution with Datasets** 🆕
- **DAG 1 (source_to_staging)**: Time-based trigger (every 15 min)
- **DAG 2 (staging_to_nds)**: Triggered when staging datasets update
- **DAG 3 (nds_to_dds)**: Triggered when NDS datasets update
- Automatic data lineage visualization in Airflow UI
- Answer questions like "which source leads to dim_customers?"

### 2. **LSET/CET Pattern** (Incremental Processing)
- Custom metadata watermark table tracks last successful run
- Incremental extraction from sources (only changed records)
- Full truncate/load on staging for data freshness
- **Critical for fact tables**: Only process new records, not all 100M rows

### 3. **SCD Type 2** (Slowly Changing Dimensions)
- Historical tracking with `valid_from`, `valid_to`, `is_current`
- Hash-based change detection (MD5 of business attributes)
- Natural key generation with source system prefix

### 4. **3-Layer Architecture**
- **Staging (STG)**: Raw data from sources with audit columns
- **Normalized Data Store (NDS)**: 3NF normalized with history
- **Dimensional Data Store (DDS)**: Star schema for analytics

### 5. **Production Best Practices**
- Event-driven orchestration (runs when data changes, not on fixed schedule)
- Modular code structure with custom hooks and operators
- TaskFlow API for cleaner, Pythonic DAG definitions
- Centralized configuration management
- Comprehensive error handling and logging
- Parameterized SQL templates

### 6. **Optimized Docker Images**
- All PostgreSQL containers use **Alpine Linux** variant
- **80% smaller** images (78 MB vs 412 MB per container)
- **63% reduction** in total stack size (~1.17 GB vs ~3.17 GB)
- Faster pulls, less disk space, same performance

## Project Structure

```
airflow-demo/
├── dags/
│   ├── config/
│   │   └── pipeline_config.py          # Centralized configuration
│   ├── source_to_staging_dag.py        # DAG 1: Source → Staging
│   ├── staging_to_nds_dag.py           # DAG 2: Staging → NDS
│   └── nds_to_dds_dag.py               # DAG 3: NDS → DDS
├── plugins/
│   ├── hooks/
│   │   └── watermark_hook.py           # Custom hook for LSET/CET
│   ├── operators/
│   │   └── scd_operator.py             # SCD Type 2 operator
│   └── utils/
│       ├── constants.py                # Global constants
│       ├── hash_utils.py               # Hash and key generation
│       └── sql_templates.py            # Parameterized SQL queries
├── scripts/
│   ├── init-postgres-source-crm.sql    # CRM database schema + data
│   ├── init-postgres-source-erp.sql    # ERP database schema + data
│   ├── init-postgres-source-web.sql    # Web database schema + data
│   ├── init-postgres-target.sql        # STG, NDS, DDS schemas
│   └── init-postgres-metadata.sql      # Watermark tracking tables
├── docker-compose.yml                  # 6 PostgreSQL + Airflow
└── README.md                           # This file
```

## Database Ports

| Database | Container | Port | Purpose |
|----------|-----------|------|---------|
| CRM Source | postgres-source-crm | 5434 | Customer data |
| ERP Source | postgres-source-erp | 5436 | Products, orders |
| Web Source | postgres-source-web | 5437 | Analytics events |
| Target (STG/NDS/DDS) | postgres-target | 5433 | Data warehouse layers |
| Metadata | postgres-metadata | 5438 | LSET/CET watermarks |
| Airflow | postgres-airflow | 5435 | Airflow metadata |

## Quick Start

### 1. Prerequisites

```bash
# Ensure Docker and Docker Compose are installed
docker --version
docker-compose --version
```

### 2. Start All Services

```bash
# Start all 6 PostgreSQL databases + Airflow
docker-compose up -d

# Check service health
docker-compose ps

# Wait for Airflow to be ready (~60 seconds)
docker logs airflow -f
```

### 3. Access Airflow UI

```bash
# Open browser to http://localhost:8080
# Default credentials: admin / admin (created on first startup)
```

### 4. Enable and Trigger DAGs

**With event-driven execution, you only need to trigger the first DAG!**

In the Airflow UI:
1. Enable all 3 DAGs (toggle the switch)
2. Trigger `source_to_staging` manually
3. **DAG 2 automatically triggers** when staging datasets update
4. **DAG 3 automatically triggers** when NDS datasets update

Or use the CLI:

```bash
# Enable all DAGs
docker exec airflow airflow dags unpause source_to_staging
docker exec airflow airflow dags unpause staging_to_nds
docker exec airflow airflow dags unpause nds_to_dds

# Trigger ONLY the first DAG - rest triggers automatically!
docker exec airflow airflow dags trigger source_to_staging

# Watch the cascade:
# - source_to_staging completes → emits staging datasets
# - staging_to_nds auto-triggers → processes staging data → emits NDS datasets
# - nds_to_dds auto-triggers → loads dimensions and facts
```

### 5. View Data Lineage

Navigate to **Browse → Datasets** in Airflow UI to see:
- Which DAGs produce which datasets
- Which DAGs consume which datasets
- Full lineage graph from source to analytical tables

Click on any dataset (e.g., `postgres://target/dds/dim_customers`) to see:
- **Upstream lineage**: crm.customers → stg_crm.customers → nds.customers → dim_customers
- **Downstream lineage**: What depends on this dataset
- **Update history**: When was it last updated and by which DAG run

## Verification and Testing

### 1. Verify Source Data

```bash
# Connect to CRM database
docker exec -it postgres-source-crm psql -U crm_user -d crm_db

# Check data
SELECT COUNT(*) FROM crm.customers;
SELECT * FROM crm.customers LIMIT 5;

# Connect to ERP database
docker exec -it postgres-source-erp psql -U erp_user -d erp_db

# Check data
SELECT COUNT(*) FROM erp.products;
SELECT COUNT(*) FROM erp.orders;
SELECT COUNT(*) FROM erp.order_items;
```

### 2. Verify Staging Layer

```bash
# Connect to target database
docker exec -it postgres-target psql -U analytics -d analytics

# Check staging tables
SELECT COUNT(*) FROM stg_crm.customers;
SELECT COUNT(*) FROM stg_erp.products;
SELECT COUNT(*) FROM stg_erp.orders;
SELECT COUNT(*) FROM stg_erp.order_items;

# Verify audit columns
SELECT *, stg_source_system, stg_loaded_at
FROM stg_crm.customers
LIMIT 5;
```

### 3. Verify NDS Layer (Type 2 SCD)

```bash
# Check NDS tables
SELECT COUNT(*) FROM nds.customers WHERE is_current = TRUE;
SELECT COUNT(*) FROM nds.products WHERE is_current = TRUE;

# Verify SCD Type 2 structure
SELECT customer_sk, customer_id_nk, first_name, last_name,
       valid_from, valid_to, is_current, hash_key
FROM nds.customers
ORDER BY customer_id_nk, valid_from
LIMIT 10;

# Check for history records (if data changed)
SELECT customer_id_nk, COUNT(*) as version_count
FROM nds.customers
GROUP BY customer_id_nk
HAVING COUNT(*) > 1;
```

### 4. Verify DDS Layer (Star Schema)

```bash
# Check dimension tables
SELECT COUNT(*) FROM dds.dim_customers;
SELECT COUNT(*) FROM dds.dim_products;
SELECT COUNT(*) FROM dds.dim_date;

# Check fact table
SELECT COUNT(*) FROM dds.fact_sales;

# Sample star schema query
SELECT
    dd.year,
    dd.month_name,
    dc.customer_segment,
    dp.category,
    SUM(fs.quantity) as total_quantity,
    SUM(fs.line_total) as total_revenue,
    SUM(fs.profit) as total_profit
FROM dds.fact_sales fs
INNER JOIN dds.dim_date dd ON fs.order_date_sk = dd.date_sk
INNER JOIN dds.dim_customers dc ON fs.customer_sk = dc.customer_sk
INNER JOIN dds.dim_products dp ON fs.product_sk = dp.product_sk
GROUP BY dd.year, dd.month_name, dc.customer_segment, dp.category
ORDER BY dd.year, dd.month_name;
```

### 5. Verify Watermark Tracking

```bash
# Connect to metadata database
docker exec -it postgres-metadata psql -U metadata_user -d metadata_db

# Check watermark table
SELECT * FROM metadata.etl_watermark ORDER BY updated_at DESC;

# Check run logs
SELECT * FROM metadata.etl_run_log ORDER BY run_start_time DESC LIMIT 20;
```

## Testing Incremental Loads

### 1. Update Source Data

```bash
# Connect to CRM source
docker exec -it postgres-source-crm psql -U crm_user -d crm_db

# Update a customer (triggers SCD Type 2)
UPDATE crm.customers
SET loyalty_tier = 'PLATINUM', updated_at = CURRENT_TIMESTAMP
WHERE customer_id = 1;

# Insert new customer
INSERT INTO crm.customers (first_name, last_name, email, phone, city, state, country, customer_segment)
VALUES ('John', 'Doe', 'john.doe@example.com', '555-0123', 'San Francisco', 'CA', 'USA', 'PREMIUM');
```

### 2. Trigger Pipeline Again

**With event-driven execution, just trigger the first DAG!**

```bash
# Trigger source_to_staging - rest cascades automatically!
docker exec airflow airflow dags trigger source_to_staging

# Watch the cascade in Airflow UI or check logs
docker exec airflow airflow dags list-runs -d staging_to_nds --no-backfill
docker exec airflow airflow dags list-runs -d nds_to_dds --no-backfill

# Verify staging (after source_to_staging completes)
docker exec -it postgres-target psql -U analytics -d analytics
SELECT * FROM stg_crm.customers WHERE email = 'john.doe@example.com';

# Verify SCD Type 2 history (after staging_to_nds auto-triggers and completes)
SELECT customer_id_nk, loyalty_tier, valid_from, valid_to, is_current
FROM nds.customers
WHERE customer_id_nk = '1'
ORDER BY valid_from;

# Expected: 2 records - old record (is_current=FALSE) and new record (is_current=TRUE)
```

### 3. Verify LSET/CET Behavior

```bash
# Check watermarks before and after run
docker exec -it postgres-metadata psql -U metadata_user -d metadata_db

SELECT dag_id, source_system, source_table, lset, cet, last_run_status
FROM metadata.etl_watermark
WHERE source_table = 'customers';

# Verify only new/changed records were processed
SELECT dag_id, source_table, rows_extracted, rows_loaded, run_start_time
FROM metadata.etl_run_log
WHERE source_table = 'customers'
ORDER BY run_start_time DESC
LIMIT 5;
```

## Monitoring and Troubleshooting

### View Logs

```bash
# Airflow logs
docker logs airflow -f

# Specific database logs
docker logs postgres-source-crm
docker logs postgres-target
```

### Common Issues

1. **DAG not appearing in UI**
   - Check Python syntax: `docker exec airflow python /opt/airflow/dags/source_to_staging_dag.py`
   - Check logs: `docker logs airflow | grep ERROR`

2. **Task failures**
   - Check task logs in Airflow UI
   - Verify database connections in Airflow UI → Admin → Connections
   - Ensure all databases are healthy: `docker-compose ps`

3. **No data in staging/NDS/DDS**
   - Verify source data exists
   - Check watermark table for errors
   - Review task execution logs in Airflow UI

### Reset Everything

```bash
# Stop all services
docker-compose down -v

# Remove all volumes (WARNING: deletes all data)
docker volume prune -f

# Restart
docker-compose up -d
```

## Performance Tuning

### Bulk Inserts

The current implementation uses row-by-row inserts. For production, consider:

```python
# Replace in source_to_staging_dag.py:
for record in records:
    target_hook.run(insert_sql, parameters=tuple(record))

# With bulk insert:
target_hook.insert_rows(
    table=staging_table,
    rows=records,
    target_fields=business_columns + ['stg_source_system', 'stg_loaded_at']
)
```

### Parallel Execution

All tasks within each DAG run in parallel by default (no dependencies).

### Schedule Optimization

Adjust schedules in `dags/config/pipeline_config.py`:

```python
SCHEDULE_SOURCE_TO_STAGING = '*/15 * * * *'  # Every 15 minutes
SCHEDULE_STAGING_TO_NDS = '*/30 * * * *'     # Every 30 minutes
SCHEDULE_NDS_TO_DDS = '0 * * * *'            # Every hour
```

## Design Decisions

### Why Event-Driven Execution?

- **Immediate Data Flow**: DAG 2 runs as soon as DAG 1 completes (no waiting for schedule)
- **Efficiency**: Only runs when upstream data changes
- **Data Lineage**: Automatic visualization of data dependencies
- **Better SLAs**: Faster end-to-end completion times
- **Impact Analysis**: See what's affected when source data changes

### Why Keep LSET/CET with Datasets?

- **Datasets**: Control WHEN to run (event-driven triggering)
- **LSET/CET**: Control WHAT to process (incremental data filtering)
- **Together**: Event-driven execution + only process changed records = optimal efficiency
- **Critical for Facts**: Fact tables can have 100M+ rows - can't reprocess all on every run

### Why Full Truncate/Load on Staging?

- **Simplicity**: Easier to reason about and debug
- **Data Freshness**: Ensures staging always reflects latest snapshot
- **Incremental at Source**: LSET/CET ensures only changed data extracted
- **Cost**: Staging tables are typically small after incremental extract

### Why Hash-Based Change Detection?

- **Efficiency**: Single MD5 hash comparison vs column-by-column
- **Consistency**: Deterministic change detection
- **Auditability**: Hash stored for debugging

### Why Natural Keys?

- **Business Meaning**: Natural keys have business significance
- **Source System Tracing**: Easy to trace back to source
- **Multi-Source Support**: Source system prefix prevents collisions

## Next Steps

1. **Add Data Quality Checks**: Implement validators using Airflow Sensors
2. **Add Alerting**: Configure email/Slack notifications on failures
3. **Add dbt Integration**: Use dbt for transformation logic
4. **Add Great Expectations**: Schema and data validation
5. **Add CI/CD**: GitHub Actions for DAG testing
6. **Add Monitoring**: Prometheus + Grafana for metrics

## License

MIT

## Contact

For questions or issues, please open a GitHub issue.

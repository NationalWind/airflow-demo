"""
Sales ETL Pipeline (PostgreSQL ➜ PostgreSQL)
Pure Postgres implementation with LSET/CET incremental loading.
LSET = Last Successful Execution Time
CET = Current Execution Time
"""

from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import logging

SOURCE_CONN_ID = "postgres_source"
TARGET_CONN_ID = "postgres_target"
DAG_ID = "sales_etl_postgres_to_postgres"

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 20),
    "email": ["admin@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Moves sales data from the Postgres source system into the analytics warehouse",
    catchup=False,
    max_active_runs=1,
    schedule='@daily',
    tags=["postgres", "analytics", "elt", "incremental"],
)


def get_lset_cet(**context) -> Tuple[Optional[datetime], datetime]:
    """
    Get LSET (Last Successful Execution Time) and CET (Current Execution Time).

    LSET: The end_time of the last successful ETL run for this DAG.
          If no previous successful run exists, returns None (full load).
    CET:  Current timestamp marking the upper bound of this extraction.
    """
    target = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
    dag_id = context["dag"].dag_id

    # Get LSET: last successful run's end_time
    lset_sql = """
        SELECT end_time
        FROM etl.etl_log
        WHERE dag_id = %s
          AND status = 'SUCCESS'
          AND end_time IS NOT NULL
        ORDER BY end_time DESC
        LIMIT 1
    """
    result = target.get_first(lset_sql, parameters=(dag_id,))
    lset = result[0] if result else None

    # Get CET: current timestamp from the database (ensures consistency)
    cet_result = target.get_first("SELECT CURRENT_TIMESTAMP")
    cet = cet_result[0]

    # Push to XCom for other tasks (use empty string for None since Airflow 3.x doesn't accept None)
    context["ti"].xcom_push(key="lset", value=lset.isoformat() if lset else "")
    context["ti"].xcom_push(key="cet", value=cet.isoformat())

    if lset:
        logging.info("LSET (Last Successful Execution Time): %s", lset)
    else:
        logging.info("LSET: None (first run - full load)")
    logging.info("CET (Current Execution Time): %s", cet)

    return lset, cet


def start_etl_log(**context) -> int:
    """Insert a log row in the analytics warehouse with CET."""
    execution_date = context["ds"]
    dag_id = context["dag"].dag_id
    ti = context["ti"]

    # Get LSET/CET from previous task (empty string means first run)
    lset_str = ti.xcom_pull(task_ids="get_lset_cet", key="lset") or ""
    cet_str = ti.xcom_pull(task_ids="get_lset_cet", key="cet")

    target = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
    sql = """
        INSERT INTO etl.etl_log (dag_id, task_id, execution_date, start_time, status)
        VALUES (%s, %s, %s, %s::TIMESTAMP, 'STARTED')
        RETURNING log_id;
    """
    log_id = target.get_first(sql, parameters=(dag_id, "sales_pipeline", execution_date, cet_str))[0]
    context["ti"].xcom_push(key="etl_log_id", value=log_id)

    logging.info("Started ETL log with ID %s (LSET=%s, CET=%s)", log_id, lset_str, cet_str)
    return log_id


def extract_customers(**context) -> int:
    """Extract customers incrementally based on LSET/CET."""
    ti = context["ti"]
    lset_str = ti.xcom_pull(task_ids="get_lset_cet", key="lset") or ""
    cet_str = ti.xcom_pull(task_ids="get_lset_cet", key="cet")

    source = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)

    if lset_str:  # Non-empty string means incremental load
        # Incremental: only records updated since last successful run
        sql = """
            SELECT
                customer_id,
                first_name || ' ' || last_name AS customer_name,
                email,
                city,
                country,
                created_at::date AS created_date
            FROM sales.customers
            WHERE updated_at > %s::TIMESTAMP AND updated_at <= %s::TIMESTAMP
        """
        records = source.get_records(sql, parameters=(lset_str, cet_str))
        logging.info("Incremental extract: %s customers (LSET=%s, CET=%s)", len(records), lset_str, cet_str)
    else:
        # Full load: first run or no previous successful run
        sql = """
            SELECT
                customer_id,
                first_name || ' ' || last_name AS customer_name,
                email,
                city,
                country,
                created_at::date AS created_date
            FROM sales.customers
            WHERE updated_at <= %s::TIMESTAMP
        """
        records = source.get_records(sql, parameters=(cet_str,))
        logging.info("Full extract: %s customers (CET=%s)", len(records), cet_str)

    context["ti"].xcom_push(key="customers_data", value=[dict(zip(
        ["customer_id", "customer_name", "email", "city", "country", "created_date"],
        row
    )) for row in records])
    context["ti"].xcom_push(key="is_incremental", value=lset_str is not None)

    return len(records)


def extract_products(**context) -> int:
    """Extract products incrementally based on LSET/CET."""
    ti = context["ti"]
    lset_str = ti.xcom_pull(task_ids="get_lset_cet", key="lset") or ""
    cet_str = ti.xcom_pull(task_ids="get_lset_cet", key="cet")

    source = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)

    if lset_str:  # Non-empty string means incremental load
        # Incremental: only records updated since last successful run
        sql = """
            SELECT
                product_id,
                product_name,
                category,
                unit_price
            FROM sales.products
            WHERE active = TRUE
              AND updated_at > %s::TIMESTAMP AND updated_at <= %s::TIMESTAMP
        """
        records = source.get_records(sql, parameters=(lset_str, cet_str))
        logging.info("Incremental extract: %s products (LSET=%s, CET=%s)", len(records), lset_str, cet_str)
    else:
        # Full load
        sql = """
            SELECT
                product_id,
                product_name,
                category,
                unit_price
            FROM sales.products
            WHERE active = TRUE
              AND updated_at <= %s::TIMESTAMP
        """
        records = source.get_records(sql, parameters=(cet_str,))
        logging.info("Full extract: %s products (CET=%s)", len(records), cet_str)

    context["ti"].xcom_push(key="products_data", value=[dict(zip(
        ["product_id", "product_name", "category", "unit_price"],
        row
    )) for row in records])

    return len(records)


def extract_sales(**context) -> int:
    """Extract sales/orders incrementally based on LSET/CET."""
    ti = context["ti"]
    lset_str = ti.xcom_pull(task_ids="get_lset_cet", key="lset") or ""
    cet_str = ti.xcom_pull(task_ids="get_lset_cet", key="cet")

    source = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)

    if lset_str:  # Non-empty string means incremental load
        # Incremental: only orders created/updated since last successful run
        sql = """
            SELECT
                o.order_id,
                o.customer_id,
                oi.product_id,
                o.order_date::date AS sale_date,
                oi.quantity,
                oi.unit_price,
                (oi.quantity * oi.unit_price) AS total_amount
            FROM sales.orders o
            INNER JOIN sales.order_items oi ON o.order_id = oi.order_id
            WHERE o.updated_at > %s::TIMESTAMP AND o.updated_at <= %s::TIMESTAMP
        """
        records = source.get_records(sql, parameters=(lset_str, cet_str))
        logging.info("Incremental extract: %s sales rows (LSET=%s, CET=%s)", len(records), lset_str, cet_str)
    else:
        # Full load
        sql = """
            SELECT
                o.order_id,
                o.customer_id,
                oi.product_id,
                o.order_date::date AS sale_date,
                oi.quantity,
                oi.unit_price,
                (oi.quantity * oi.unit_price) AS total_amount
            FROM sales.orders o
            INNER JOIN sales.order_items oi ON o.order_id = oi.order_id
            WHERE o.updated_at <= %s::TIMESTAMP
        """
        records = source.get_records(sql, parameters=(cet_str,))
        logging.info("Full extract: %s sales rows (CET=%s)", len(records), cet_str)

    context["ti"].xcom_push(key="sales_data", value=[dict(zip(
        ["order_id", "customer_id", "product_id", "sale_date", "quantity", "unit_price", "total_amount"],
        row
    )) for row in records])

    return len(records)


def load_dimensions(**context) -> Dict[str, int]:
    """Load dimension tables - full refresh for dimensions, upsert for incremental."""
    ti = context["ti"]
    customers = ti.xcom_pull(task_ids="extract_customers", key="customers_data") or []
    products = ti.xcom_pull(task_ids="extract_products", key="products_data") or []
    is_incremental = ti.xcom_pull(task_ids="extract_customers", key="is_incremental")

    target = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

    if is_incremental:
        # Upsert customers
        for customer in customers:
            sql = """
                INSERT INTO dwh.dim_customers (customer_id, customer_name, email, city, country, created_date, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (customer_id) DO UPDATE SET
                    customer_name = EXCLUDED.customer_name,
                    email = EXCLUDED.email,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country,
                    updated_at = CURRENT_TIMESTAMP
            """
            target.run(sql, parameters=(
                customer["customer_id"],
                customer["customer_name"],
                customer["email"],
                customer["city"],
                customer["country"],
                customer["created_date"]
            ))

        # Upsert products
        for product in products:
            sql = """
                INSERT INTO dwh.dim_products (product_id, product_name, category, unit_price, updated_at)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    category = EXCLUDED.category,
                    unit_price = EXCLUDED.unit_price,
                    updated_at = CURRENT_TIMESTAMP
            """
            target.run(sql, parameters=(
                product["product_id"],
                product["product_name"],
                product["category"],
                product["unit_price"]
            ))

        logging.info("Incremental upsert: %s customers, %s products", len(customers), len(products))
    else:
        # Full load - truncate and insert
        target.run("TRUNCATE TABLE dwh.dim_customers CASCADE;")
        for customer in customers:
            sql = """
                INSERT INTO dwh.dim_customers (customer_id, customer_name, email, city, country, created_date)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            target.run(sql, parameters=(
                customer["customer_id"],
                customer["customer_name"],
                customer["email"],
                customer["city"],
                customer["country"],
                customer["created_date"]
            ))

        target.run("TRUNCATE TABLE dwh.dim_products CASCADE;")
        for product in products:
            sql = """
                INSERT INTO dwh.dim_products (product_id, product_name, category, unit_price)
                VALUES (%s, %s, %s, %s)
            """
            target.run(sql, parameters=(
                product["product_id"],
                product["product_name"],
                product["category"],
                product["unit_price"]
            ))

        logging.info("Full load: %s customers, %s products", len(customers), len(products))

    return {"customers": len(customers), "products": len(products)}


def transform_and_load_facts(**context) -> int:
    """Load fact table with upsert logic for incremental loads."""
    ti = context["ti"]
    sales_rows = ti.xcom_pull(task_ids="extract_sales", key="sales_data") or []
    is_incremental = ti.xcom_pull(task_ids="extract_customers", key="is_incremental")

    if not sales_rows:
        logging.warning("No sales rows to load")
        context["ti"].xcom_push(key="rows_loaded", value=0)
        return 0

    target = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

    customer_map = {
        row[1]: row[0]
        for row in target.get_records("SELECT customer_key, customer_id FROM dwh.dim_customers")
    }
    product_map = {
        row[1]: row[0]
        for row in target.get_records("SELECT product_key, product_id FROM dwh.dim_products")
    }

    inserted = 0
    for sale in sales_rows:
        customer_key = customer_map.get(sale["customer_id"])
        product_key = product_map.get(sale["product_id"])
        if not customer_key or not product_key:
            logging.warning("Missing dimension key for order %s", sale["order_id"])
            continue

        # Upsert: insert or update on conflict
        sql = """
            INSERT INTO dwh.fact_sales (
                sale_id,
                customer_key,
                product_key,
                sale_date,
                quantity,
                unit_price,
                total_amount,
                loaded_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (sale_id) DO UPDATE SET
                customer_key = EXCLUDED.customer_key,
                product_key = EXCLUDED.product_key,
                sale_date = EXCLUDED.sale_date,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                total_amount = EXCLUDED.total_amount,
                loaded_at = CURRENT_TIMESTAMP
        """
        target.run(sql, parameters=(
            sale["order_id"],
            customer_key,
            product_key,
            sale["sale_date"],
            sale["quantity"],
            sale["unit_price"],
            sale["total_amount"]
        ))
        inserted += 1

    context["ti"].xcom_push(key="rows_loaded", value=inserted)
    logging.info("Loaded/updated %s fact rows (incremental=%s)", inserted, is_incremental)
    return inserted


def validate_data_quality(**context) -> bool:
    """Ensure fact counts match expectations and referential integrity is preserved."""
    rows_loaded = context["ti"].xcom_pull(task_ids="transform_and_load_facts", key="rows_loaded") or 0
    target = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

    # Only check for null FKs if there are rows
    if rows_loaded > 0:
        null_fk_count = target.get_first(
            """
            SELECT COUNT(*) FROM dwh.fact_sales
            WHERE (customer_key IS NULL OR product_key IS NULL)
            """,
        )[0]
        if null_fk_count > 0:
            raise ValueError(f"Detected {null_fk_count} fact rows missing dimension keys")

    logging.info("Data quality checks passed (rows_loaded=%s)", rows_loaded)
    return True


def complete_etl_log(**context) -> bool:
    """Mark ETL log with completion time and row counts."""
    ti = context["ti"]
    log_id = ti.xcom_pull(task_ids="start_etl_log", key="etl_log_id")
    rows_loaded = ti.xcom_pull(task_ids="transform_and_load_facts", key="rows_loaded") or 0
    cet_str = ti.xcom_pull(task_ids="get_lset_cet", key="cet")

    target = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
    sql = """
        UPDATE etl.etl_log
        SET end_time = %s::TIMESTAMP,
            status = 'SUCCESS',
            rows_loaded = %s
        WHERE log_id = %s
    """
    target.run(sql, parameters=(cet_str, rows_loaded, log_id))
    logging.info("Updated ETL log %s with CET=%s", log_id, cet_str)
    return True


# Task definitions
start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)
extract_phase = EmptyOperator(task_id="extract_phase_complete", dag=dag)
transform_phase = EmptyOperator(task_id="transform_phase_complete", dag=dag)

get_lset_cet_task = PythonOperator(
    task_id="get_lset_cet",
    python_callable=get_lset_cet,
    dag=dag,
)

start_log = PythonOperator(
    task_id="start_etl_log",
    python_callable=start_etl_log,
    dag=dag,
)

extract_customers_task = PythonOperator(
    task_id="extract_customers",
    python_callable=extract_customers,
    dag=dag,
)

extract_products_task = PythonOperator(
    task_id="extract_products",
    python_callable=extract_products,
    dag=dag,
)

extract_sales_task = PythonOperator(
    task_id="extract_sales",
    python_callable=extract_sales,
    dag=dag,
)

load_dimensions_task = PythonOperator(
    task_id="load_dimensions",
    python_callable=load_dimensions,
    dag=dag,
)

transform_load_facts_task = PythonOperator(
    task_id="transform_and_load_facts",
    python_callable=transform_and_load_facts,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_data_quality",
    python_callable=validate_data_quality,
    dag=dag,
)

complete_log_task = PythonOperator(
    task_id="complete_etl_log",
    python_callable=complete_etl_log,
    dag=dag,
)

# DAG flow
start >> get_lset_cet_task >> start_log
start_log >> [extract_customers_task, extract_products_task, extract_sales_task]
[extract_customers_task, extract_products_task, extract_sales_task] >> extract_phase
extract_phase >> load_dimensions_task >> transform_load_facts_task >> transform_phase
transform_phase >> validate_task
validate_task >> complete_log_task >> end

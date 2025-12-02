"""
Sales ETL Pipeline (PostgreSQL ➜ PostgreSQL)
Pure Postgres implementation that stages data from a transactional source schema
into the analytics warehouse without touching SQL Server or SSIS.
"""

from datetime import datetime, timedelta
from typing import Dict

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import logging

SOURCE_CONN_ID = "postgres_source"
TARGET_CONN_ID = "postgres_target"

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
    dag_id="sales_etl_postgres_to_postgres",
    default_args=default_args,
    description="Moves sales data from the Postgres source system into the analytics warehouse",
    catchup=False,
    max_active_runs=1,
    tags=["postgres", "analytics", "elt"],
)


def start_etl_log(**context) -> int:
    """Insert a log row in the analytics warehouse."""
    execution_date = context["ds"]
    dag_id = context["dag"].dag_id
    target = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
    sql = """
        INSERT INTO etl.etl_log (dag_id, task_id, execution_date, start_time, status)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP, 'STARTED')
        RETURNING log_id;
    """
    log_id = target.get_first(sql, parameters=(dag_id, "sales_pipeline", execution_date))[0]
    context["ti"].xcom_push(key="etl_log_id", value=log_id)
    logging.info("Started ETL log with ID %s", log_id)
    return log_id


def extract_customers(**context) -> int:
    """Extract customers from the transactional source."""
    source = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
    sql = """
        SELECT 
            customer_id,
            first_name || ' ' || last_name AS customer_name,
            email,
            city,
            country,
            created_at::date AS created_date
        FROM sales.customers
    """
    records = source.get_records(sql)
    context["ti"].xcom_push(key="customers_data", value=[dict(zip(
        ["customer_id", "customer_name", "email", "city", "country", "created_date"],
        row
    )) for row in records])
    logging.info("Extracted %s customers", len(records))
    return len(records)


def extract_products(**context) -> int:
    """Extract products from the transactional source."""
    source = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
    sql = """
        SELECT 
            product_id,
            product_name,
            category,
            unit_price
        FROM sales.products
        WHERE active = TRUE
    """
    records = source.get_records(sql)
    context["ti"].xcom_push(key="products_data", value=[dict(zip(
        ["product_id", "product_name", "category", "unit_price"],
        row
    )) for row in records])
    logging.info("Extracted %s products", len(records))
    return len(records)


def extract_sales(**context) -> int:
    """Extract order line items for the execution date."""
    execution_date = context["ds"]
    source = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
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
        WHERE o.order_date = %s
    """
    records = source.get_records(sql, parameters=(execution_date,))
    context["ti"].xcom_push(key="sales_data", value=[dict(zip(
        ["order_id", "customer_id", "product_id", "sale_date", "quantity", "unit_price", "total_amount"],
        row
    )) for row in records])
    logging.info("Extracted %s sales rows for %s", len(records), execution_date)
    return len(records)


def load_dimensions(**context) -> Dict[str, int]:
    """Overwrite dimension tables in the warehouse."""
    ti = context["ti"]
    customers = ti.xcom_pull(task_ids="extract_customers", key="customers_data") or []
    products = ti.xcom_pull(task_ids="extract_products", key="products_data") or []
    target = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

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

    logging.info("Loaded %s customers and %s products", len(customers), len(products))
    return {"customers": len(customers), "products": len(products)}


def transform_and_load_facts(**context) -> int:
    """Map source rows to warehouse surrogate keys and populate fact_sales."""
    execution_date = context["ds"]
    sales_rows = context["ti"].xcom_pull(task_ids="extract_sales", key="sales_data") or []
    if not sales_rows:
        logging.warning("No sales rows to load for %s", execution_date)
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

    target.run("DELETE FROM dwh.fact_sales WHERE sale_date = %s", parameters=(execution_date,))

    inserted = 0
    for sale in sales_rows:
        customer_key = customer_map.get(sale["customer_id"])
        product_key = product_map.get(sale["product_id"])
        if not customer_key or not product_key:
            logging.warning("Missing dimension key for order %s", sale["order_id"])
            continue
        sql = """
            INSERT INTO dwh.fact_sales (
                sale_id,
                customer_key,
                product_key,
                sale_date,
                quantity,
                unit_price,
                total_amount
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
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
    logging.info("Loaded %s fact rows", inserted)
    return inserted


def create_daily_summary(**context) -> bool:
    """Aggregate daily KPIs inside the warehouse."""
    execution_date = context["ds"]
    target = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
    sql = """
        WITH summary AS (
            SELECT
                COUNT(DISTINCT customer_key) AS unique_customers,
                COUNT(*) AS total_orders,
                SUM(quantity) AS total_items_sold,
                SUM(total_amount) AS total_revenue,
                AVG(total_amount) AS avg_order_value,
                MIN(total_amount) AS min_order_value,
                MAX(total_amount) AS max_order_value
            FROM dwh.fact_sales
            WHERE sale_date = %s
        ),
        top_product AS (
            SELECT
                dp.product_id,
                dp.product_name,
                SUM(fs.total_amount) AS revenue
            FROM dwh.fact_sales fs
            INNER JOIN dwh.dim_products dp ON fs.product_key = dp.product_key
            WHERE fs.sale_date = %s
            GROUP BY dp.product_id, dp.product_name
            ORDER BY revenue DESC
            LIMIT 1
        )
        INSERT INTO dwh.sales_summary_daily (
            summary_date,
            unique_customers,
            total_orders,
            total_items_sold,
            total_revenue,
            avg_order_value,
            min_order_value,
            max_order_value,
            top_product_id,
            top_product_name,
            top_product_revenue
        )
        SELECT
            %s,
            s.unique_customers,
            s.total_orders,
            s.total_items_sold,
            s.total_revenue,
            s.avg_order_value,
            s.min_order_value,
            s.max_order_value,
            tp.product_id,
            tp.product_name,
            tp.revenue
        FROM summary s
        CROSS JOIN top_product tp
        ON CONFLICT (summary_date) DO UPDATE SET
            unique_customers = EXCLUDED.unique_customers,
            total_orders = EXCLUDED.total_orders,
            total_items_sold = EXCLUDED.total_items_sold,
            total_revenue = EXCLUDED.total_revenue,
            avg_order_value = EXCLUDED.avg_order_value,
            min_order_value = EXCLUDED.min_order_value,
            max_order_value = EXCLUDED.max_order_value,
            top_product_id = EXCLUDED.top_product_id,
            top_product_name = EXCLUDED.top_product_name,
            top_product_revenue = EXCLUDED.top_product_revenue,
            updated_at = CURRENT_TIMESTAMP;
    """
    target.run(sql, parameters=(execution_date, execution_date, execution_date))
    logging.info("Daily summary refreshed for %s", execution_date)
    return True


def validate_data_quality(**context) -> bool:
    """Ensure fact counts match expectations and referential integrity is preserved."""
    execution_date = context["ds"]
    rows_loaded = context["ti"].xcom_pull(task_ids="transform_and_load_facts", key="rows_loaded") or 0
    target = PostgresHook(postgres_conn_id=TARGET_CONN_ID)

    fact_count = target.get_first(
        "SELECT COUNT(*) FROM dwh.fact_sales WHERE sale_date = %s",
        parameters=(execution_date,)
    )[0]
    if fact_count != rows_loaded:
        raise ValueError(f"Data quality check failed: expected {rows_loaded}, found {fact_count}")

    null_fk_count = target.get_first(
        """
        SELECT COUNT(*) FROM dwh.fact_sales
        WHERE (customer_key IS NULL OR product_key IS NULL) AND sale_date = %s
        """,
        parameters=(execution_date,)
    )[0]
    if null_fk_count > 0:
        raise ValueError(f"Detected {null_fk_count} fact rows missing dimension keys")

    summary_exists = target.get_first(
        "SELECT COUNT(*) FROM dwh.sales_summary_daily WHERE summary_date = %s",
        parameters=(execution_date,)
    )[0]
    if summary_exists == 0:
        raise ValueError("Daily summary table not updated")

    logging.info("Data quality checks passed")
    return True


def complete_etl_log(**context) -> bool:
    """Mark ETL log with completion time and row counts."""
    ti = context["ti"]
    log_id = ti.xcom_pull(task_ids="start_etl_log", key="etl_log_id")
    rows_loaded = ti.xcom_pull(task_ids="transform_and_load_facts", key="rows_loaded") or 0
    target = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
    sql = """
        UPDATE etl.etl_log
        SET end_time = CURRENT_TIMESTAMP,
            status = 'SUCCESS',
            rows_loaded = %s
        WHERE log_id = %s
    """
    target.run(sql, parameters=(rows_loaded, log_id))
    logging.info("Updated ETL log %s", log_id)
    return True


def capture_summary(**context) -> Dict[str, float]:
    """Fetch summary stats for downstream notifications."""
    execution_date = context["ds"]
    target = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
    sql = """
        SELECT 
            summary_date,
            total_orders,
            total_revenue,
            unique_customers,
            top_product_name,
            top_product_revenue
        FROM dwh.sales_summary_daily
        WHERE summary_date = %s
    """
    result = target.get_first(sql, parameters=(execution_date,))
    summary = {}
    if result:
        summary = {
            "date": str(result[0]),
            "total_orders": int(result[1]),
            "total_revenue": float(result[2]),
            "unique_customers": int(result[3]),
            "top_product": result[4],
            "top_revenue": float(result[5]) if result[5] is not None else 0.0,
        }
    context["ti"].xcom_push(key="summary_stats", value=summary)
    logging.info("Summary snapshot: %s", summary)
    return summary


start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)
extract_phase = EmptyOperator(task_id="extract_phase_complete", dag=dag)
transform_phase = EmptyOperator(task_id="transform_phase_complete", dag=dag)

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

create_summary_task = PythonOperator(
    task_id="create_daily_summary",
    python_callable=create_daily_summary,
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

capture_summary_task = PythonOperator(
    task_id="capture_summary_stats",
    python_callable=capture_summary,
    dag=dag,
)

start >> start_log
start_log >> [extract_customers_task, extract_products_task, extract_sales_task]
[extract_customers_task, extract_products_task, extract_sales_task] >> extract_phase
extract_phase >> load_dimensions_task >> transform_load_facts_task >> transform_phase
transform_phase >> create_summary_task >> validate_task
validate_task >> [complete_log_task, capture_summary_task] >> end

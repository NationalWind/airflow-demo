"""
Sales ETL Pipeline - SQL Server to PostgreSQL
Extract data from SQL Server, Transform, and Load to PostgreSQL Data Warehouse
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
import logging

# ============================================
# DAG CONFIGURATION
# ============================================
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 20),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1),
}

dag = DAG(
    dag_id='sales_etl_sqlserver_to_postgres',
    default_args=default_args,
    description='Complete ETL Pipeline: SQL Server → Transform → PostgreSQL DWH',
    catchup=False,
    max_active_runs=1,
    tags=['production', 'sales', 'etl', 'sqlserver', 'postgres']
)

# ============================================
# TASK 1: START ETL LOG
# ============================================
def start_etl_log(**context):
    """Initialize ETL log in target database"""
    execution_date = context['ds']
    dag_id = context['dag'].dag_id
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_target')
    
    sql = """
        INSERT INTO etl.etl_log (dag_id, task_id, execution_date, start_time, status)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP, 'STARTED')
        RETURNING log_id;
    """
    
    result = postgres_hook.get_first(sql, parameters=(dag_id, 'etl_pipeline', execution_date))
    log_id = result[0]
    
    # Push log_id to XCom
    context['ti'].xcom_push(key='etl_log_id', value=log_id)
    
    logging.info(f"✅ Started ETL log with ID: {log_id}")
    return log_id

start_log = PythonOperator(
    task_id='start_etl_log',
    python_callable=start_etl_log,
    dag=dag
)

# ============================================
# TASK 2: EXTRACT CUSTOMERS
# ============================================
def extract_customers(**context):
    """Extract customers from SQL Server"""
    logging.info("Extracting customers from SQL Server...")
    
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_source')
    
    sql = """
        SELECT 
            customer_id,
            customer_name,
            email,
            city,
            country,
            created_date
        FROM Customers
    """
    
    df = mssql_hook.get_pandas_df(sql)
    
    # Convert to dict for XCom
    data = df.to_dict('records')
    context['ti'].xcom_push(key='customers_data', value=data)
    
    logging.info(f"✅ Extracted {len(df)} customers")
    return len(df)

extract_customers_task = PythonOperator(
    task_id='extract_customers',
    python_callable=extract_customers,
    dag=dag
)

# ============================================
# TASK 3: EXTRACT PRODUCTS
# ============================================
def extract_products(**context):
    """Extract products from SQL Server"""
    logging.info("Extracting products from SQL Server...")
    
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_source')
    
    sql = """
        SELECT 
            product_id,
            product_name,
            category,
            unit_price
        FROM Products
    """
    
    df = mssql_hook.get_pandas_df(sql)
    data = df.to_dict('records')
    context['ti'].xcom_push(key='products_data', value=data)
    
    logging.info(f"✅ Extracted {len(df)} products")
    return len(df)

extract_products_task = PythonOperator(
    task_id='extract_products',
    python_callable=extract_products,
    dag=dag
)

# ============================================
# TASK 4: EXTRACT SALES
# ============================================
def extract_sales(**context):
    """Extract sales transactions for execution date"""
    execution_date = context['ds']
    logging.info(f"Extracting sales for {execution_date}...")
    
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_source')
    
    # Use stored procedure
    sql = f"""
        EXEC sp_GetSalesByDateRange 
            @start_date = '{execution_date}', 
            @end_date = '{execution_date}'
    """
    
    df = mssql_hook.get_pandas_df(sql)
    
    if df.empty:
        logging.warning(f"⚠️ No sales data for {execution_date}")
        return 0
    
    data = df.to_dict('records')
    context['ti'].xcom_push(key='sales_data', value=data)
    
    logging.info(f"✅ Extracted {len(df)} sales records")
    return len(df)

extract_sales_task = PythonOperator(
    task_id='extract_sales',
    python_callable=extract_sales,
    dag=dag
)

# ============================================
# TASK 5: LOAD DIMENSIONS
# ============================================
def load_dimensions(**context):
    """Load dimension tables (SCD Type 1 - Overwrite)"""
    ti = context['ti']
    
    customers = ti.xcom_pull(task_ids='extract_customers', key='customers_data')
    products = ti.xcom_pull(task_ids='extract_products', key='products_data')
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_target')
    
    # Load Customers
    logging.info("Loading customers dimension...")
    postgres_hook.run("TRUNCATE TABLE dwh.dim_customers CASCADE;")
    
    for customer in customers:
        sql = """
            INSERT INTO dwh.dim_customers (customer_id, customer_name, email, city, country, created_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        postgres_hook.run(sql, parameters=(
            customer['customer_id'],
            customer['customer_name'],
            customer['email'],
            customer['city'],
            customer['country'],
            customer['created_date']
        ))
    
    # Load Products
    logging.info("Loading products dimension...")
    postgres_hook.run("TRUNCATE TABLE dwh.dim_products CASCADE;")
    
    for product in products:
        sql = """
            INSERT INTO dwh.dim_products (product_id, product_name, category, unit_price)
            VALUES (%s, %s, %s, %s)
        """
        postgres_hook.run(sql, parameters=(
            product['product_id'],
            product['product_name'],
            product['category'],
            product['unit_price']
        ))
    
    logging.info(f"✅ Loaded {len(customers)} customers and {len(products)} products")
    return {'customers': len(customers), 'products': len(products)}

load_dimensions_task = PythonOperator(
    task_id='load_dimensions',
    python_callable=load_dimensions,
    dag=dag
)

# ============================================
# TASK 6: TRANSFORM AND LOAD FACTS
# ============================================
def transform_and_load_facts(**context):
    """Transform and load fact table"""
    ti = context['ti']
    execution_date = context['ds']
    
    sales_data = ti.xcom_pull(task_ids='extract_sales', key='sales_data')
    
    if not sales_data:
        logging.warning("No sales data to load")
        return 0
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_target')
    
    # Get dimension keys mapping
    customer_keys = {}
    product_keys = {}
    
    customers_sql = "SELECT customer_key, customer_id FROM dwh.dim_customers"
    customers = postgres_hook.get_records(customers_sql)
    for row in customers:
        customer_keys[row[1]] = row[0]
    
    products_sql = "SELECT product_key, product_id FROM dwh.dim_products"
    products = postgres_hook.get_records(products_sql)
    for row in products:
        product_keys[row[1]] = row[0]
    
    # Delete existing records for this date
    delete_sql = "DELETE FROM dwh.fact_sales WHERE sale_date = %s"
    postgres_hook.run(delete_sql, parameters=(execution_date,))
    
    # Insert fact records
    loaded_count = 0
    print("SAMPLE SALES DATA:", sales_data[0])
    for sale in sales_data:
        # Find dimension keys
        customer_key = customer_keys.get(sale['customer_id'])
        product_key = product_keys.get(sale['product_id'])
        
        if not customer_key or not product_key:
            logging.warning(f"Missing dimension key for sale_id: {sale['sale_id']}")
            continue
        
        sql = """
            INSERT INTO dwh.fact_sales 
            (sale_id, customer_key, product_key, sale_date, quantity, unit_price, total_amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        postgres_hook.run(sql, parameters=(
            sale['sale_id'],
            customer_key,
            product_key,
            sale['sale_date'],
            sale['quantity'],
            sale['unit_price'],
            sale['total_amount']
        ))
        loaded_count += 1
    
    # Push to XCom
    ti.xcom_push(key='rows_loaded', value=loaded_count)
    
    logging.info(f"✅ Loaded {loaded_count} fact records")
    return loaded_count

transform_load_facts_task = PythonOperator(
    task_id='transform_and_load_facts',
    python_callable=transform_and_load_facts,
    dag=dag
)

def create_daily_summary(**context):
    """Create aggregated daily summary (safe PostgreSQL version)"""
    execution_date = context['ds']
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_target')
    
    sql = """
        WITH top_product AS (
            SELECT dp.product_id, dp.product_name, SUM(fs2.total_amount) as revenue
            FROM dwh.fact_sales fs2
            INNER JOIN dwh.dim_products dp ON fs2.product_key = dp.product_key
            WHERE fs2.sale_date = %s
            GROUP BY dp.product_id, dp.product_name
            ORDER BY revenue DESC
            LIMIT 1
        ),
        summary_agg AS (
            SELECT
                COUNT(DISTINCT customer_key) as unique_customers,
                COUNT(*) as total_orders,
                SUM(quantity) as total_items_sold,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_order_value,
                MIN(total_amount) as min_order_value,
                MAX(total_amount) as max_order_value
            FROM dwh.fact_sales
            WHERE sale_date = %s
        )
        INSERT INTO dwh.sales_summary_daily (
            summary_date, unique_customers, total_orders, total_items_sold,
            total_revenue, avg_order_value, min_order_value, max_order_value,
            top_product_id, top_product_name, top_product_revenue
        )
        SELECT 
            %s as summary_date,
            sa.unique_customers,
            sa.total_orders,
            sa.total_items_sold,
            sa.total_revenue,
            sa.avg_order_value,
            sa.min_order_value,
            sa.max_order_value,
            tp.product_id,
            tp.product_name,
            tp.revenue
        FROM summary_agg sa
        CROSS JOIN top_product tp
        ON CONFLICT (summary_date) 
        DO UPDATE SET
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
    
    postgres_hook.run(sql, parameters=(execution_date, execution_date, execution_date))
    
    logging.info(f"✅ Created daily summary for {execution_date}")
    return True

create_summary_task = PythonOperator(
    task_id='create_daily_summary',
    python_callable=create_daily_summary,
    dag=dag
)

# ============================================
# TASK 8: VALIDATE DATA QUALITY
# ============================================
def validate_data_quality(**context):
    """Validate loaded data"""
    execution_date = context['ds']
    ti = context['ti']
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_target')
    
    # Check 1: Fact count matches source
    rows_loaded = ti.xcom_pull(task_ids='transform_and_load_facts', key='rows_loaded')
    
    fact_count_sql = "SELECT COUNT(*) FROM dwh.fact_sales WHERE sale_date = %s"
    fact_count = postgres_hook.get_first(fact_count_sql, parameters=(execution_date,))[0]
    
    if fact_count != rows_loaded:
        raise ValueError(f"Data quality check failed: Expected {rows_loaded}, found {fact_count}")
    
    # Check 2: No null foreign keys
    null_check_sql = """
        SELECT COUNT(*) FROM dwh.fact_sales 
        WHERE sale_date = %s AND (customer_key IS NULL OR product_key IS NULL)
    """
    null_count = postgres_hook.get_first(null_check_sql, parameters=(execution_date,))[0]
    
    if null_count > 0:
        raise ValueError(f"Found {null_count} records with null foreign keys")
    
    # Check 3: Summary exists
    summary_check_sql = "SELECT COUNT(*) FROM dwh.sales_summary_daily WHERE summary_date = %s"
    summary_count = postgres_hook.get_first(summary_check_sql, parameters=(execution_date,))[0]
    
    if summary_count == 0:
        raise ValueError("Daily summary not created")
    
    logging.info("✅ All data quality checks passed")
    return True

validate_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag
)

# ============================================
# TASK 9: UPDATE ETL LOG - SUCCESS
# ============================================
def complete_etl_log(**context):
    """Mark ETL as completed successfully"""
    ti = context['ti']
    log_id = ti.xcom_pull(task_ids='start_etl_log', key='etl_log_id')
    rows_loaded = ti.xcom_pull(task_ids='transform_and_load_facts', key='rows_loaded')
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_target')
    
    sql = """
        UPDATE etl.etl_log
        SET end_time = CURRENT_TIMESTAMP,
            status = 'SUCCESS',
            rows_loaded = %s
        WHERE log_id = %s
    """
    
    postgres_hook.run(sql, parameters=(rows_loaded, log_id))
    
    logging.info(f"✅ ETL completed successfully. Log ID: {log_id}")
    return True

complete_log_task = PythonOperator(
    task_id='complete_etl_log',
    python_callable=complete_etl_log,
    dag=dag
)

# ============================================
# TASK 10: SEND SUCCESS NOTIFICATION
# ============================================
def get_summary_stats(**context):
    """Get summary statistics for email"""
    execution_date = context['ds']
    postgres_hook = PostgresHook(postgres_conn_id='postgres_target')
    
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
    
    result = postgres_hook.get_first(sql, parameters=(execution_date,))
    
    if result:
        stats = {
            'date': result[0],
            'total_orders': result[1],
            'total_revenue': float(result[2]),
            'unique_customers': result[3],
            'top_product': result[4],
            'top_revenue': float(result[5])
        }
        context['ti'].xcom_push(key='summary_stats', value=stats)
        return stats
    return None

get_stats_task = PythonOperator(
    task_id='get_summary_stats',
    python_callable=get_summary_stats,
    dag=dag
)

# ============================================
# TASK DEPENDENCIES
# ============================================

# Start
start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

# Extract Phase (Parallel)
extract_phase = EmptyOperator(task_id='extract_phase_complete', dag=dag)

# Transform Phase
transform_phase = EmptyOperator(task_id='transform_phase_complete', dag=dag)

# Pipeline Flow
start >> start_log

start_log >> [extract_customers_task, extract_products_task, extract_sales_task]

[extract_customers_task, extract_products_task, extract_sales_task] >> extract_phase

extract_phase >> load_dimensions_task >> transform_load_facts_task >> transform_phase

transform_phase >> create_summary_task >> validate_task

validate_task >> [complete_log_task, get_stats_task]

[complete_log_task, get_stats_task] >> end
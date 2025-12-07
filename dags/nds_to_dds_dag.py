"""
DAG 3: NDS to DDS Layer
Dimensional modeling - load star schema (dimensions and facts) from NDS.
Event-driven: Triggered when NDS datasets are updated.
Uses LSET/CET for incremental fact loading.
"""

from datetime import datetime, timedelta
from typing import Dict, List

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

import sys
sys.path.append('/opt/airflow')

from plugins.hooks.watermark_hook import WatermarkHook
from dags.config.pipeline_config import (
    CONN_TARGET, CONN_METADATA,
    DEFAULT_ARGS
)
from dags.datasets_config import (
    # Inputs: NDS datasets (triggers)
    DATASET_NDS_CUSTOMERS,
    DATASET_NDS_PRODUCTS,
    DATASET_NDS_ORDERS,
    DATASET_NDS_ORDER_ITEMS,
    # Outputs: DDS datasets (produced)
    DATASET_DDS_DIM_CUSTOMERS,
    DATASET_DDS_DIM_PRODUCTS,
    DATASET_DDS_DIM_DATE,
    DATASET_DDS_FACT_SALES,
)


@dag(
    dag_id='nds_to_dds',
    default_args=DEFAULT_ARGS,
    description='Event-driven dimensional modeling triggered by NDS updates',
    # EVENT-DRIVEN: Triggered when NDS assets are updated
    schedule=[
        DATASET_NDS_CUSTOMERS,
        DATASET_NDS_PRODUCTS,
        DATASET_NDS_ORDERS,
        DATASET_NDS_ORDER_ITEMS,
    ],
    start_date=datetime(2024, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'dds', 'dimensional', 'star-schema', 'event-driven'],
)
def nds_to_dds_pipeline():
    """
    NDS to DDS ETL Pipeline.

    Flow:
    1. Verify database connections
    2. Load dimension tables (dim_customers, dim_products, dim_date)
    3. Load fact table (fact_sales) with dimension lookups
    4. Use LSET/CET for incremental fact loading
    """

    @task
    def verify_database_connections(**context) -> Dict[str, str]:
        """
        Verify all required database connections are working.

        Returns:
            Dictionary with connection test results
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        results = {}
        connections = {
            'Target': CONN_TARGET,
            'Metadata': CONN_METADATA
        }

        for name, conn_id in connections.items():
            try:
                hook = PostgresHook(postgres_conn_id=conn_id)
                result = hook.get_first("SELECT 1")
                if result and result[0] == 1:
                    results[name] = 'OK'
                    print(f"✓ {name} database connection successful")
                else:
                    results[name] = 'FAILED'
                    print(f"✗ {name} database connection failed")
            except Exception as e:
                results[name] = f'ERROR: {str(e)}'
                print(f"✗ {name} database connection error: {str(e)}")
                raise

        return results

    @task
    def load_dimension_customers(**context) -> Dict[str, int]:
        """
        Load dim_customers from nds.customers with SCD Type 2 logic.
        Tracks historical changes by comparing hash keys.
        """
        dag_id = context['dag'].dag_id
        execution_date = context.get('ds') or datetime.now().strftime('%Y-%m-%d')

        target_hook = PostgresHook(postgres_conn_id=CONN_TARGET)
        watermark_hook = WatermarkHook(postgres_conn_id=CONN_METADATA)

        # Get LSET/CET
        lset, cet = watermark_hook.get_watermark(
            dag_id=dag_id,
            source_system='nds',
            source_table='customers',
            target_layer='dds'
        )

        print(f"[dim_customers] LSET: {lset}, CET: {cet}")

        # SCD Type 2: Expire old records and insert new versions
        scd_sql = """
            WITH source_data AS (
                SELECT
                    customer_nk,
                    first_name,
                    last_name,
                    email,
                    phone,
                    date_of_birth,
                    city,
                    state,
                    country,
                    postal_code,
                    customer_segment,
                    loyalty_tier,
                    source_system,
                    source_created_at,
                    source_updated_at,
                    MD5(CONCAT_WS('|',
                        COALESCE(first_name, ''),
                        COALESCE(last_name, ''),
                        COALESCE(email, ''),
                        COALESCE(phone, ''),
                        COALESCE(date_of_birth::TEXT, ''),
                        COALESCE(city, ''),
                        COALESCE(state, ''),
                        COALESCE(country, ''),
                        COALESCE(postal_code, ''),
                        COALESCE(customer_segment, ''),
                        COALESCE(loyalty_tier, '')
                    )) AS hash_key
                FROM nds.customers
            ),
            changed_records AS (
                SELECT s.*
                FROM source_data s
                LEFT JOIN dds.dim_customers d
                    ON s.customer_nk = d.customer_nk
                    AND d.is_current = TRUE
                WHERE d.customer_nk IS NULL  -- New record
                   OR d.hash_key != s.hash_key  -- Changed record
            ),
            expire_old AS (
                UPDATE dds.dim_customers d
                SET
                    valid_to = CURRENT_TIMESTAMP,
                    is_current = FALSE
                FROM changed_records c
                WHERE d.customer_nk = c.customer_nk
                  AND d.is_current = TRUE
                  AND d.hash_key != c.hash_key
            )
            INSERT INTO dds.dim_customers (
                customer_nk,
                first_name,
                last_name,
                email,
                phone,
                date_of_birth,
                city,
                state,
                country,
                postal_code,
                customer_segment,
                loyalty_tier,
                source_system,
                source_created_at,
                source_updated_at,
                valid_from,
                valid_to,
                is_current,
                hash_key,
                dds_loaded_at
            )
            SELECT
                customer_nk,
                first_name,
                last_name,
                email,
                phone,
                date_of_birth,
                city,
                state,
                country,
                postal_code,
                customer_segment,
                loyalty_tier,
                source_system,
                source_created_at,
                source_updated_at,
                CURRENT_TIMESTAMP AS valid_from,
                NULL AS valid_to,
                TRUE AS is_current,
                hash_key,
                CURRENT_TIMESTAMP AS dds_loaded_at
            FROM changed_records
        """

        target_hook.run(scd_sql)

        # Get counts
        total_result = target_hook.get_first("SELECT COUNT(*) FROM dds.dim_customers WHERE is_current = TRUE")
        total_count = total_result[0] if total_result else 0

        print(f"[dim_customers] Total current records in dimension: {total_count}")

        # Update watermark
        watermark_hook.update_watermark(
            dag_id=dag_id,
            source_system='nds',
            source_table='customers',
            target_layer='dds',
            target_table='dim_customers',
            lset=lset,
            cet=cet,
            status='SUCCESS',
            rows_extracted=total_count,
            rows_loaded=total_count
        )

        # Log run
        watermark_hook.log_etl_run(
            dag_id=dag_id,
            dag_run_id=context['dag_run'].run_id,
            task_id=context['task'].task_id,
            source_system='nds',
            source_table='customers',
            target_layer='dds',
            target_table='dim_customers',
            execution_date=execution_date,
            status='SUCCESS',
            lset=lset,
            cet=cet,
            rows_extracted=total_count,
            rows_loaded=total_count
        )

        return {'dimension': 'dim_customers', 'total_records': total_count}

    @task
    def load_dimension_products(**context) -> Dict[str, int]:
        """
        Load dim_products from nds.products with SCD Type 2 logic.
        Tracks historical changes by comparing hash keys.
        """
        dag_id = context['dag'].dag_id
        execution_date = context.get('ds') or datetime.now().strftime('%Y-%m-%d')

        target_hook = PostgresHook(postgres_conn_id=CONN_TARGET)
        watermark_hook = WatermarkHook(postgres_conn_id=CONN_METADATA)

        # Get LSET/CET
        lset, cet = watermark_hook.get_watermark(
            dag_id=dag_id,
            source_system='nds',
            source_table='products',
            target_layer='dds'
        )

        print(f"[dim_products] LSET: {lset}, CET: {cet}")

        # SCD Type 2: Expire old records and insert new versions
        scd_sql = """
            WITH source_data AS (
                SELECT
                    product_nk,
                    sku,
                    product_name,
                    category,
                    subcategory,
                    brand,
                    unit_price,
                    cost_price,
                    weight_kg,
                    active,
                    source_system,
                    source_created_at,
                    source_updated_at,
                    MD5(CONCAT_WS('|',
                        COALESCE(sku, ''),
                        COALESCE(product_name, ''),
                        COALESCE(category, ''),
                        COALESCE(subcategory, ''),
                        COALESCE(brand, ''),
                        COALESCE(unit_price::TEXT, ''),
                        COALESCE(cost_price::TEXT, ''),
                        COALESCE(weight_kg::TEXT, ''),
                        COALESCE(active::TEXT, '')
                    )) AS hash_key
                FROM nds.products
            ),
            changed_records AS (
                SELECT s.*
                FROM source_data s
                LEFT JOIN dds.dim_products d
                    ON s.product_nk = d.product_nk
                    AND d.is_current = TRUE
                WHERE d.product_nk IS NULL  -- New record
                   OR d.hash_key != s.hash_key  -- Changed record
            ),
            expire_old AS (
                UPDATE dds.dim_products d
                SET
                    valid_to = CURRENT_TIMESTAMP,
                    is_current = FALSE
                FROM changed_records c
                WHERE d.product_nk = c.product_nk
                  AND d.is_current = TRUE
                  AND d.hash_key != c.hash_key
            )
            INSERT INTO dds.dim_products (
                product_nk,
                sku,
                product_name,
                category,
                subcategory,
                brand,
                unit_price,
                cost_price,
                weight_kg,
                active,
                source_system,
                source_created_at,
                source_updated_at,
                valid_from,
                valid_to,
                is_current,
                hash_key,
                dds_loaded_at
            )
            SELECT
                product_nk,
                sku,
                product_name,
                category,
                subcategory,
                brand,
                unit_price,
                cost_price,
                weight_kg,
                active,
                source_system,
                source_created_at,
                source_updated_at,
                CURRENT_TIMESTAMP AS valid_from,
                NULL AS valid_to,
                TRUE AS is_current,
                hash_key,
                CURRENT_TIMESTAMP AS dds_loaded_at
            FROM changed_records
        """

        target_hook.run(scd_sql)

        # Get counts
        total_result = target_hook.get_first("SELECT COUNT(*) FROM dds.dim_products WHERE is_current = TRUE")
        total_count = total_result[0] if total_result else 0

        print(f"[dim_products] Total current records in dimension: {total_count}")

        # Update watermark
        watermark_hook.update_watermark(
            dag_id=dag_id,
            source_system='nds',
            source_table='products',
            target_layer='dds',
            target_table='dim_products',
            lset=lset,
            cet=cet,
            status='SUCCESS',
            rows_extracted=total_count,
            rows_loaded=total_count
        )

        # Log run
        watermark_hook.log_etl_run(
            dag_id=dag_id,
            dag_run_id=context['dag_run'].run_id,
            task_id=context['task'].task_id,
            source_system='nds',
            source_table='products',
            target_layer='dds',
            target_table='dim_products',
            execution_date=execution_date,
            status='SUCCESS',
            lset=lset,
            cet=cet,
            rows_extracted=total_count,
            rows_loaded=total_count
        )

        return {'dimension': 'dim_products', 'total_records': total_count}

    @task
    def verify_dimension_date(**context) -> Dict[str, int]:
        """
        Verify dim_date dimension table.
        Note: dim_date is pre-populated by the database init script (not generated by DAG).
        This task just verifies the data exists.
        """
        target_hook = PostgresHook(postgres_conn_id=CONN_TARGET)

        # Get counts to verify dim_date is populated
        total_result = target_hook.get_first("SELECT COUNT(*) FROM dds.dim_date")
        total_count = total_result[0] if total_result else 0

        print(f"[dim_date] Pre-populated dimension has {total_count} records")

        if total_count == 0:
            raise ValueError("dim_date is empty! Run the database init script to populate it.")

        return {'dimension': 'dim_date', 'total_records': total_count}

    @task
    def load_fact_sales(**context) -> Dict[str, int]:
        """
        Load fact_sales from nds.order_items with dimension lookups.
        Uses LSET/CET for incremental loading.
        """
        dag_id = context['dag'].dag_id
        execution_date = context.get('ds') or datetime.now().strftime('%Y-%m-%d')

        target_hook = PostgresHook(postgres_conn_id=CONN_TARGET)
        watermark_hook = WatermarkHook(postgres_conn_id=CONN_METADATA)

        # Get LSET/CET
        lset, cet = watermark_hook.get_watermark(
            dag_id=dag_id,
            source_system='nds',
            source_table='order_items',
            target_layer='dds'
        )

        print(f"[fact_sales] LSET: {lset}, CET: {cet}")

        # Build incremental filter
        if lset:
            time_filter = f"AND oi.nds_loaded_at > '{lset}' AND oi.nds_loaded_at <= '{cet}'"
        else:
            time_filter = f"AND oi.nds_loaded_at <= '{cet}'"

        # Insert fact records with dimension lookups
        insert_sql = f"""
            INSERT INTO dds.fact_sales (
                sale_nk,
                customer_key,
                product_key,
                date_key,
                order_number,
                quantity,
                unit_price,
                discount_amount,
                line_total,
                cost_amount,
                profit_amount,
                source_system,
                dds_loaded_at
            )
            SELECT
                oi.order_item_nk AS sale_nk,
                dc.customer_key,
                dp.product_key,
                dd.date_key,
                o.order_number,
                oi.quantity,
                oi.unit_price,
                COALESCE(oi.line_total * oi.discount_percent / 100, 0) AS discount_amount,
                oi.line_total,
                dp.cost_price * oi.quantity AS cost_amount,
                oi.line_total - (dp.cost_price * oi.quantity) AS profit_amount,
                oi.source_system,
                CURRENT_TIMESTAMP
            FROM nds.order_items oi
            INNER JOIN nds.orders o
                ON oi.order_nk = o.order_nk
            LEFT JOIN dds.dim_customers dc
                ON o.customer_email = dc.email
                AND dc.is_current = TRUE
            LEFT JOIN dds.dim_products dp
                ON oi.product_nk = dp.product_nk
                AND dp.is_current = TRUE
            LEFT JOIN dds.dim_date dd
                ON o.order_date::DATE = dd.full_date
            WHERE {time_filter.replace('AND ', '', 1)}
              AND NOT EXISTS (
                  SELECT 1 FROM dds.fact_sales fs
                  WHERE fs.sale_nk = oi.order_item_nk
              )
        """

        target_hook.run(insert_sql)

        # Get counts
        inserted_result = target_hook.get_first(f"""
            SELECT COUNT(*) FROM dds.fact_sales
            WHERE dds_loaded_at >= CURRENT_TIMESTAMP - INTERVAL '5 seconds'
        """)
        inserted_count = inserted_result[0] if inserted_result else 0

        total_result = target_hook.get_first("SELECT COUNT(*) FROM dds.fact_sales")
        total_count = total_result[0] if total_result else 0

        print(f"[fact_sales] Inserted {inserted_count} new records")
        print(f"[fact_sales] Total records in fact table: {total_count}")

        # Update watermark
        watermark_hook.update_watermark(
            dag_id=dag_id,
            source_system='nds',
            source_table='order_items',
            target_layer='dds',
            target_table='fact_sales',
            lset=lset,
            cet=cet,
            status='SUCCESS',
            rows_extracted=inserted_count,
            rows_loaded=inserted_count
        )

        # Log run
        watermark_hook.log_etl_run(
            dag_id=dag_id,
            dag_run_id=context['dag_run'].run_id,
            task_id=context['task'].task_id,
            source_system='nds',
            source_table='order_items',
            target_layer='dds',
            target_table='fact_sales',
            execution_date=execution_date,
            status='SUCCESS',
            lset=lset,
            cet=cet,
            rows_extracted=inserted_count,
            rows_loaded=inserted_count
        )

        return {
            'fact_table': 'fact_sales',
            'inserted': inserted_count,
            'total_records': total_count
        }

    # Define task dependencies
    dim_customers = load_dimension_customers.override(
        task_id='load_dim_customers',
        inlets=[DATASET_NDS_CUSTOMERS],
        outlets=[DATASET_DDS_DIM_CUSTOMERS]
    )()

    dim_products = load_dimension_products.override(
        task_id='load_dim_products',
        inlets=[DATASET_NDS_PRODUCTS],
        outlets=[DATASET_DDS_DIM_PRODUCTS]
    )()

    dim_date = verify_dimension_date.override(
        task_id='verify_dim_date',
        outlets=[DATASET_DDS_DIM_DATE]
    )()

    fact_sales = load_fact_sales.override(
        task_id='load_fact_sales',
        inlets=[DATASET_NDS_ORDER_ITEMS, DATASET_NDS_CUSTOMERS, DATASET_NDS_PRODUCTS, DATASET_NDS_ORDERS],
        outlets=[DATASET_DDS_FACT_SALES]
    )()

    # Verify connections first, then load dimensions in parallel, then load facts
    verify_conn = verify_database_connections()
    verify_conn >> [dim_customers, dim_products, dim_date] >> fact_sales


# Instantiate the DAG
nds_to_dds_dag = nds_to_dds_pipeline()

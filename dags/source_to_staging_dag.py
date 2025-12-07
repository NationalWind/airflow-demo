"""
DAG 1: Source to Staging Layer
Incremental extraction from source systems (CRM, ERP, Web) to Staging with full truncate/load.
Uses LSET/CET for incremental extraction, event-driven with Datasets for lineage.
"""

from datetime import datetime, timedelta
from typing import Dict, List

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

import sys
sys.path.append('/opt/airflow')

from plugins.hooks.watermark_hook import WatermarkHook
from plugins.utils.sql_templates import SQLTemplates
from dags.config.pipeline_config import (
    CRM_CONFIG, ERP_CONFIG,
    CONN_TARGET, CONN_METADATA,
    DEFAULT_ARGS
)
from dags.datasets_config import (
    # Source datasets (inlets)
    DATASET_SOURCE_CRM_CUSTOMERS,
    DATASET_SOURCE_ERP_PRODUCTS,
    DATASET_SOURCE_ERP_ORDERS,
    DATASET_SOURCE_ERP_ORDER_ITEMS,
    # Staging datasets (outlets)
    DATASET_STG_CRM_CUSTOMERS,
    DATASET_STG_ERP_PRODUCTS,
    DATASET_STG_ERP_ORDERS,
    DATASET_STG_ERP_ORDER_ITEMS,
)


@dag(
    dag_id='source_to_staging',
    default_args=DEFAULT_ARGS,
    description='Incremental extraction from sources to staging with LSET/CET',
    schedule='0 0 * * *',  # Daily at midnight
    start_date=datetime(2024, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'staging', 'incremental'],
)
def source_to_staging_pipeline():
    """
    Source to Staging ETL Pipeline.

    Flow:
    1. Verify database connections
    2. Get LSET/CET watermarks for each source table
    3. Extract incrementally from source using LSET/CET
    4. Truncate staging table
    5. Load extracted data to staging
    6. Update watermarks
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
            'CRM': CRM_CONFIG['connection_id'],
            'ERP': ERP_CONFIG['connection_id'],
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
    def extract_and_load_table(
        source_system: str,
        table_name: str,
        source_conn_id: str,
        source_table: str,
        staging_table: str,
        business_columns: List[str],
        **context
    ) -> Dict[str, int]:
        """
        Extract from source and load to staging for a single table.

        Args:
            source_system: Source system identifier (crm, erp, web)
            table_name: Table name
            source_conn_id: Airflow connection ID for source
            source_table: Fully qualified source table name
            staging_table: Staging table name
            business_columns: List of columns to extract

        Returns:
            Dictionary with extraction metrics
        """
        dag_id = context['dag'].dag_id
        execution_date = context.get('ds') or datetime.now().strftime('%Y-%m-%d')

        # Initialize hooks
        watermark_hook = WatermarkHook(postgres_conn_id=CONN_METADATA)
        source_hook = PostgresHook(postgres_conn_id=source_conn_id)
        target_hook = PostgresHook(postgres_conn_id=CONN_TARGET)

        # Step 1: Get LSET/CET
        lset, cet = watermark_hook.get_watermark(
            dag_id=dag_id,
            source_system=source_system,
            source_table=table_name,
            target_layer='stg'
        )

        print(f"[{source_system}.{table_name}] LSET: {lset}, CET: {cet}")

        # Step 2: Extract incrementally from source
        has_lset = lset is not None
        extract_sql = SQLTemplates.incremental_extract(
            source_table=source_table,
            columns=business_columns,
            has_lset=has_lset
        )

        if has_lset:
            records = source_hook.get_records(extract_sql, parameters=(lset, cet))
        else:
            records = source_hook.get_records(extract_sql, parameters=(cet,))

        rows_extracted = len(records)
        print(f"[{source_system}.{table_name}] Extracted {rows_extracted} rows")

        # Step 3: Truncate staging table
        truncate_sql = SQLTemplates.truncate_staging(staging_table)
        target_hook.run(truncate_sql)
        print(f"[{source_system}.{table_name}] Truncated {staging_table}")

        # Step 4: Load to staging
        rows_loaded = 0
        if rows_extracted > 0:
            insert_sql = SQLTemplates.insert_staging(
                staging_table=staging_table,
                columns=business_columns,
                source_system=source_system
            )

            for record in records:
                target_hook.run(insert_sql, parameters=tuple(record))
                rows_loaded += 1

        print(f"[{source_system}.{table_name}] Loaded {rows_loaded} rows to {staging_table}")

        # Return metrics for watermark update task
        return {
            'dag_id': dag_id,
            'dag_run_id': context['dag_run'].run_id,
            'task_id': context['task'].task_id,
            'execution_date': execution_date,
            'source_system': source_system,
            'table': table_name,
            'staging_table': staging_table,
            'rows_extracted': rows_extracted,
            'rows_loaded': rows_loaded,
            'lset': str(lset) if lset else None,
            'cet': str(cet)
        }

    @task
    def update_watermark(extraction_result: Dict, **context):
        """
        Update watermark and log ETL run after successful extraction.

        Args:
            extraction_result: Dictionary containing extraction metrics
        """
        from plugins.hooks.watermark_hook import WatermarkHook

        watermark_hook = WatermarkHook(postgres_conn_id=CONN_METADATA)

        # Parse LSET/CET back from strings
        lset = extraction_result['lset']
        cet = extraction_result['cet']

        # Update watermark
        watermark_hook.update_watermark(
            dag_id=extraction_result['dag_id'],
            source_system=extraction_result['source_system'],
            source_table=extraction_result['table'],
            target_layer='stg',
            target_table=extraction_result['staging_table'],
            lset=lset,
            cet=cet,
            status='SUCCESS',
            rows_extracted=extraction_result['rows_extracted'],
            rows_loaded=extraction_result['rows_loaded']
        )

        # Log run
        watermark_hook.log_etl_run(
            dag_id=extraction_result['dag_id'],
            dag_run_id=extraction_result['dag_run_id'],
            task_id=extraction_result['task_id'],
            source_system=extraction_result['source_system'],
            source_table=extraction_result['table'],
            target_layer='stg',
            target_table=extraction_result['staging_table'],
            execution_date=extraction_result['execution_date'],
            status='SUCCESS',
            lset=lset,
            cet=cet,
            rows_extracted=extraction_result['rows_extracted'],
            rows_loaded=extraction_result['rows_loaded']
        )

        print(f"✓ Watermark updated for {extraction_result['source_system']}.{extraction_result['table']}")

        return extraction_result

    # CRM Tables
    crm_customers = extract_and_load_table.override(
        task_id='extract_crm_customers',
        inlets=[DATASET_SOURCE_CRM_CUSTOMERS],
        outlets=[DATASET_STG_CRM_CUSTOMERS]
    )(
        source_system=CRM_CONFIG['source_system'],
        table_name='customers',
        source_conn_id=CRM_CONFIG['connection_id'],
        source_table=CRM_CONFIG['tables']['customers']['source_table'],
        staging_table=CRM_CONFIG['tables']['customers']['staging_table'],
        business_columns=CRM_CONFIG['tables']['customers']['business_columns']
    )

    # ERP Tables
    erp_products = extract_and_load_table.override(
        task_id='extract_erp_products',
        inlets=[DATASET_SOURCE_ERP_PRODUCTS],
        outlets=[DATASET_STG_ERP_PRODUCTS]
    )(
        source_system=ERP_CONFIG['source_system'],
        table_name='products',
        source_conn_id=ERP_CONFIG['connection_id'],
        source_table=ERP_CONFIG['tables']['products']['source_table'],
        staging_table=ERP_CONFIG['tables']['products']['staging_table'],
        business_columns=ERP_CONFIG['tables']['products']['business_columns']
    )

    erp_orders = extract_and_load_table.override(
        task_id='extract_erp_orders',
        inlets=[DATASET_SOURCE_ERP_ORDERS],
        outlets=[DATASET_STG_ERP_ORDERS]
    )(
        source_system=ERP_CONFIG['source_system'],
        table_name='orders',
        source_conn_id=ERP_CONFIG['connection_id'],
        source_table=ERP_CONFIG['tables']['orders']['source_table'],
        staging_table=ERP_CONFIG['tables']['orders']['staging_table'],
        business_columns=ERP_CONFIG['tables']['orders']['business_columns']
    )

    erp_order_items = extract_and_load_table.override(
        task_id='extract_erp_order_items',
        inlets=[DATASET_SOURCE_ERP_ORDER_ITEMS],
        outlets=[DATASET_STG_ERP_ORDER_ITEMS]
    )(
        source_system=ERP_CONFIG['source_system'],
        table_name='order_items',
        source_conn_id=ERP_CONFIG['connection_id'],
        source_table=ERP_CONFIG['tables']['order_items']['source_table'],
        staging_table=ERP_CONFIG['tables']['order_items']['staging_table'],
        business_columns=ERP_CONFIG['tables']['order_items']['business_columns']
    )

    # Create watermark update tasks
    update_crm_customers = update_watermark.override(task_id='update_watermark_crm_customers')(crm_customers)
    update_erp_products = update_watermark.override(task_id='update_watermark_erp_products')(erp_products)
    update_erp_orders = update_watermark.override(task_id='update_watermark_erp_orders')(erp_orders)
    update_erp_order_items = update_watermark.override(task_id='update_watermark_erp_order_items')(erp_order_items)

    # Dependencies: verify connections -> extract in parallel -> update watermarks in parallel
    verify_conn = verify_database_connections()

    # Extraction tasks
    verify_conn >> [crm_customers, erp_products, erp_orders, erp_order_items]

    # Watermark updates (depend on their respective extractions)
    crm_customers >> update_crm_customers
    erp_products >> update_erp_products
    erp_orders >> update_erp_orders
    erp_order_items >> update_erp_order_items


# Instantiate the DAG
source_to_staging_dag = source_to_staging_pipeline()

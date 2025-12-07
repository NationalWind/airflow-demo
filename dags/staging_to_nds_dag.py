"""
DAG 2: Staging to NDS Layer
Simple UPSERT to maintain current state (no history tracking).
Event-driven: Triggered when staging datasets are updated.
SCD Type 2 logic is implemented in the DDS layer.
"""

from datetime import datetime, timedelta
from typing import Dict, List

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

import sys
sys.path.append('/opt/airflow')

from plugins.hooks.watermark_hook import WatermarkHook
from dags.config.pipeline_config import (
    CRM_CONFIG, ERP_CONFIG,
    CONN_TARGET, CONN_METADATA,
    DEFAULT_ARGS
)
from dags.datasets_config import (
    # Inputs: Staging datasets (triggers)
    DATASET_STG_CRM_CUSTOMERS,
    DATASET_STG_ERP_PRODUCTS,
    DATASET_STG_ERP_ORDERS,
    DATASET_STG_ERP_ORDER_ITEMS,
    # Outputs: NDS datasets (produced)
    DATASET_NDS_CUSTOMERS,
    DATASET_NDS_PRODUCTS,
    DATASET_NDS_ORDERS,
    DATASET_NDS_ORDER_ITEMS,
)


@dag(
    dag_id='staging_to_nds',
    default_args=DEFAULT_ARGS,
    description='Event-driven UPSERT transformation triggered by staging updates',
    # EVENT-DRIVEN: Triggered when staging assets are updated
    schedule=[
        DATASET_STG_CRM_CUSTOMERS,
        DATASET_STG_ERP_PRODUCTS,
        DATASET_STG_ERP_ORDERS,
        DATASET_STG_ERP_ORDER_ITEMS,
    ],
    start_date=datetime(2024, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'nds', 'upsert', 'event-driven'],
)
def staging_to_nds_pipeline():
    """
    Staging to NDS ETL Pipeline.

    Flow:
    1. Verify database connections
    2. Get LSET/CET watermarks for each staging table
    3. UPSERT records from staging to NDS (INSERT or UPDATE)
    4. Update watermarks
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
    def upsert_to_nds(
        source_system: str,
        table_name: str,
        staging_table: str,
        nds_table: str,
        natural_key_columns: List[str],
        business_columns: List[str],
        **context
    ) -> Dict[str, int]:
        """
        UPSERT records from staging to NDS (maintains current state only).

        Args:
            source_system: Source system identifier (crm, erp, web)
            table_name: Table name
            staging_table: Staging table name
            nds_table: NDS table name
            natural_key_columns: Columns forming the natural key
            business_columns: Business attribute columns

        Returns:
            Dictionary with upsert metrics
        """
        dag_id = context['dag'].dag_id
        execution_date = context.get('ds') or datetime.now().strftime('%Y-%m-%d')

        # Initialize hooks
        watermark_hook = WatermarkHook(postgres_conn_id=CONN_METADATA)
        target_hook = PostgresHook(postgres_conn_id=CONN_TARGET)

        # Step 1: Get LSET/CET
        lset, cet = watermark_hook.get_watermark(
            dag_id=dag_id,
            source_system=source_system,
            source_table=table_name,
            target_layer='nds'
        )

        print(f"[{source_system}.{table_name}] LSET: {lset}, CET: {cet}")

        # Determine natural key column name in NDS table
        # NDS tables use {entity}_nk format (e.g., customer_id -> customer_nk)
        if len(natural_key_columns) == 1:
            # Remove '_id' suffix if present, then add '_nk'
            base_name = natural_key_columns[0].replace('_id', '')
            nk_column = f"{base_name}_nk"
        else:
            nk_column = "natural_key"

        # Step 2: Map staging columns to NDS columns and build UPSERT
        # Map business columns to NDS columns
        # For _id columns: convert to _nk format (source_system + '-' + ID)
        nds_columns = []
        for col in business_columns:
            if col == 'created_at':
                nds_columns.append('source_created_at')
            elif col == 'updated_at':
                nds_columns.append('source_updated_at')
            elif col.endswith('_id'):
                # Replace _id with _nk in NDS (e.g., customer_id -> customer_nk)
                base_name = col.replace('_id', '')
                nds_columns.append(f"{base_name}_nk")
            else:
                nds_columns.append(col)

        # Build SELECT expressions to match NDS columns
        select_exprs = []
        for col in business_columns:
            if col == 'created_at':
                select_exprs.append('stg.created_at')
            elif col == 'updated_at':
                select_exprs.append('stg.updated_at')
            elif col.endswith('_id'):
                # Keep exact ID value from source (just rename column to _nk)
                select_exprs.append(f'stg.{col}')
            else:
                select_exprs.append(f'stg.{col}')

        nds_columns_str = ', '.join(nds_columns)
        select_exprs_str = ', '.join(select_exprs)

        # Build UPDATE SET clause for ON CONFLICT (exclude NK column from updates)
        update_set_parts = []
        for col in nds_columns:
            if col != nk_column:  # Don't update the natural key
                update_set_parts.append(f"{col} = EXCLUDED.{col}")
        update_set_parts.append("nds_updated_at = CURRENT_TIMESTAMP")
        update_set_str = ', '.join(update_set_parts)

        # UPSERT (INSERT ... ON CONFLICT UPDATE)
        upsert_sql = f"""
            INSERT INTO {nds_table} (
                {nds_columns_str},
                source_system,
                nds_loaded_at,
                nds_updated_at
            )
            SELECT
                {select_exprs_str},
                '{source_system}' AS source_system,
                CURRENT_TIMESTAMP AS nds_loaded_at,
                CURRENT_TIMESTAMP AS nds_updated_at
            FROM {staging_table} stg
            ON CONFLICT ({nk_column})
            DO UPDATE SET
                {update_set_str}
        """

        target_hook.run(upsert_sql)

        # Get counts
        total_result = target_hook.get_first(f"SELECT COUNT(*) FROM {nds_table}")
        total_count = total_result[0] if total_result else 0

        print(f"[{source_system}.{table_name}] UPSERT completed - Total records in NDS: {total_count}")

        # Step 3: Update watermark
        watermark_hook.update_watermark(
            dag_id=dag_id,
            source_system=source_system,
            source_table=table_name,
            target_layer='nds',
            target_table=nds_table,
            lset=lset,
            cet=cet,
            status='SUCCESS',
            rows_extracted=total_count,
            rows_loaded=total_count
        )

        # Step 4: Log run
        watermark_hook.log_etl_run(
            dag_id=dag_id,
            dag_run_id=context['dag_run'].run_id,
            task_id=context['task'].task_id,
            source_system=source_system,
            source_table=table_name,
            target_layer='nds',
            target_table=nds_table,
            execution_date=execution_date,
            status='SUCCESS',
            lset=lset,
            cet=cet,
            rows_extracted=total_count,
            rows_loaded=total_count
        )

        return {
            'source_system': source_system,
            'table': table_name,
            'total_records': total_count,
            'lset': str(lset) if lset else 'None',
            'cet': str(cet)
        }

    # CRM Tables
    crm_customers = upsert_to_nds.override(
        task_id='upsert_crm_customers',
        inlets=[DATASET_STG_CRM_CUSTOMERS],
        outlets=[DATASET_NDS_CUSTOMERS]
    )(
        source_system=CRM_CONFIG['source_system'],
        table_name='customers',
        staging_table=CRM_CONFIG['tables']['customers']['staging_table'],
        nds_table=CRM_CONFIG['tables']['customers']['nds_table'],
        natural_key_columns=CRM_CONFIG['tables']['customers']['natural_key'],
        business_columns=CRM_CONFIG['tables']['customers']['business_columns']
    )

    # ERP Tables
    erp_products = upsert_to_nds.override(
        task_id='upsert_erp_products',
        inlets=[DATASET_STG_ERP_PRODUCTS],
        outlets=[DATASET_NDS_PRODUCTS]
    )(
        source_system=ERP_CONFIG['source_system'],
        table_name='products',
        staging_table=ERP_CONFIG['tables']['products']['staging_table'],
        nds_table=ERP_CONFIG['tables']['products']['nds_table'],
        natural_key_columns=ERP_CONFIG['tables']['products']['natural_key'],
        business_columns=ERP_CONFIG['tables']['products']['business_columns']
    )

    erp_orders = upsert_to_nds.override(
        task_id='upsert_erp_orders',
        inlets=[DATASET_STG_ERP_ORDERS],
        outlets=[DATASET_NDS_ORDERS]
    )(
        source_system=ERP_CONFIG['source_system'],
        table_name='orders',
        staging_table=ERP_CONFIG['tables']['orders']['staging_table'],
        nds_table=ERP_CONFIG['tables']['orders']['nds_table'],
        natural_key_columns=ERP_CONFIG['tables']['orders']['natural_key'],
        business_columns=ERP_CONFIG['tables']['orders']['business_columns']
    )

    erp_order_items = upsert_to_nds.override(
        task_id='upsert_erp_order_items',
        inlets=[DATASET_STG_ERP_ORDER_ITEMS],
        outlets=[DATASET_NDS_ORDER_ITEMS]
    )(
        source_system=ERP_CONFIG['source_system'],
        table_name='order_items',
        staging_table=ERP_CONFIG['tables']['order_items']['staging_table'],
        nds_table=ERP_CONFIG['tables']['order_items']['nds_table'],
        natural_key_columns=ERP_CONFIG['tables']['order_items']['natural_key'],
        business_columns=ERP_CONFIG['tables']['order_items']['business_columns']
    )

    # Verify connections first, then run all upsert tasks in parallel
    verify_conn = verify_database_connections()
    verify_conn >> [crm_customers, erp_products, erp_orders, erp_order_items]


# Instantiate the DAG
staging_to_nds_dag = staging_to_nds_pipeline()

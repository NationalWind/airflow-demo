"""
Constants and configuration values for the ETL pipeline.
"""

# Source Systems
SOURCE_SYSTEMS = {
    'CRM': 'crm',
    'ERP': 'erp',
    'WEB': 'web'
}

# Data Layers
LAYERS = {
    'STAGING': 'stg',
    'NDS': 'nds',
    'DDS': 'dds'
}

# Connection IDs
CONNECTIONS = {
    'CRM': 'postgres_source_crm',
    'ERP': 'postgres_source_erp',
    'WEB': 'postgres_source_web',
    'TARGET': 'postgres_target',
    'METADATA': 'postgres_metadata'
}

# Status Values
STATUS = {
    'SUCCESS': 'SUCCESS',
    'FAILED': 'FAILED',
    'RUNNING': 'RUNNING',
    'SKIPPED': 'SKIPPED'
}

# SCD Types
SCD_TYPE_1 = 'TYPE_1'
SCD_TYPE_2 = 'TYPE_2'

# Schema Mappings
STAGING_SCHEMAS = {
    'crm': 'stg_crm',
    'erp': 'stg_erp',
    'web': 'stg_web'
}

NDS_SCHEMA = 'nds'
DDS_SCHEMA = 'dds'

# Table Mappings for CRM
CRM_TABLES = {
    'customers': {
        'source': 'crm.customers',
        'staging': 'stg_crm.customers',
        'nds': 'nds.customers',
        'dds': 'dds.dim_customers',
        'natural_key': ['source_system', 'customer_id'],
        'scd_type': SCD_TYPE_2
    },
    'customer_interactions': {
        'source': 'crm.customer_interactions',
        'staging': 'stg_crm.customer_interactions',
        'natural_key': ['source_system', 'interaction_id']
    }
}

# Table Mappings for ERP
ERP_TABLES = {
    'products': {
        'source': 'erp.products',
        'staging': 'stg_erp.products',
        'nds': 'nds.products',
        'dds': 'dds.dim_products',
        'natural_key': ['source_system', 'product_id'],
        'scd_type': SCD_TYPE_2
    },
    'orders': {
        'source': 'erp.orders',
        'staging': 'stg_erp.orders',
        'nds': 'nds.orders',
        'natural_key': ['source_system', 'order_id'],
        'scd_type': SCD_TYPE_2
    },
    'order_items': {
        'source': 'erp.order_items',
        'staging': 'stg_erp.order_items',
        'nds': 'nds.order_items',
        'dds': 'dds.fact_sales',
        'natural_key': ['source_system', 'order_item_id'],
        'scd_type': SCD_TYPE_2
    }
}

# DAG Default Args
DEFAULT_DAG_ARGS = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': 300,  # 5 minutes in seconds
}

# Execution Intervals
SCHEDULE_INTERVALS = {
    'source_to_staging': '*/15 * * * *',  # Every 15 minutes
    'staging_to_nds': '*/30 * * * *',     # Every 30 minutes
    'nds_to_dds': '0 * * * *',            # Every hour
}

"""
Pipeline configuration for ETL workflows.
"""

from datetime import timedelta

# ============================================
# CONNECTION IDS
# ============================================
CONN_CRM = 'postgres_source_crm'
CONN_ERP = 'postgres_source_erp'
CONN_WEB = 'postgres_source_web'
CONN_TARGET = 'postgres_target'
CONN_METADATA = 'postgres_metadata'

# ============================================
# SOURCE SYSTEM CONFIGURATIONS
# ============================================
CRM_CONFIG = {
    'source_system': 'crm',
    'connection_id': CONN_CRM,
    'tables': {
        'customers': {
            'source_table': 'crm.customers',
            'staging_table': 'stg_crm.customers',
            'nds_table': 'nds.customers',
            'dds_table': 'dds.dim_customers',
            'natural_key': ['customer_id'],
            'business_columns': [
                'customer_id', 'first_name', 'last_name', 'email', 'phone',
                'date_of_birth', 'city', 'state', 'country', 'postal_code',
                'customer_segment', 'loyalty_tier', 'created_at', 'updated_at'
            ],
            'scd_type': 2
        }
    }
}

ERP_CONFIG = {
    'source_system': 'erp',
    'connection_id': CONN_ERP,
    'tables': {
        'products': {
            'source_table': 'erp.products',
            'staging_table': 'stg_erp.products',
            'nds_table': 'nds.products',
            'dds_table': 'dds.dim_products',
            'natural_key': ['product_id'],
            'business_columns': [
                'product_id', 'sku', 'product_name', 'category', 'subcategory',
                'brand', 'unit_price', 'cost_price', 'weight_kg', 'active',
                'created_at', 'updated_at'
            ],
            'scd_type': 2
        },
        'orders': {
            'source_table': 'erp.orders',
            'staging_table': 'stg_erp.orders',
            'nds_table': 'nds.orders',
            'natural_key': ['order_id'],
            'business_columns': [
                'order_id', 'order_number', 'customer_email', 'order_date',
                'order_status', 'payment_method', 'payment_status',
                'total_amount', 'created_at', 'updated_at'
            ],
            'scd_type': 2
        },
        'order_items': {
            'source_table': 'erp.order_items',
            'staging_table': 'stg_erp.order_items',
            'nds_table': 'nds.order_items',
            'dds_table': 'dds.fact_sales',
            'natural_key': ['order_item_id'],
            'business_columns': [
                'order_item_id', 'order_id', 'product_id', 'quantity',
                'unit_price', 'discount_percent', 'line_total',
                'created_at', 'updated_at'
            ],
            'scd_type': 2
        }
    }
}

WEB_CONFIG = {
    'source_system': 'web',
    'connection_id': CONN_WEB,
    'tables': {
        'page_views': {
            'source_table': 'web.page_views',
            'staging_table': 'stg_web.page_views',
            'natural_key': ['page_view_id'],
            'business_columns': [
                'page_view_id', 'session_id', 'user_email', 'page_url',
                'page_title', 'referrer_url', 'device_type', 'browser',
                'country', 'view_timestamp', 'time_on_page_seconds',
                'created_at', 'updated_at'
            ]
        },
        'events': {
            'source_table': 'web.events',
            'staging_table': 'stg_web.events',
            'natural_key': ['event_id'],
            'business_columns': [
                'event_id', 'session_id', 'user_email', 'event_type',
                'event_category', 'event_action', 'event_label', 'event_value',
                'event_timestamp', 'page_url', 'created_at', 'updated_at'
            ]
        }
    }
}

# ============================================
# DAG DEFAULT ARGUMENTS
# ============================================
DEFAULT_ARGS = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email': ['data-team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ============================================
# SCHEDULE INTERVALS
# ============================================
SCHEDULE_SOURCE_TO_STAGING = '*/15 * * * *'  # Every 15 minutes
SCHEDULE_STAGING_TO_NDS = '*/30 * * * *'     # Every 30 minutes
SCHEDULE_NDS_TO_DDS = '0 * * * *'            # Every hour

# ============================================
# QUALITY CHECK THRESHOLDS
# ============================================
QUALITY_THRESHOLDS = {
    'max_null_percent': 5.0,  # Max percentage of NULL values allowed
    'max_duplicate_percent': 1.0,  # Max percentage of duplicates allowed
    'min_row_count': 1,  # Minimum rows expected
}

"""
Asset definitions for data lineage tracking.
Airflow 3 uses Assets (renamed from Datasets) to automatically build lineage graphs.

Note: Using generic URI scheme to avoid postgres-specific validation.
Format: dataset:///<layer>/<schema>/<table>
"""

from airflow.sdk.definitions.asset import Asset

# ============================================
# SOURCE LAYER ASSETS (External Systems)
# ============================================
DATASET_SOURCE_CRM_CUSTOMERS = Asset(uri="dataset:///source/crm/customers")
DATASET_SOURCE_ERP_PRODUCTS = Asset(uri="dataset:///source/erp/products")
DATASET_SOURCE_ERP_ORDERS = Asset(uri="dataset:///source/erp/orders")
DATASET_SOURCE_ERP_ORDER_ITEMS = Asset(uri="dataset:///source/erp/order_items")

# ============================================
# STAGING LAYER ASSETS
# ============================================
DATASET_STG_CRM_CUSTOMERS = Asset(uri="dataset:///stg/stg_crm/customers")
DATASET_STG_ERP_PRODUCTS = Asset(uri="dataset:///stg/stg_erp/products")
DATASET_STG_ERP_ORDERS = Asset(uri="dataset:///stg/stg_erp/orders")
DATASET_STG_ERP_ORDER_ITEMS = Asset(uri="dataset:///stg/stg_erp/order_items")
DATASET_STG_WEB_PAGE_VIEWS = Asset(uri="dataset:///stg/stg_web/page_views")
DATASET_STG_WEB_EVENTS = Asset(uri="dataset:///stg/stg_web/events")

# ============================================
# NDS LAYER ASSETS
# ============================================
DATASET_NDS_CUSTOMERS = Asset(uri="dataset:///nds/nds/customers")
DATASET_NDS_PRODUCTS = Asset(uri="dataset:///nds/nds/products")
DATASET_NDS_ORDERS = Asset(uri="dataset:///nds/nds/orders")
DATASET_NDS_ORDER_ITEMS = Asset(uri="dataset:///nds/nds/order_items")

# ============================================
# DDS LAYER ASSETS
# ============================================
DATASET_DDS_DIM_CUSTOMERS = Asset(uri="dataset:///dds/dds/dim_customers")
DATASET_DDS_DIM_PRODUCTS = Asset(uri="dataset:///dds/dds/dim_products")
DATASET_DDS_DIM_DATE = Asset(uri="dataset:///dds/dds/dim_date")
DATASET_DDS_FACT_SALES = Asset(uri="dataset:///dds/dds/fact_sales")

# ============================================
# LINEAGE MAPPINGS (for documentation)
# ============================================
LINEAGE_MAP = {
    # Source → Staging
    "crm.customers": "stg_crm.customers → nds.customers → dds.dim_customers",
    "erp.products": "stg_erp.products → nds.products → dds.dim_products",
    "erp.orders": "stg_erp.orders → nds.orders → (fact_sales)",
    "erp.order_items": "stg_erp.order_items → nds.order_items → dds.fact_sales",

    # Web tables (not in DDS yet)
    "web.page_views": "stg_web.page_views → (future analytics)",
    "web.events": "stg_web.events → (future analytics)",
}

"""
SSIS Integration using airflow-ssis-provider
Clean and simple approach to run SSIS packages from Airflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Import SSIS Operator from the provider
try:
    from airflow_ssis_provider.operators.ssis import SSISOperator
    SSIS_AVAILABLE = True
except ImportError:
    SSIS_AVAILABLE = False
    print("WARNING: airflow-ssis-provider not installed")

from airflow.providers.common.sql.hooks.sql import DbApiHook
import logging

# ============================================
# CUSTOM MSSQL HOOK (Workaround)
# ============================================
class MsSqlHook(DbApiHook):
    """Custom MS SQL Hook using pymssql"""
    conn_name_attr = 'mssql_conn_id'
    default_conn_name = 'mssql_default'
    conn_type = 'mssql'
    hook_name = 'Microsoft SQL Server'
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)
    
    def get_conn(self):
        """Returns a mssql connection object"""
        import pymssql
        conn = self.get_connection(self.mssql_conn_id)
        
        return pymssql.connect(
            server=conn.host,
            user=conn.login,
            password=conn.password,
            database=conn.schema or self.schema or 'master',
            port=conn.port or 1433
        )

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
}

dag = DAG(
    'ssis_provider_demo',
    default_args=default_args,
    description='Execute SSIS packages using airflow-ssis-provider',
    catchup=False,
    max_active_runs=1,
    tags=['ssis', 'provider', 'production']
)

# ============================================
# CONFIGURATION VARIABLES
# ============================================
SSIS_CONN_ID = 'ssis_default'
FOLDER_NAME = 'AirflowDemo'
PROJECT_NAME = 'AirflowSSISDemo'
PACKAGE_NAME = 'SalesDataETL.dtsx'
ENVIRONMENT_NAME = 'Production'

# ============================================
# TASK 1: PRE-EXECUTION VALIDATION
# ============================================
def validate_setup(**context):
    """Validate that SSIS catalog and package are accessible"""
    
    logging.info("Validating SSIS setup...")
    
    mssql_hook = MsSqlHook(mssql_conn_id=SSIS_CONN_ID)
    
    # Check 1: SSISDB exists
    ssisdb_check = """
        SELECT COUNT(*) FROM sys.databases WHERE name = 'SSISDB';
    """
    result = mssql_hook.get_first(ssisdb_check)
    if not result or result[0] == 0:
        raise Exception("SSISDB catalog not found")
    logging.info("✓ SSISDB catalog exists")
    
    # Check 2: Folder exists
    folder_check = f"""
        SELECT COUNT(*) FROM SSISDB.catalog.folders 
        WHERE name = '{FOLDER_NAME}';
    """
    result = mssql_hook.get_first(folder_check)
    if not result or result[0] == 0:
        raise Exception(f"Folder '{FOLDER_NAME}' not found in SSISDB")
    logging.info(f"✓ Folder '{FOLDER_NAME}' exists")
    
    # Check 3: Project exists
    project_check = f"""
        SELECT COUNT(*) 
        FROM SSISDB.catalog.projects p
        INNER JOIN SSISDB.catalog.folders f ON p.folder_id = f.folder_id
        WHERE f.name = '{FOLDER_NAME}' AND p.name = '{PROJECT_NAME}';
    """
    result = mssql_hook.get_first(project_check)
    if not result or result[0] == 0:
        raise Exception(f"Project '{PROJECT_NAME}' not found in folder '{FOLDER_NAME}'")
    logging.info(f"✓ Project '{PROJECT_NAME}' exists")
    
    # Check 4: Package exists
    package_check = f"""
        SELECT COUNT(*) 
        FROM SSISDB.catalog.packages pk
        INNER JOIN SSISDB.catalog.projects pr ON pk.project_id = pr.project_id
        INNER JOIN SSISDB.catalog.folders f ON pr.folder_id = f.folder_id
        WHERE f.name = '{FOLDER_NAME}' 
        AND pr.name = '{PROJECT_NAME}'
        AND pk.name = '{PACKAGE_NAME}';
    """
    result = mssql_hook.get_first(package_check)
    if not result or result[0] == 0:
        raise Exception(f"Package '{PACKAGE_NAME}' not found")
    logging.info(f"✓ Package '{PACKAGE_NAME}' exists")
    
    # Check 5: Environment exists (if using)
    if ENVIRONMENT_NAME:
        env_check = f"""
            SELECT COUNT(*) 
            FROM SSISDB.catalog.environments e
            INNER JOIN SSISDB.catalog.folders f ON e.folder_id = f.folder_id
            WHERE f.name = '{FOLDER_NAME}' AND e.name = '{ENVIRONMENT_NAME}';
        """
        result = mssql_hook.get_first(env_check)
        if not result or result[0] == 0:
            logging.warning(f"Environment '{ENVIRONMENT_NAME}' not found - will use package defaults")
        else:
            logging.info(f"✓ Environment '{ENVIRONMENT_NAME}' exists")
    
    logging.info("✅ All validation checks passed")
    return True

validate_task = PythonOperator(
    task_id='validate_ssis_setup',
    python_callable=validate_setup,
    dag=dag
)

# ============================================
# TASK 2: EXECUTE SSIS PACKAGE
# ============================================
if SSIS_AVAILABLE:
    execute_ssis = SSISOperator(
        task_id='execute_ssis_package',
        
        # Connection
        mssql_conn_id=SSIS_CONN_ID,
        
        # Package location
        server='host.docker.internal',  # SQL Server host
        folder_name=FOLDER_NAME,
        project_name=PROJECT_NAME,
        package_name=PACKAGE_NAME,
        
        # Environment
        environment_folder=FOLDER_NAME,
        environment_name=ENVIRONMENT_NAME,
        
        # Package parameters (override defaults)
        package_params={
            'ExecutionDate': '{{ ds }}',  # Airflow execution date
            'BatchSize': 1000,
            'Environment': 'Production'
        },
        
        # Execution options
        use32runtime=False,
        logging_level=1,  # 0=None, 1=Basic, 2=Performance, 3=Verbose
        
        # Timeout
        timeout=3600,  # 1 hour
        
        dag=dag
    )
else:
    # Fallback if provider not installed
    def execute_ssis_fallback(**context):
        raise Exception("airflow-ssis-provider not installed. Run: pip install airflow-ssis-provider")
    
    execute_ssis = PythonOperator(
        task_id='execute_ssis_package',
        python_callable=execute_ssis_fallback,
        dag=dag
    )

# ============================================
# TASK 3: VERIFY EXECUTION
# ============================================
def verify_ssis_execution(**context):
    """Verify SSIS package executed successfully"""
    
    execution_date = context['ds']
    mssql_hook = MsSqlHook(mssql_conn_id=SSIS_CONN_ID)
    
    logging.info("Verifying SSIS execution...")
    
    # Get latest execution info
    execution_query = f"""
        SELECT TOP 1
            e.execution_id,
            e.status,
            e.start_time,
            e.end_time,
            DATEDIFF(SECOND, e.start_time, e.end_time) as duration_seconds,
            CASE e.status
                WHEN 1 THEN 'Created'
                WHEN 2 THEN 'Running'
                WHEN 3 THEN 'Canceled'
                WHEN 4 THEN 'Failed'
                WHEN 5 THEN 'Pending'
                WHEN 6 THEN 'Ended Unexpectedly'
                WHEN 7 THEN 'Succeeded'
                WHEN 8 THEN 'Stopping'
                WHEN 9 THEN 'Completed'
            END as status_text
        FROM SSISDB.catalog.executions e
        INNER JOIN SSISDB.catalog.folders f ON e.folder_name = f.name
        WHERE f.name = '{FOLDER_NAME}'
        AND e.project_name = '{PROJECT_NAME}'
        AND e.package_name = '{PACKAGE_NAME}'
        AND CAST(e.start_time AS DATE) = CAST(GETDATE() AS DATE)
        ORDER BY e.execution_id DESC;
    """
    
    result = mssql_hook.get_first(execution_query)
    
    if not result:
        raise Exception("No execution found for today")
    
    execution_id = result[0]
    status = result[1]
    status_text = result[5]
    duration = result[4]
    
    logging.info(f"Execution ID: {execution_id}")
    logging.info(f"Status: {status_text} ({status})")
    logging.info(f"Duration: {duration} seconds")
    
    # Check for success (status 7 = Succeeded)
    if status != 7:
        # Get error messages
        error_query = f"""
            SELECT TOP 5
                message_time,
                message,
                message_type
            FROM SSISDB.catalog.operation_messages
            WHERE operation_id = {execution_id}
            AND message_type = 120  -- Error
            ORDER BY message_time DESC;
        """
        
        errors = mssql_hook.get_records(error_query)
        
        error_msg = f"SSIS package execution failed with status: {status_text}"
        if errors:
            error_msg += "\n\nErrors:\n"
            for error in errors:
                error_msg += f"- {error[1]}\n"
        
        raise Exception(error_msg)
    
    # Get execution statistics
    stats_query = f"""
        SELECT 
            SUM(CASE WHEN message_type = 120 THEN 1 ELSE 0 END) as error_count,
            SUM(CASE WHEN message_type = 110 THEN 1 ELSE 0 END) as warning_count,
            SUM(CASE WHEN message_type = 70 THEN 1 ELSE 0 END) as info_count
        FROM SSISDB.catalog.operation_messages
        WHERE operation_id = {execution_id};
    """
    
    stats = mssql_hook.get_first(stats_query)
    
    if stats:
        logging.info(f"Errors: {stats[0]}")
        logging.info(f"Warnings: {stats[1]}")
        logging.info(f"Info messages: {stats[2]}")
    
    # Verify data was loaded
    data_check = f"""
        SELECT COUNT(*) 
        FROM stg.Sales 
        WHERE CAST(sale_date AS DATE) = '{execution_date}';
    """
    
    row_count = mssql_hook.get_first(data_check)
    
    if row_count and row_count[0] > 0:
        logging.info(f"✓ Data verified: {row_count[0]} rows loaded")
    else:
        logging.warning("⚠️ No data found in staging table")
    
    # Store results in XCom
    context['ti'].xcom_push(key='execution_id', value=execution_id)
    context['ti'].xcom_push(key='rows_loaded', value=row_count[0] if row_count else 0)
    context['ti'].xcom_push(key='duration', value=duration)
    
    logging.info("✅ Verification completed successfully")
    return True

verify_task = PythonOperator(
    task_id='verify_execution',
    python_callable=verify_ssis_execution,
    dag=dag
)

# ============================================
# TASK 4: GET DETAILED LOGS
# ============================================
def get_execution_logs(**context):
    """Retrieve detailed execution logs from SSISDB"""
    
    ti = context['ti']
    execution_id = ti.xcom_pull(task_ids='verify_execution', key='execution_id')
    
    if not execution_id:
        logging.warning("No execution ID found, skipping log retrieval")
        return
    
    mssql_hook = MsSqlHook(mssql_conn_id=SSIS_CONN_ID)
    
    # Get component-level statistics
    component_stats = f"""
        SELECT TOP 10
            package_path,
            execution_path,
            subcomponent_name,
            message_type,
            COUNT(*) as message_count
        FROM SSISDB.catalog.operation_messages
        WHERE operation_id = {execution_id}
        GROUP BY package_path, execution_path, subcomponent_name, message_type
        ORDER BY message_count DESC;
    """
    
    stats = mssql_hook.get_records(component_stats)
    
    if stats:
        logging.info("=" * 80)
        logging.info("COMPONENT EXECUTION STATISTICS")
        logging.info("=" * 80)
        for stat in stats:
            logging.info(f"Path: {stat[0]}")
            logging.info(f"Component: {stat[2]}")
            logging.info(f"Message Type: {stat[3]}, Count: {stat[4]}")
            logging.info("-" * 80)
    
    # Get latest messages
    latest_messages = f"""
        SELECT TOP 20
            message_time,
            CASE message_type
                WHEN 120 THEN 'ERROR'
                WHEN 110 THEN 'WARNING'
                WHEN 70 THEN 'INFO'
                ELSE CAST(message_type AS VARCHAR)
            END as type,
            message_source_name,
            message
        FROM SSISDB.catalog.operation_messages
        WHERE operation_id = {execution_id}
        AND message_type IN (70, 110, 120)  -- Info, Warning, Error
        ORDER BY message_time DESC;
    """
    
    messages = mssql_hook.get_records(latest_messages)
    
    if messages:
        logging.info("=" * 80)
        logging.info("LATEST EXECUTION MESSAGES")
        logging.info("=" * 80)
        for msg in messages:
            logging.info(f"[{msg[0]}] {msg[1]}: {msg[2]}")
            logging.info(f"  {msg[3][:200]}...")  # First 200 chars
            logging.info("-" * 80)
    
    return True

logs_task = PythonOperator(
    task_id='get_execution_logs',
    python_callable=get_execution_logs,
    dag=dag
)

# ============================================
# TASK 5: GENERATE EXECUTION REPORT
# ============================================
def generate_report(**context):
    """Generate execution summary report"""
    
    ti = context['ti']
    execution_date = context['ds']
    
    execution_id = ti.xcom_pull(task_ids='verify_execution', key='execution_id')
    rows_loaded = ti.xcom_pull(task_ids='verify_execution', key='rows_loaded')
    duration = ti.xcom_pull(task_ids='verify_execution', key='duration')
    
    report = f"""
    ╔════════════════════════════════════════════════════════════╗
    ║          SSIS EXECUTION REPORT                             ║
    ╚════════════════════════════════════════════════════════════╝
    
    📅 Execution Date: {execution_date}
    🔢 Execution ID: {execution_id}
    ⏱️  Duration: {duration} seconds
    📊 Rows Loaded: {rows_loaded}
    
    📦 Package Details:
       Folder: {FOLDER_NAME}
       Project: {PROJECT_NAME}
       Package: {PACKAGE_NAME}
       Environment: {ENVIRONMENT_NAME}
    
    ✅ Status: COMPLETED SUCCESSFULLY
    
    ════════════════════════════════════════════════════════════
    """
    
    logging.info(report)
    
    # Store report in XCom
    context['ti'].xcom_push(key='execution_report', value=report)
    
    return report

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag
)

# ============================================
# TASK 6: CLEANUP (Optional)
# ============================================
def cleanup_old_logs(**context):
    """Clean up old execution logs (optional)"""
    
    mssql_hook = MsSqlHook(mssql_conn_id=SSIS_CONN_ID)
    
    # Delete logs older than 30 days
    cleanup_sql = """
        DECLARE @retention_days INT = 30;
        
        -- Get executions to delete
        DECLARE @cutoff_date DATETIME = DATEADD(DAY, -@retention_days, GETDATE());
        
        -- This is just a demo - in production, be careful with cleanup
        -- DELETE FROM SSISDB.catalog.executions 
        -- WHERE start_time < @cutoff_date;
        
        -- For now, just log what would be deleted
        SELECT COUNT(*) as executions_to_cleanup
        FROM SSISDB.catalog.executions 
        WHERE start_time < @cutoff_date;
    """
    
    result = mssql_hook.get_first(cleanup_sql)
    
    if result:
        logging.info(f"Would clean up {result[0]} old executions (disabled for safety)")
    
    return True

cleanup_task = PythonOperator(
    task_id='cleanup_old_logs',
    python_callable=cleanup_old_logs,
    dag=dag
)

# ============================================
# TASK DEPENDENCIES
# ============================================
start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

# Linear flow
start >> validate_task >> execute_ssis >> verify_task

# Parallel post-processing
verify_task >> [logs_task, report_task]

# Cleanup and end
[logs_task, report_task] >> cleanup_task >> end
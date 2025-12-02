"""
Production SSIS Integration with Airflow
Connects to SQL Server on Windows Host and triggers SSIS packages
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.hooks.sql import DbApiHook
import time
import logging
import sys
import traceback

# Force logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

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
        logger.info(f"Initializing MsSqlHook with args: {args}, kwargs: {kwargs}")
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)
    
    def get_conn(self):
        """Returns a mssql connection object"""
        try:
            logger.info("=" * 60)
            logger.info("ATTEMPTING DATABASE CONNECTION")
            logger.info("=" * 60)
            
            import pymssql
            logger.info(f"✅ pymssql imported successfully, version: {pymssql.__version__}")
            
            logger.info(f"Retrieving connection: {self.mssql_conn_id}")
            conn = self.get_connection(self.mssql_conn_id)
            
            logger.info(f"Connection details retrieved:")
            logger.info(f"  Host: {conn.host}")
            logger.info(f"  Port: {conn.port or 1433}")
            logger.info(f"  Login: {conn.login}")
            logger.info(f"  Schema: {conn.schema or self.schema or 'master'}")
            
            logger.info("Connecting to SQL Server...")
            connection = pymssql.connect(
                server=conn.host,
                user=conn.login,
                password=conn.password,
                database=conn.schema or self.schema or 'master',
                port=conn.port or 1433,
                timeout=30,
                login_timeout=30
            )
            
            logger.info("✅ Connection established successfully!")
            logger.info("=" * 60)
            return connection
            
        except Exception as e:
            logger.error("=" * 60)
            logger.error("❌ CONNECTION FAILED")
            logger.error("=" * 60)
            logger.error(f"Error Type: {type(e).__name__}")
            logger.error(f"Error Message: {str(e)}")
            logger.error(f"Full Traceback:\n{traceback.format_exc()}")
            logger.error("=" * 60)
            raise

# ============================================
# DAG CONFIGURATION
# ============================================
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 20),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'retries': 1,  # Reduced for debugging
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'production_ssis_integration',
    default_args=default_args,
    description='Execute SSIS packages via SQL Server Agent from Airflow',
    catchup=False,
    max_active_runs=1,
    tags=['production', 'ssis', 'sql-server-agent']
)

# ============================================
# CONFIGURATION
# ============================================
MSSQL_CONN_ID = 'mssql_host'  # Connection to Windows host SQL Server
JOB_NAME = 'Airflow_SalesETL_Parametrized'
MAX_WAIT_SECONDS = 3600  # 1 hour timeout
CHECK_INTERVAL = 10  # Check every 10 seconds

# ============================================
# TASK 1: VALIDATE PREREQUISITES
# ============================================
def validate_prerequisites(**context):
    """Check if SQL Server Agent and SSIS package are ready"""
    
    try:
        logger.info("=" * 80)
        logger.info("STARTING PREREQUISITES VALIDATION")
        logger.info("=" * 80)
        
        logger.info(f"Connection ID to use: {MSSQL_CONN_ID}")
        logger.info(f"Job name to check: {JOB_NAME}")
        
        logger.info("\nCreating MsSqlHook instance...")
        mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        logger.info("✅ MsSqlHook created")
        
        logger.info("\nGetting database connection...")
        conn = mssql_hook.get_conn()
        logger.info("✅ Database connection obtained")
        
        # Check 1: SQL Server Agent is running
        logger.info("\n" + "=" * 80)
        logger.info("CHECK 1: SQL Server Agent Status")
        logger.info("=" * 80)
        
        agent_status_sql = """
            SELECT 
                servicename,
                status_desc,
                CASE 
                    WHEN status = 4 THEN 'Running'
                    ELSE 'Stopped'
                END as agent_status
            FROM sys.dm_server_services
            WHERE servicename LIKE 'SQL Server Agent%';
        """
        
        logger.info("Executing query...")
        cursor = conn.cursor()
        cursor.execute(agent_status_sql)
        result = cursor.fetchone()
        
        logger.info(f"Query result: {result}")
        
        if not result:
            logger.error("❌ No SQL Server Agent service found")
            raise Exception("SQL Server Agent service not found in sys.dm_server_services")
        
        service_name = result[0]
        status_desc = result[1]
        agent_status = result[2]
        
        logger.info(f"Service Name: {service_name}")
        logger.info(f"Status: {status_desc}")
        logger.info(f"Parsed Status: {agent_status}")
        
        if agent_status != 'Running':
            logger.error(f"❌ SQL Server Agent is not running (Status: {status_desc})")
            raise Exception(f"SQL Server Agent is not running. Current status: {status_desc}")
        
        logger.info("✅ SQL Server Agent is running")
        
        # Check 2: Job exists
        logger.info("\n" + "=" * 80)
        logger.info("CHECK 2: SQL Server Agent Job Exists")
        logger.info("=" * 80)
        
        job_exists_sql = f"""
            SELECT 
                name,
                enabled,
                date_created,
                date_modified
            FROM msdb.dbo.sysjobs 
            WHERE name = '{JOB_NAME}';
        """
        
        logger.info(f"Looking for job: {JOB_NAME}")
        cursor.execute(job_exists_sql)
        job_result = cursor.fetchone()
        
        if not job_result:
            logger.error(f"❌ Job '{JOB_NAME}' not found")
            
            # List all available jobs
            cursor.execute("SELECT name FROM msdb.dbo.sysjobs")
            all_jobs = cursor.fetchall()
            logger.info(f"Available jobs ({len(all_jobs)}):")
            for job in all_jobs[:10]:  # Show first 10
                logger.info(f"  - {job[0]}")
            
            raise Exception(f"SQL Server Agent job '{JOB_NAME}' not found")
        
        job_name = job_result[0]
        job_enabled = job_result[1]
        
        logger.info(f"Job Name: {job_name}")
        logger.info(f"Enabled: {job_enabled}")
        logger.info(f"Created: {job_result[2]}")
        logger.info(f"Modified: {job_result[3]}")
        
        if not job_enabled:
            logger.error(f"❌ Job '{JOB_NAME}' is disabled")
            raise Exception(f"SQL Server Agent job '{JOB_NAME}' is disabled")
        
        logger.info(f"✅ Job '{JOB_NAME}' exists and is enabled")
        
        # Check 3: SSIS package exists in SSISDB
        logger.info("\n" + "=" * 80)
        logger.info("CHECK 3: SSISDB Package Exists")
        logger.info("=" * 80)
        
        # First check if SSISDB exists
        cursor.execute("SELECT COUNT(*) FROM sys.databases WHERE name = 'SSISDB'")
        ssisdb_exists = cursor.fetchone()[0]
        
        if ssisdb_exists == 0:
            logger.error("❌ SSISDB catalog not found")
            raise Exception("SSISDB catalog does not exist. Please enable Integration Services.")
        
        logger.info("✅ SSISDB catalog exists")
        
        package_exists_sql = """
            SELECT 
                f.name as folder_name,
                pr.name as project_name,
                p.name as package_name,
                p.package_id
            FROM SSISDB.catalog.packages p
            INNER JOIN SSISDB.catalog.projects pr ON p.project_id = pr.project_id
            INNER JOIN SSISDB.catalog.folders f ON pr.folder_id = f.folder_id
            WHERE f.name = 'AirflowDemo' 
            AND pr.name = 'AirflowSSISDemo';
        """
        
        logger.info("Checking for SSIS packages...")
        cursor.execute(package_exists_sql)
        packages = cursor.fetchall()
        
        if not packages:
            logger.error("❌ No packages found in AirflowDemo/AirflowSSISDemo")
            
            # List available folders
            cursor.execute("SELECT name FROM SSISDB.catalog.folders")
            folders = cursor.fetchall()
            logger.info(f"Available folders in SSISDB:")
            for folder in folders:
                logger.info(f"  - {folder[0]}")
            
            raise Exception("SSIS package not found in SSISDB catalog")
        
        logger.info(f"Found {len(packages)} package(s):")
        for pkg in packages:
            logger.info(f"  - Folder: {pkg[0]}, Project: {pkg[1]}, Package: {pkg[2]}")
        
        logger.info("✅ SSIS packages found in SSISDB catalog")
        
        cursor.close()
        conn.close()
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ ALL PREREQUISITES VALIDATED SUCCESSFULLY")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error("\n" + "=" * 80)
        logger.error("❌ VALIDATION FAILED")
        logger.error("=" * 80)
        logger.error(f"Error Type: {type(e).__name__}")
        logger.error(f"Error Message: {str(e)}")
        logger.error(f"Full Traceback:\n{traceback.format_exc()}")
        logger.error("=" * 80)
        raise

validate_task = PythonOperator(
    task_id='validate_prerequisites',
    python_callable=validate_prerequisites,
    dag=dag
)

# ============================================
# TASK 2: START SSIS PACKAGE VIA SQL AGENT
# ============================================
def start_ssis_job(**context):
    """Start SQL Server Agent job with parameters"""
    
    try:
        execution_date = context['ds']
        logger.info(f"Starting SSIS job for execution date: {execution_date}")
        
        mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = mssql_hook.get_conn()
        cursor = conn.cursor()
        
        # Build parameter string
        parameters = f"ExecutionDate={execution_date};BatchSize=1000;Environment=Production"
        
        # Start job with parameters using sp_start_job
        start_job_sql = f"""
            DECLARE @job_id UNIQUEIDENTIFIER;
            
            SELECT @job_id = job_id 
            FROM msdb.dbo.sysjobs 
            WHERE name = '{JOB_NAME}';
            
            -- Check if job is already running
            IF EXISTS (
                SELECT 1 FROM msdb.dbo.sysjobactivity ja
                WHERE ja.job_id = @job_id
                AND ja.start_execution_date IS NOT NULL
                AND ja.stop_execution_date IS NULL
                AND ja.session_id = (SELECT MAX(session_id) FROM msdb.dbo.sysjobactivity)
            )
            BEGIN
                RAISERROR('Job is already running', 16, 1);
                RETURN;
            END
            
            -- Start the job
            EXEC msdb.dbo.sp_start_job 
                @job_name = '{JOB_NAME}';
            
            -- Wait a moment for job to start
            WAITFOR DELAY '00:00:02';
            
            -- Return the job execution ID
            SELECT 
                ja.job_history_id,
                ja.start_execution_date
            FROM msdb.dbo.sysjobactivity ja
            WHERE ja.job_id = @job_id
            AND ja.session_id = (SELECT MAX(session_id) FROM msdb.dbo.sysjobactivity);
        """
        
        logger.info("Executing job start command...")
        cursor.execute(start_job_sql)
        result = cursor.fetchone()
        
        if result:
            job_history_id = result[0]
            start_time = result[1]
            
            logger.info(f"✅ SSIS job started successfully")
            logger.info(f"Job History ID: {job_history_id}")
            logger.info(f"Start Time: {start_time}")
            
            # Store in XCom for monitoring
            context['ti'].xcom_push(key='job_start_time', value=str(start_time))
            
            cursor.close()
            conn.close()
            return True
        else:
            raise Exception("Failed to start job - no execution info returned")
            
    except Exception as e:
        logger.error(f"Failed to start SSIS job: {str(e)}")
        logger.error(f"Full Traceback:\n{traceback.format_exc()}")
        raise

start_job_task = PythonOperator(
    task_id='start_ssis_job',
    python_callable=start_ssis_job,
    dag=dag
)

# ============================================
# TASK 3: MONITOR JOB EXECUTION
# ============================================
def monitor_job_execution(**context):
    """Monitor SQL Server Agent job execution until completion"""
    
    mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    
    logger.info(f"Monitoring job execution (timeout: {MAX_WAIT_SECONDS}s)...")
    
    elapsed = 0
    last_message = ""
    
    while elapsed < MAX_WAIT_SECONDS:
        try:
            conn = mssql_hook.get_conn()
            cursor = conn.cursor()
            
            # Query job status
            status_sql = f"""
                SELECT TOP 1
                    h.run_status,
                    h.run_date,
                    h.run_time,
                    h.run_duration,
                    h.message,
                    CASE h.run_status
                        WHEN 0 THEN 'Failed'
                        WHEN 1 THEN 'Succeeded'
                        WHEN 2 THEN 'Retry'
                        WHEN 3 THEN 'Canceled'
                        WHEN 4 THEN 'In Progress'
                    END as status_text
                FROM msdb.dbo.sysjobs j
                INNER JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
                WHERE j.name = '{JOB_NAME}'
                AND h.step_id = 0  -- Job outcome (not individual steps)
                ORDER BY h.instance_id DESC;
            """
            
            cursor.execute(status_sql)
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if result:
                run_status = result[0]
                status_text = result[5]
                message = result[4]
                
                # Log progress if message changed
                if message != last_message:
                    logger.info(f"Status: {status_text}")
                    if message:
                        logger.info(f"Message: {message[:200]}...")
                    last_message = message
                
                # Check if job completed
                if run_status == 1:  # Succeeded
                    duration = result[3]
                    logger.info(f"✅ SSIS job completed successfully")
                    logger.info(f"Duration: {duration} seconds")
                    
                    # Store results in XCom
                    context['ti'].xcom_push(key='job_status', value='SUCCESS')
                    context['ti'].xcom_push(key='job_duration', value=duration)
                    context['ti'].xcom_push(key='job_message', value=message)
                    
                    return 'verify_execution'
                    
                elif run_status == 0:  # Failed
                    error_msg = message or "Job failed without error message"
                    logger.error(f"❌ SSIS job failed: {error_msg}")
                    
                    context['ti'].xcom_push(key='job_status', value='FAILED')
                    context['ti'].xcom_push(key='job_message', value=error_msg)
                    
                    raise Exception(f"SSIS job failed: {error_msg}")
                    
                elif run_status == 3:  # Canceled
                    logger.warning(f"⚠️ SSIS job was canceled")
                    raise Exception("SSIS job was canceled")
            
        except Exception as e:
            if "Job failed" in str(e) or "canceled" in str(e):
                raise
            logger.warning(f"Error checking job status: {e}")
        
        # Wait before next check
        time.sleep(CHECK_INTERVAL)
        elapsed += CHECK_INTERVAL
        
        # Log progress every minute
        if elapsed % 60 == 0:
            logger.info(f"Still monitoring... ({elapsed}/{MAX_WAIT_SECONDS}s elapsed)")
    
    # Timeout
    raise Exception(f"Job monitoring timeout after {MAX_WAIT_SECONDS} seconds")

monitor_job_task = BranchPythonOperator(
    task_id='monitor_job_execution',
    python_callable=monitor_job_execution,
    dag=dag
)

# ============================================
# TASK 4: VERIFY EXECUTION RESULTS
# ============================================
def verify_execution(**context):
    """Verify SSIS package execution results in database"""
    
    execution_date = context['ds']
    mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    
    logger.info("Verifying SSIS execution results...")
    
    conn = mssql_hook.get_conn()
    cursor = conn.cursor()
    
    # Check 1: Data was loaded
    check_data_sql = f"""
        SELECT 
            COUNT(*) as rows_loaded,
            MAX(created_at) as last_load_time
        FROM SalesDB.dbo.Sales
        WHERE CAST(sale_date AS DATE) = '{execution_date}';
    """
    
    cursor.execute(check_data_sql)
    result = cursor.fetchone()
    
    if result:
        rows_loaded = result[0]
        last_load_time = result[1]
        
        if rows_loaded == 0:
            raise ValueError(f"No data was loaded for {execution_date}")
        
        logger.info(f"✓ Rows loaded: {rows_loaded}")
        logger.info(f"✓ Last load time: {last_load_time}")
        
        # Check 2: Package execution logged in SSISDB
        check_log_sql = f"""
            SELECT TOP 1
                execution_id,
                status,
                start_time,
                end_time,
                DATEDIFF(SECOND, start_time, end_time) as duration_seconds
            FROM SSISDB.catalog.executions
            WHERE folder_name = 'AirflowDemo'
            AND project_name = 'AirflowSSISDemo'
            AND CAST(start_time AS DATE) = CAST(GETDATE() AS DATE)
            ORDER BY execution_id DESC;
        """
        
        cursor.execute(check_log_sql)
        log_result = cursor.fetchone()
        
        if log_result:
            execution_id = log_result[0]
            status = log_result[1]
            duration = log_result[4]
            
            if status != 7:  # Not Succeeded
                raise ValueError(f"SSIS execution status is not 'Succeeded': {status}")
            
            logger.info(f"✓ SSISDB Execution ID: {execution_id}")
            logger.info(f"✓ Status: Succeeded")
            logger.info(f"✓ Duration: {duration} seconds")
        
        # Store verification results
        context['ti'].xcom_push(key='rows_loaded', value=rows_loaded)
        context['ti'].xcom_push(key='verification_status', value='PASSED')
        
        cursor.close()
        conn.close()
        
        logger.info("✅ All verification checks passed")
        return True
        
    else:
        raise ValueError("Failed to verify execution - no data found")

verify_task = PythonOperator(
    task_id='verify_execution',
    python_callable=verify_execution,
    dag=dag
)

# ============================================
# TASK 5: GET DETAILED EXECUTION LOG
# ============================================
def get_execution_log(**context):
    """Retrieve detailed execution log from SSISDB"""
    
    mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    conn = mssql_hook.get_conn()
    cursor = conn.cursor()
    
    # Get latest execution details
    log_sql = """
        SELECT TOP 1
            e.execution_id,
            e.folder_name,
            e.project_name,
            e.package_name,
            e.status,
            e.start_time,
            e.end_time,
            DATEDIFF(SECOND, e.start_time, e.end_time) as duration_seconds
        FROM SSISDB.catalog.executions e
        WHERE e.folder_name = 'AirflowDemo'
        AND e.project_name = 'AirflowSSISDemo'
        AND CAST(e.start_time AS DATE) = CAST(GETDATE() AS DATE)
        ORDER BY e.execution_id DESC;
    """
    
    cursor.execute(log_sql)
    result = cursor.fetchone()
    
    if result:
        log_info = {
            'execution_id': result[0],
            'folder': result[1],
            'project': result[2],
            'package': result[3],
            'status': result[4],
            'start_time': str(result[5]),
            'end_time': str(result[6]),
            'duration_seconds': result[7]
        }
        
        logger.info("=" * 60)
        logger.info("SSIS EXECUTION LOG SUMMARY")
        logger.info("=" * 60)
        for key, value in log_info.items():
            logger.info(f"{key}: {value}")
        logger.info("=" * 60)
        
        # Store in XCom
        context['ti'].xcom_push(key='execution_log', value=log_info)
        
        cursor.close()
        conn.close()
        return log_info
    else:
        logger.warning("No execution log found")
        cursor.close()
        conn.close()
        return None

log_task = PythonOperator(
    task_id='get_execution_log',
    python_callable=get_execution_log,
    dag=dag
)

# ============================================
# TASK 6: CLEANUP (Optional)
# ============================================
cleanup_task = EmptyOperator(
    task_id='cleanup',
    dag=dag
)

# ============================================
# TASK 7: SEND NOTIFICATION
# ============================================
def send_notification(**context):
    """Send execution summary notification"""
    
    ti = context['ti']
    
    # Get results from previous tasks
    rows_loaded = ti.xcom_pull(task_ids='verify_execution', key='rows_loaded')
    job_duration = ti.xcom_pull(task_ids='monitor_job_execution', key='job_duration')
    execution_log = ti.xcom_pull(task_ids='get_execution_log', key='execution_log')
    
    summary = f"""
    SSIS Execution Summary
    =====================================================
    Execution Date: {context['ds']}
    
    Status: ✅ SUCCESS
    Rows Loaded: {rows_loaded}
    Duration: {job_duration} seconds
    
    SSISDB Execution ID: {execution_log.get('execution_id') if execution_log else 'N/A'}
    =====================================================
    """
    
    logger.info(summary)
    
    return True

notification_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification,
    trigger_rule='all_done',
    dag=dag
)

# ============================================
# TASK DEPENDENCIES
# ============================================
start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag, trigger_rule='all_done')

start >> validate_task >> start_job_task >> monitor_job_task

monitor_job_task >> verify_task >> log_task >> cleanup_task

[verify_task, cleanup_task] >> notification_task >> end
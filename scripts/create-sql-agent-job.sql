-- ============================================
-- CREATE SQL SERVER AGENT JOB FOR SSIS PACKAGE
-- ============================================

USE msdb;
GO

-- ============================================
-- 1. CREATE JOB CATEGORY (if not exists)
-- ============================================
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name = 'SSIS Packages' AND category_class = 1)
BEGIN
    EXEC msdb.dbo.sp_add_category
        @class = N'JOB',
        @type = N'LOCAL',
        @name = N'SSIS Packages';
END
GO

-- ============================================
-- 2. CREATE JOB
-- ============================================
DECLARE @jobId BINARY(16);
DECLARE @ReturnCode INT = 0;
DECLARE @jobName NVARCHAR(128) = N'Airflow_SalesETL_Job';

-- Delete job if exists
IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = @jobName)
BEGIN
    EXEC msdb.dbo.sp_delete_job @job_name = @jobName, @delete_unused_schedule = 1;
    PRINT 'Deleted existing job: ' + @jobName;
END

-- Create the job
EXEC @ReturnCode = msdb.dbo.sp_add_job
    @job_name = @jobName,
    @enabled = 1,
    @notify_level_eventlog = 2,  -- On failure
    @notify_level_email = 2,     -- On failure (if SQL Mail configured)
    @delete_level = 0,           -- Never delete
    @description = N'SSIS Sales ETL Job - Triggered by Apache Airflow',
    @category_name = N'SSIS Packages',
    @job_id = @jobId OUTPUT;

IF (@@ERROR <> 0 OR @ReturnCode <> 0) 
BEGIN
    PRINT 'Error creating job';
    GOTO QuitWithRollback;
END

PRINT 'Job created: ' + @jobName;
PRINT 'Job ID: ' + CAST(@jobId AS NVARCHAR(50));

-- ============================================
-- 3. ADD JOB STEP
-- ============================================
DECLARE @stepName NVARCHAR(128) = N'Run SSIS Package';

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep
    @job_id = @jobId,
    @step_name = @stepName,
    @step_id = 1,
    @cmdexec_success_code = 0,
    @on_success_action = 1,  -- Quit with success
    @on_fail_action = 2,     -- Quit with failure
    @retry_attempts = 2,
    @retry_interval = 5,
    @subsystem = N'SSIS',
    @command = N'/ISSERVER "\"\SSISDB\AirflowDemo\AirflowSSISDemo\SalesDataETL.dtsx\"" /SERVER "\"localhost\"" /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /Par "\"$ServerOption::SYNCHRONIZED(Boolean)\"";True /CALLERINFO SQLAGENT /REPORTING E',
    @database_name = N'master',
    @flags = 0;

IF (@@ERROR <> 0 OR @ReturnCode <> 0) 
BEGIN
    PRINT 'Error creating job step';
    GOTO QuitWithRollback;
END

PRINT 'Job step created: ' + @stepName;

-- ============================================
-- 4. ADD JOB SERVER
-- ============================================
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver
    @job_id = @jobId,
    @server_name = N'(local)';

IF (@@ERROR <> 0 OR @ReturnCode <> 0) 
BEGIN
    PRINT 'Error adding job server';
    GOTO QuitWithRollback;
END

PRINT 'Job server added';

-- ============================================
-- 5. CREATE JOB WITH PARAMETERS (Advanced)
-- ============================================
DECLARE @jobName2 NVARCHAR(128) = N'Airflow_SalesETL_Parametrized';
DECLARE @jobId2 BINARY(16);

-- Delete if exists
IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = @jobName2)
BEGIN
    EXEC msdb.dbo.sp_delete_job @job_name = @jobName2, @delete_unused_schedule = 1;
END

-- Create job
EXEC @ReturnCode = msdb.dbo.sp_add_job
    @job_name = @jobName2,
    @enabled = 1,
    @description = N'Parametrized SSIS Job - Can pass parameters from Airflow',
    @category_name = N'SSIS Packages',
    @job_id = @jobId2 OUTPUT;

-- Add step with parameter placeholders
-- Note: $(ESCAPE_SQUOTE(ExecutionDate)) will be replaced by actual value when called from Airflow
DECLARE @commandWithParams NVARCHAR(MAX) = 
    N'/ISSERVER "\"\SSISDB\AirflowDemo\AirflowSSISDemo\SalesDataETL.dtsx\"" ' +
    N'/SERVER "\"localhost\"" ' +
    N'/Par "\"ExecutionDate\"";$(ESCAPE_SQUOTE(ExecutionDate)) ' +
    N'/Par "\"BatchSize\"";$(ESCAPE_SQUOTE(BatchSize)) ' +
    N'/Par "\"Environment\"";$(ESCAPE_SQUOTE(Environment)) ' +
    N'/Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 ' +
    N'/Par "\"$ServerOption::SYNCHRONIZED(Boolean)\"";True ' +
    N'/CALLERINFO SQLAGENT ' +
    N'/REPORTING E';

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep
    @job_id = @jobId2,
    @step_name = N'Run SSIS with Parameters',
    @subsystem = N'SSIS',
    @command = @commandWithParams,
    @database_name = N'master';

EXEC msdb.dbo.sp_add_jobserver @job_id = @jobId2, @server_name = N'(local)';

PRINT 'Parametrized job created: ' + @jobName2;

-- ============================================
-- 6. VERIFICATION
-- ============================================
PRINT '============================================';
PRINT 'SQL SERVER AGENT JOBS CREATED';
PRINT '============================================';

SELECT 
    j.job_id,
    j.name as job_name,
    j.enabled,
    j.description,
    j.date_created,
    c.name as category_name
FROM msdb.dbo.sysjobs j
LEFT JOIN msdb.dbo.syscategories c ON j.category_id = c.category_id
WHERE j.name IN (N'Airflow_SalesETL_Job', N'Airflow_SalesETL_Parametrized')
ORDER BY j.date_created DESC;

-- List job steps
SELECT 
    j.name as job_name,
    s.step_id,
    s.step_name,
    s.subsystem,
    s.command
FROM msdb.dbo.sysjobs j
INNER JOIN msdb.dbo.sysjobsteps s ON j.job_id = s.job_id
WHERE j.name IN (N'Airflow_SalesETL_Job', N'Airflow_SalesETL_Parametrized')
ORDER BY j.name, s.step_id;

PRINT '============================================';
PRINT 'NEXT STEPS:';
PRINT '1. Test job execution manually:';
PRINT '   EXEC msdb.dbo.sp_start_job @job_name = ''Airflow_SalesETL_Job''';
PRINT '2. Configure Airflow to trigger this job';
PRINT '3. Monitor execution in SQL Server Agent Job History';
PRINT '============================================';

GOTO EndSave;

QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION;
    PRINT 'Job creation failed!';

EndSave:
GO

-- ============================================
-- 7. TEST JOB EXECUTION
-- ============================================
-- Uncomment to test
-- EXEC msdb.dbo.sp_start_job @job_name = N'Airflow_SalesETL_Job';
-- GO

-- ============================================
-- 8. HELPER QUERIES
-- ============================================

-- Check job status
/*
SELECT 
    j.name AS job_name,
    ja.run_requested_date,
    ja.start_execution_date,
    ja.stop_execution_date,
    ja.job_history_id,
    CASE ja.last_executed_step_id
        WHEN 0 THEN 'Not running'
        ELSE 'Step ' + CAST(ja.last_executed_step_id AS VARCHAR(10))
    END AS current_step
FROM msdb.dbo.sysjobs j
INNER JOIN msdb.dbo.sysjobactivity ja ON j.job_id = ja.job_id
WHERE j.name = 'Airflow_SalesETL_Job'
    AND ja.session_id = (SELECT MAX(session_id) FROM msdb.dbo.sysjobactivity);
*/

-- View job history
/*
SELECT TOP 10
    j.name AS job_name,
    h.step_id,
    h.step_name,
    h.run_status,
    CASE h.run_status
        WHEN 0 THEN 'Failed'
        WHEN 1 THEN 'Succeeded'
        WHEN 2 THEN 'Retry'
        WHEN 3 THEN 'Canceled'
        WHEN 4 THEN 'In Progress'
    END AS status_text,
    h.message,
    CAST(h.run_date AS VARCHAR(8)) + ' ' + 
    STUFF(STUFF(RIGHT('000000' + CAST(h.run_time AS VARCHAR(6)), 6), 5, 0, ':'), 3, 0, ':') AS run_datetime,
    h.run_duration
FROM msdb.dbo.sysjobs j
INNER JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
WHERE j.name = 'Airflow_SalesETL_Job'
ORDER BY h.instance_id DESC;
*/
GO
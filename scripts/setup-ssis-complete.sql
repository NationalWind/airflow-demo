-- ============================================
-- COMPLETE SSIS SETUP FOR AIRFLOW-SSIS-PROVIDER
-- Run this on Windows Host SQL Server
-- ============================================

USE master;
GO

-- ============================================
-- STEP 1: CREATE SSISDB CATALOG
-- ============================================
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'SSISDB')
BEGIN
    PRINT 'Creating SSISDB Catalog...';
    
    -- Enable CLR
    EXEC sp_configure 'clr enabled', 1;
    RECONFIGURE;
    
    -- Create catalog
    EXEC catalog.create_catalog 
        @password = N'TQP@91204';
    
    PRINT '✓ SSISDB Catalog created';
END
ELSE
BEGIN
    PRINT '✓ SSISDB Catalog already exists';
END
GO

USE SSISDB;
GO

-- ============================================
-- STEP 2: CREATE FOLDER
-- ============================================
DECLARE @folder_name NVARCHAR(128) = N'AirflowDemo';
DECLARE @folder_id BIGINT;

IF NOT EXISTS (SELECT 1 FROM catalog.folders WHERE name = @folder_name)
BEGIN
    EXEC catalog.create_folder 
        @folder_name = @folder_name,
        @folder_id = @folder_id OUTPUT;
    
    PRINT 'Folder created: ' + @folder_name;
END
ELSE
BEGIN
    PRINT 'Folder already exists: ' + @folder_name;
END
GO

-- ============================================
-- STEP 3: CREATE ENVIRONMENT
-- ============================================
DECLARE @environment_name NVARCHAR(128) = N'Production';
DECLARE @folder_name NVARCHAR(128) = N'AirflowDemo';

IF NOT EXISTS (
    SELECT 1 
    FROM catalog.environments e
    INNER JOIN catalog.folders f ON e.folder_id = f.folder_id
    WHERE e.name = @environment_name AND f.name = @folder_name
)
BEGIN
    EXEC catalog.create_environment
        @environment_name = @environment_name,
        @environment_description = N'Production environment for Airflow SSIS integration',
        @folder_name = @folder_name;
    
    PRINT 'Environment created: ' + @environment_name;
END
ELSE
BEGIN
    PRINT 'Environment already exists: ' + @environment_name;
END
GO

-- ============================================
-- STEP 4: CREATE ENVIRONMENT VARIABLES
-- ============================================
DECLARE @folder_name NVARCHAR(128) = N'AirflowDemo';
DECLARE @environment_name NVARCHAR(128) = N'Production';

-- Variable 1: ExecutionDate
IF NOT EXISTS (
    SELECT 1 FROM catalog.environment_variables ev
    INNER JOIN catalog.environments e ON ev.environment_id = e.environment_id
    INNER JOIN catalog.folders f ON e.folder_id = f.folder_id
    WHERE ev.name = 'ExecutionDate' AND e.name = @environment_name AND f.name = @folder_name
)
BEGIN
    EXEC catalog.create_environment_variable
        @variable_name = N'ExecutionDate',
        @sensitive = 0,
        @description = N'Date to process data for',
        @environment_name = @environment_name,
        @folder_name = @folder_name,
        @value = N'2025-11-26',
        @data_type = N'String';
    PRINT '✓ Variable created: ExecutionDate';
END

-- Variable 2: BatchSize
IF NOT EXISTS (
    SELECT 1 FROM catalog.environment_variables ev
    INNER JOIN catalog.environments e ON ev.environment_id = e.environment_id
    INNER JOIN catalog.folders f ON e.folder_id = f.folder_id
    WHERE ev.name = 'BatchSize' AND e.name = @environment_name AND f.name = @folder_name
)
BEGIN
    EXEC catalog.create_environment_variable
        @variable_name = N'BatchSize',
        @sensitive = 0,
        @description = N'Number of rows per batch',
        @environment_name = @environment_name,
        @folder_name = @folder_name,
        @value = 1000,
        @data_type = N'Int32';
    PRINT '✓ Variable created: BatchSize';
END

-- Variable 3: Environment
IF NOT EXISTS (
    SELECT 1 FROM catalog.environment_variables ev
    INNER JOIN catalog.environments e ON ev.environment_id = e.environment_id
    INNER JOIN catalog.folders f ON e.folder_id = f.folder_id
    WHERE ev.name = 'Environment' AND e.name = @environment_name AND f.name = @folder_name
)
BEGIN
    EXEC catalog.create_environment_variable
        @variable_name = N'Environment',
        @sensitive = 0,
        @description = N'Deployment environment',
        @environment_name = @environment_name,
        @folder_name = @folder_name,
        @value = N'Production',
        @data_type = N'String';
    PRINT '✓ Variable created: Environment';
END

-- Variable 4: SourceConnectionString
IF NOT EXISTS (
    SELECT 1 FROM catalog.environment_variables ev
    INNER JOIN catalog.environments e ON ev.environment_id = e.environment_id
    INNER JOIN catalog.folders f ON e.folder_id = f.folder_id
    WHERE ev.name = 'SourceConnectionString' AND e.name = @environment_name AND f.name = @folder_name
)
BEGIN
    EXEC catalog.create_environment_variable
        @variable_name = N'SourceConnectionString',
        @sensitive = 1,  -- Sensitive data
        @description = N'Source database connection string',
        @environment_name = @environment_name,
        @folder_name = @folder_name,
        @value = N'Data Source=localhost;Initial Catalog=SalesDB;User ID=sa;Password=YourStrong@Passw0rd;',
        @data_type = N'String';
    PRINT '✓ Variable created: SourceConnectionString';
END

GO

-- ============================================
-- STEP 5: VERIFICATION
-- ============================================
PRINT '';
PRINT '============================================';
PRINT 'SSIS CATALOG SETUP COMPLETE';
PRINT '============================================';
PRINT '';

-- Display folders
PRINT 'Folders:';
SELECT 
    folder_id,
    name,
    description,
    created_time
FROM catalog.folders
WHERE name = 'AirflowDemo';

PRINT '';
PRINT 'Environments:';
SELECT 
    e.environment_id,
    f.name as folder_name,
    e.name as environment_name,
    e.description
FROM catalog.environments e
INNER JOIN catalog.folders f ON e.folder_id = f.folder_id
WHERE f.name = 'AirflowDemo';

PRINT '';
PRINT 'Environment Variables:';
SELECT 
    f.name as folder_name,
    e.name as environment_name,
    v.name as variable_name,
    v.type,
    v.sensitive,
    CASE WHEN v.sensitive = 1 THEN '***HIDDEN***' ELSE CAST(v.value AS NVARCHAR(MAX)) END as value
FROM catalog.environment_variables v
INNER JOIN catalog.environments e ON v.environment_id = e.environment_id
INNER JOIN catalog.folders f ON e.folder_id = f.folder_id
WHERE f.name = 'AirflowDemo'
ORDER BY e.name, v.name;

PRINT '';
PRINT '============================================';
PRINT 'NEXT STEPS:';
PRINT '1. Create SSIS package in Visual Studio';
PRINT '2. Deploy package to SSISDB/AirflowDemo folder';
PRINT '3. Configure Airflow connection';
PRINT '4. Test execution from Airflow';
PRINT '============================================';
GO

SELECT * FROM sys.assemblies WHERE name = 'ISSERVER';

EXEC sp_configure 'clr enabled', 1;
RECONFIGURE;

EXEC catalog.create_catalog @password = 'TQP@91204';
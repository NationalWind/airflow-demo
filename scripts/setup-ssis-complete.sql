-- ============================================
-- SETUP SSIS CATALOG (SSISDB)
-- ============================================

USE SSISDB;
GO

-- ============================================
-- 1. CREATE FOLDER FOR PROJECTS
-- ============================================
DECLARE @folder_id BIGINT;
DECLARE @folder_name NVARCHAR(128) = N'AirflowDemo';

-- Check if folder exists
IF NOT EXISTS (SELECT 1 FROM catalog.folders WHERE name = @folder_name)
BEGIN
    EXEC catalog.create_folder 
        @folder_name = @folder_name,
        @folder_id = @folder_id OUTPUT;
    
    PRINT 'Folder created: ' + @folder_name;
    PRINT 'Folder ID: ' + CAST(@folder_id AS NVARCHAR(20));
END
ELSE
BEGIN
    SELECT @folder_id = folder_id 
    FROM catalog.folders 
    WHERE name = @folder_name;
    
    PRINT 'Folder already exists: ' + @folder_name;
END
GO

-- ============================================
-- 2. CREATE ENVIRONMENT
-- ============================================
DECLARE @environment_name NVARCHAR(128) = N'Production';
DECLARE @folder_name NVARCHAR(128) = N'AirflowDemo';

-- Check if environment exists
IF NOT EXISTS (
    SELECT 1 
    FROM catalog.environments e
    INNER JOIN catalog.folders f ON e.folder_id = f.folder_id
    WHERE e.name = @environment_name AND f.name = @folder_name
)
BEGIN
    EXEC catalog.create_environment
        @environment_name = @environment_name,
        @environment_description = N'Production environment for Airflow-triggered packages',
        @folder_name = @folder_name;
    
    PRINT 'Environment created: ' + @environment_name;
END
ELSE
BEGIN
    PRINT 'Environment already exists: ' + @environment_name;
END
GO

-- ============================================
-- 3. ADD ENVIRONMENT VARIABLES
-- ============================================
DECLARE @folder_name NVARCHAR(128) = N'AirflowDemo';
DECLARE @environment_name NVARCHAR(128) = N'Production';
DECLARE @var_name NVARCHAR(128);
DECLARE @var_value SQL_VARIANT;

-- Source Connection String
SET @var_name = N'SourceConnectionString';
SET @var_value = N'Data Source=sqlserver-source,1433;Initial Catalog=SalesDB;User ID=sa;Password=TQP@91204;';

IF NOT EXISTS (
    SELECT 1 FROM catalog.environment_variables 
    WHERE environment_id = (
        SELECT environment_id FROM catalog.environments 
        WHERE name = @environment_name AND folder_id = (
            SELECT folder_id FROM catalog.folders WHERE name = @folder_name
        )
    ) AND name = @var_name
)
BEGIN
    EXEC catalog.create_environment_variable
        @variable_name = @var_name,
        @sensitive = 0,
        @description = N'Source database connection string',
        @environment_name = @environment_name,
        @folder_name = @folder_name,
        @value = @var_value,
        @data_type = N'String';
    PRINT 'Variable created: ' + @var_name;
END

-- Target Connection String
SET @var_name = N'TargetConnectionString';
SET @var_value = N'Host=postgres-target;Port=5432;Database=analytics_db;Username=analytics;Password=analytics123;';

IF NOT EXISTS (
    SELECT 1 FROM catalog.environment_variables 
    WHERE environment_id = (
        SELECT environment_id FROM catalog.environments 
        WHERE name = @environment_name AND folder_id = (
            SELECT folder_id FROM catalog.folders WHERE name = @folder_name
        )
    ) AND name = @var_name
)
BEGIN
    EXEC catalog.create_environment_variable
        @variable_name = @var_name,
        @sensitive = 0,
        @description = N'Target database connection string',
        @environment_name = @environment_name,
        @folder_name = @folder_name,
        @value = @var_value,
        @data_type = N'String';
    PRINT 'Variable created: ' + @var_name;
END

-- Batch Size
SET @var_name = N'BatchSize';
SET @var_value = 1000;

IF NOT EXISTS (
    SELECT 1 FROM catalog.environment_variables 
    WHERE environment_id = (
        SELECT environment_id FROM catalog.environments 
        WHERE name = @environment_name AND folder_id = (
            SELECT folder_id FROM catalog.folders WHERE name = @folder_name
        )
    ) AND name = @var_name
)
BEGIN
    EXEC catalog.create_environment_variable
        @variable_name = @var_name,
        @sensitive = 0,
        @description = N'Number of rows per batch',
        @environment_name = @environment_name,
        @folder_name = @folder_name,
        @value = @var_value,
        @data_type = N'Int32';
    PRINT 'Variable created: ' + @var_name;
END

GO

-- ============================================
-- 4. VERIFY SETUP
-- ============================================
PRINT '============================================';
PRINT 'SSISDB SETUP VERIFICATION';
PRINT '============================================';

-- List folders
SELECT 
    folder_id,
    name as folder_name,
    description,
    created_time
FROM catalog.folders
WHERE name = 'AirflowDemo';

-- List environments
SELECT 
    e.environment_id,
    f.name as folder_name,
    e.name as environment_name,
    e.description,
    e.created_time
FROM catalog.environments e
INNER JOIN catalog.folders f ON e.folder_id = f.folder_id
WHERE f.name = 'AirflowDemo';

-- List environment variables
SELECT 
    f.name as folder_name,
    e.name as environment_name,
    v.name as variable_name,
    v.type,
    v.description,
    v.value,
    v.sensitive
FROM catalog.environment_variables v
INNER JOIN catalog.environments e ON v.environment_id = e.environment_id
INNER JOIN catalog.folders f ON e.folder_id = f.folder_id
WHERE f.name = 'AirflowDemo'
ORDER BY e.name, v.name;

PRINT '============================================';
PRINT 'Setup completed successfully!';
PRINT 'Next step: Deploy SSIS packages to this folder';
PRINT '============================================';
GO
param(
    [string]$SqlServerInstance = "localhost",
    [string]$IspacFilePath = "E:\Everything\Works\HCMUSProject\BI\airflow-demo\airflow-demo\bin\Development\Development\airflow-demo.ispac",
    [string]$FolderName = "AirflowDemo",
    [string]$ProjectName = "AirflowSSISDemo",
    [string]$EnvironmentName = "Production"
)

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "SSIS Package Deployment Script" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# Load SSIS Assembly
Write-Host "`nLoading SSIS assemblies..." -ForegroundColor Yellow
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Management.IntegrationServices") | Out-Null

try {
    # Connect to SQL Server
    Write-Host "Connecting to SQL Server: $SqlServerInstance..." -ForegroundColor Yellow
    $sqlConnectionString = "Data Source=$SqlServerInstance;Initial Catalog=master;Integrated Security=SSPI;"
    $sqlConnection = New-Object System.Data.SqlClient.SqlConnection $sqlConnectionString
    $sqlConnection.Open()
    
    # Create Integration Services object
    $integrationServices = New-Object Microsoft.SqlServer.Management.IntegrationServices.IntegrationServices $sqlConnection
    
    # Get SSIS Catalog
    $catalog = $integrationServices.Catalogs["SSISDB"]
    if (!$catalog) {
        throw "SSISDB catalog does not exist. Please create it first."
    }
    Write-Host "✓ Connected to SSISDB catalog" -ForegroundColor Green
    
    # Get or create folder
    Write-Host "`nChecking folder: $FolderName..." -ForegroundColor Yellow
    $folder = $catalog.Folders[$FolderName]
    if (!$folder) {
        Write-Host "Creating folder: $FolderName..." -ForegroundColor Yellow
        $folder = New-Object Microsoft.SqlServer.Management.IntegrationServices.CatalogFolder($catalog, $FolderName, "Folder for Airflow-triggered SSIS packages")
        $folder.Create()
        Write-Host "✓ Folder created" -ForegroundColor Green
    } else {
        Write-Host "✓ Folder exists" -ForegroundColor Green
    }
    
    # Read ISPAC file
    Write-Host "`nReading ISPAC file: $IspacFilePath..." -ForegroundColor Yellow
    if (!(Test-Path $IspacFilePath)) {
        throw "ISPAC file not found: $IspacFilePath"
    }
    
    [byte[]]$projectFile = [System.IO.File]::ReadAllBytes($IspacFilePath)
    Write-Host "✓ ISPAC file loaded ($($projectFile.Length) bytes)" -ForegroundColor Green
    
    # Deploy project
    Write-Host "`nDeploying project: $ProjectName..." -ForegroundColor Yellow
    
    # Check if project already exists
    $existingProject = $folder.Projects[$ProjectName]
    if ($existingProject) {
        Write-Host "Project already exists. Removing old version..." -ForegroundColor Yellow
        $existingProject.Drop()
    }
    
    # Deploy new version
    $folder.DeployProject($ProjectName, $projectFile)
    Write-Host "✓ Project deployed successfully" -ForegroundColor Green
    
    # Create environment reference
    Write-Host "`nCreating environment reference..." -ForegroundColor Yellow
    $project = $folder.Projects[$ProjectName]
    
    $environment = $folder.Environments[$EnvironmentName]
    if (!$environment) {
        Write-Host "Warning: Environment '$EnvironmentName' not found. Please create it manually." -ForegroundColor Yellow
    } else {
        # Check if reference already exists
        $existingReference = $project.References[$EnvironmentName, $folder.Name]
        if (!$existingReference) {
            $project.References.Add($EnvironmentName, $folder.Name)
            $project.Alter()
            Write-Host "✓ Environment reference created" -ForegroundColor Green
        } else {
            Write-Host "✓ Environment reference already exists" -ForegroundColor Green
        }
    }
    
    # List packages in project
    Write-Host "`nPackages in project:" -ForegroundColor Cyan
    foreach ($package in $project.Packages) {
        Write-Host "  - $($package.Name)" -ForegroundColor White
    }
    
    # Summary
    Write-Host "`n============================================" -ForegroundColor Cyan
    Write-Host "DEPLOYMENT COMPLETED SUCCESSFULLY!" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Cyan
    Write-Host "Server: $SqlServerInstance" -ForegroundColor White
    Write-Host "Folder: $FolderName" -ForegroundColor White
    Write-Host "Project: $ProjectName" -ForegroundColor White
    Write-Host "Environment: $EnvironmentName" -ForegroundColor White
    Write-Host "`nNext steps:" -ForegroundColor Yellow
    Write-Host "1. Configure environment variables" -ForegroundColor White
    Write-Host "2. Map package parameters to environment variables" -ForegroundColor White
    Write-Host "3. Test package execution" -ForegroundColor White
    Write-Host "4. Create SQL Server Agent job (optional)" -ForegroundColor White
    
} catch {
    Write-Host "`n❌ ERROR: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host $_.Exception.StackTrace -ForegroundColor Red
    exit 1
} finally {
    if ($sqlConnection) {
        $sqlConnection.Close()
    }
}

#!/bin/bash

# ============================================
# Setup SQL Server Database
# ============================================

echo "Waiting for SQL Server to be ready..."
sleep 30

echo "Running initialization script..."
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "TQP@91204" -i /scripts/init-sqlserver.sql

echo "SQL Server setup completed!"
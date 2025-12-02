-- ============================================
-- CREATE STAGING DATABASE
-- ============================================
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'SalesDB_Stage')
BEGIN
    CREATE DATABASE SalesDB_Stage;
END
GO

USE SalesDB_Stage;
GO

-- ============================================
-- TABLE: Stage_Customers
-- ============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Stage_Customers')
BEGIN
    CREATE TABLE Stage_Customers (
        stage_id INT PRIMARY KEY IDENTITY(1,1),
        customer_id INT,
        customer_name NVARCHAR(100),
        email NVARCHAR(100),
        city NVARCHAR(50),
        country NVARCHAR(50),
        created_date DATE,
        loaded_at DATETIME DEFAULT GETDATE(),
        load_status NVARCHAR(20) DEFAULT 'New'
    );
END
GO

-- ============================================
-- TABLE: Stage_Products
-- ============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Stage_Products')
BEGIN
    CREATE TABLE Stage_Products (
        stage_id INT PRIMARY KEY IDENTITY(1,1),
        product_id INT,
        product_name NVARCHAR(100),
        category NVARCHAR(50),
        unit_price DECIMAL(10,2),
        stock_quantity INT,
        loaded_at DATETIME DEFAULT GETDATE(),
        load_status NVARCHAR(20) DEFAULT 'New'
    );
END
GO

-- ============================================
-- TABLE: Stage_Sales
-- ============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Stage_Sales')
BEGIN
    CREATE TABLE Stage_Sales (
        sale_id INT,
        customer_id INT,
        product_id INT,
        quantity INT,
        unit_price DECIMAL(10,2),
        total_amount DECIMAL(10,2),
        sale_date DATE,
        load_timestamp DATETIME,
        batch_id INT,
        loaded_by VARCHAR(100)
    );
    END
GO

-- ============================================
-- INITIALIZE SQL SERVER SOURCE DATABASE
-- ============================================

-- Create Sales Database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'SalesDB')
BEGIN
    CREATE DATABASE SalesDB;
END
GO

USE SalesDB;
GO

-- ============================================
-- TABLE: Customers
-- ============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Customers')
BEGIN
    CREATE TABLE Customers (
        customer_id INT PRIMARY KEY IDENTITY(1,1),
        customer_name NVARCHAR(100) NOT NULL,
        email NVARCHAR(100),
        city NVARCHAR(50),
        country NVARCHAR(50),
        created_date DATE DEFAULT GETDATE()
    );
END
GO

-- ============================================
-- TABLE: Products
-- ============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Products')
BEGIN
    CREATE TABLE Products (
        product_id INT PRIMARY KEY IDENTITY(1,1),
        product_name NVARCHAR(100) NOT NULL,
        category NVARCHAR(50),
        unit_price DECIMAL(10,2) NOT NULL,
        stock_quantity INT DEFAULT 0
    );
END
GO

-- ============================================
-- TABLE: Sales (Raw Transaction Data)
-- ============================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Sales')
BEGIN
    CREATE TABLE Sales (
        sale_id INT PRIMARY KEY IDENTITY(1,1),
        customer_id INT NOT NULL,
        product_id INT NOT NULL,
        quantity INT NOT NULL,
        unit_price DECIMAL(10,2) NOT NULL,
        total_amount DECIMAL(10,2) NOT NULL,
        sale_date DATE NOT NULL,
        created_at DATETIME DEFAULT GETDATE(),
        FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
        FOREIGN KEY (product_id) REFERENCES Products(product_id)
    );
END
GO

-- ============================================
-- INSERT SAMPLE DATA - Customers
-- ============================================
IF NOT EXISTS (SELECT * FROM Customers)
BEGIN
    SET IDENTITY_INSERT Customers ON;
    
    INSERT INTO Customers (customer_id, customer_name, email, city, country, created_date) VALUES
    (1, N'Nguyen Van A', 'nguyenvana@email.com', N'Ho Chi Minh', N'Vietnam', '2025-01-01'),
    (2, N'Tran Thi B', 'tranthib@email.com', N'Ha Noi', N'Vietnam', '2025-01-02'),
    (3, N'Le Van C', 'levanc@email.com', N'Da Nang', N'Vietnam', '2025-01-03'),
    (4, N'Pham Thi D', 'phamthid@email.com', N'Can Tho', N'Vietnam', '2025-01-04'),
    (5, N'Hoang Van E', 'hoangvane@email.com', N'Hai Phong', N'Vietnam', '2025-01-05');
    
    SET IDENTITY_INSERT Customers OFF;
END
GO

-- ============================================
-- INSERT SAMPLE DATA - Products
-- ============================================
IF NOT EXISTS (SELECT * FROM Products)
BEGIN
    SET IDENTITY_INSERT Products ON;
    
    INSERT INTO Products (product_id, product_name, category, unit_price, stock_quantity) VALUES
    (1, N'Laptop Dell XPS 13', N'Electronics', 25000000, 50),
    (2, N'iPhone 15 Pro', N'Electronics', 30000000, 100),
    (3, N'Samsung Galaxy S24', N'Electronics', 22000000, 80),
    (4, N'Sony WH-1000XM5', N'Audio', 8000000, 150),
    (5, N'iPad Pro 12.9', N'Electronics', 28000000, 60),
    (6, N'MacBook Air M3', N'Electronics', 32000000, 40),
    (7, N'AirPods Pro', N'Audio', 6000000, 200),
    (8, N'Samsung Monitor 27"', N'Electronics', 7000000, 70),
    (9, N'Logitech MX Master 3', N'Accessories', 2000000, 300),
    (10, N'Apple Watch Series 9', N'Wearables', 10000000, 120);
    
    SET IDENTITY_INSERT Products OFF;
END
GO

-- ============================================
-- INSERT SAMPLE DATA - Sales (Last 7 days)
-- ============================================
IF NOT EXISTS (SELECT * FROM Sales)
BEGIN
    DECLARE @today DATE = CAST(GETDATE() AS DATE);
    DECLARE @counter INT = 0;
    
    SET IDENTITY_INSERT Sales ON;
    
    -- Generate sales for last 7 days
    WHILE @counter < 7
    BEGIN
        DECLARE @sale_date DATE = DATEADD(DAY, -@counter, @today);
        
        INSERT INTO Sales (sale_id, customer_id, product_id, quantity, unit_price, total_amount, sale_date, created_at) VALUES
        (@counter * 10 + 1, 1, 1, 1, 25000000, 25000000, @sale_date, GETDATE()),
        (@counter * 10 + 2, 2, 2, 2, 30000000, 60000000, @sale_date, GETDATE()),
        (@counter * 10 + 3, 3, 4, 1, 8000000, 8000000, @sale_date, GETDATE()),
        (@counter * 10 + 4, 4, 7, 3, 6000000, 18000000, @sale_date, GETDATE()),
        (@counter * 10 + 5, 5, 9, 5, 2000000, 10000000, @sale_date, GETDATE()),
        (@counter * 10 + 6, 1, 5, 1, 28000000, 28000000, @sale_date, GETDATE()),
        (@counter * 10 + 7, 2, 3, 1, 22000000, 22000000, @sale_date, GETDATE()),
        (@counter * 10 + 8, 3, 6, 1, 32000000, 32000000, @sale_date, GETDATE()),
        (@counter * 10 + 9, 4, 8, 2, 7000000, 14000000, @sale_date, GETDATE()),
        (@counter * 10 + 10, 5, 10, 1, 10000000, 10000000, @sale_date, GETDATE());
        
        SET @counter = @counter + 1;
    END
    
    SET IDENTITY_INSERT Sales OFF;
END
GO

-- ============================================
-- CREATE VIEW: Daily Sales Summary
-- ============================================
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_DailySalesSummary')
    DROP VIEW vw_DailySalesSummary;
GO

CREATE VIEW vw_DailySalesSummary AS
SELECT 
    sale_date,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(*) as total_orders,
    SUM(quantity) as total_items_sold,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    MIN(total_amount) as min_order_value,
    MAX(total_amount) as max_order_value
FROM Sales
GROUP BY sale_date;
GO

-- ============================================
-- CREATE STORED PROCEDURE: Get Sales by Date Range
-- ============================================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_GetSalesByDateRange')
    DROP PROCEDURE sp_GetSalesByDateRange;
GO

CREATE PROCEDURE sp_GetSalesByDateRange
    @start_date DATE,
    @end_date DATE
AS
BEGIN
    SELECT 
        s.sale_id,
        s.sale_date,
        c.customer_name,
        c.city,
        p.product_name,
        p.category,
        s.quantity,
        s.unit_price,
        s.total_amount
    FROM Sales s
    INNER JOIN Customers c ON s.customer_id = c.customer_id
    INNER JOIN Products p ON s.product_id = p.product_id
    WHERE s.sale_date BETWEEN @start_date AND @end_date
    ORDER BY s.sale_date DESC, s.sale_id;
END
GO

-- ============================================
-- VERIFICATION QUERIES
-- ============================================
PRINT '============================================';
PRINT 'DATABASE INITIALIZATION COMPLETED';
PRINT '============================================';

PRINT 'Total Customers: ' + CAST((SELECT COUNT(*) FROM Customers) AS VARCHAR(10));
PRINT 'Total Products: ' + CAST((SELECT COUNT(*) FROM Products) AS VARCHAR(10));
PRINT 'Total Sales: ' + CAST((SELECT COUNT(*) FROM Sales) AS VARCHAR(10));

PRINT '============================================';
PRINT 'Sample Data from vw_DailySalesSummary:';
SELECT TOP 5 * FROM vw_DailySalesSummary ORDER BY sale_date DESC;

PRINT '============================================';
GO
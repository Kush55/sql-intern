CREATE TABLE Dim_Product (
    ProductKey INT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT,
    ProductName VARCHAR(255),
    Lineage_Id BIGINT
);

CREATE TABLE Dim_Customer (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT,
    CustomerName VARCHAR(255),
    CustomerAddress VARCHAR(255),
    StartDate DATE,
    EndDate DATE,
    IsCurrent BIT,
    Lineage_Id BIGINT
);

CREATE TABLE Dim_Employee (
    EmployeeKey INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID INT,
    EmployeeName VARCHAR(255),
    EmployeeTitle VARCHAR(255),
    StartDate DATE,
    EndDate DATE,
    IsCurrent BIT,
    Lineage_Id BIGINT
);

CREATE TABLE Dim_Geography (
    GeographyKey INT IDENTITY(1,1) PRIMARY KEY,
    GeographyID INT,
    GeographyName VARCHAR(255),
    Population INT,
    PreviousPopulation INT,
    Lineage_Id BIGINT
);

CREATE TABLE Dim_Date (
    DateKey INT PRIMARY KEY,
    Date DATE,
    Day_Number INT,
    Month_Name VARCHAR(50),
    Short_Month VARCHAR(3),
    Calendar_Month_Number INT,
    Calendar_Year INT,
    Fiscal_Month_Number INT,
    Fiscal_Year INT,
    Week_Number INT
);

CREATE TABLE Lineage (
    Lineage_Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    Source_System VARCHAR(100),
    Load_Stat_Datetime DATETIME,
    Load_EndDatetime DATETIME,
    Rows_at_Source INT,
    Rows_at_destination_Fact INT,
    Load_Status BIT
);

CREATE TABLE Fact_Sales (
    SalesKey INT IDENTITY(1,1) PRIMARY KEY,
    ProductKey INT,
    CustomerKey INT,
    EmployeeKey INT,
    GeographyKey INT,
    DateKey INT,
    SalesAmount DECIMAL(18, 2),
    Lineage_Id BIGINT
);

-- Generate Date Dimension Data from 2000 to 2023
DECLARE @StartDate DATE = '2000-01-01';
DECLARE @EndDate DATE = '2023-12-31';

WITH DateSequence AS (
    SELECT @StartDate AS DateValue
    UNION ALL
    SELECT DATEADD(DAY, 1, DateValue)
    FROM DateSequence
    WHERE DateValue < @EndDate
)
INSERT INTO Dim_Date (DateKey, Date, Day_Number, Month_Name, Short_Month, Calendar_Month_Number, Calendar_Year, Fiscal_Month_Number, Fiscal_Year, Week_Number)
SELECT 
    CAST(FORMAT(DateValue, 'yyyyMMdd') AS INT) AS DateKey,
    DateValue,
    DAY(DateValue) AS Day_Number,
    DATENAME(MONTH, DateValue) AS Month_Name,
    LEFT(DATENAME(MONTH, DateValue), 3) AS Short_Month,
    MONTH(DateValue) AS Calendar_Month_Number,
    YEAR(DateValue) AS Calendar_Year,
    MONTH(DATEADD(MONTH, -6, DateValue)) AS Fiscal_Month_Number,
    YEAR(DATEADD(MONTH, -6, DateValue)) AS Fiscal_Year,
    DATEPART(WEEK, DateValue) AS Week_Number
FROM DateSequence
OPTION (MAXRECURSION 0);

USE YourDatabase;


-- Create Staging Copy Table
SELECT * INTO Insignia_staging_copy FROM Insignia_staging WHERE 1=0;

CREATE PROCEDURE sp_IncrementalLoad
AS
BEGIN
    DECLARE @StartDatetime DATETIME = GETDATE();
    DECLARE @Load_Id BIGINT;
    DECLARE @RowsAtSource INT;
    DECLARE @RowsAtDestinationFact INT;
    DECLARE @LoadStatus BIT;

    -- Step 1: Create a copy of Insignia_staging table
    TRUNCATE TABLE Insignia_staging_copy;
    INSERT INTO Insignia_staging_copy SELECT * FROM Insignia_staging;

    -- Step 2: Load data from staging copy into Dimensions
    -- Example for Product Dimension (SCD Type 1)
    INSERT INTO Dim_Product (ProductID, ProductName, Lineage_Id)
    SELECT DISTINCT ProductID, ProductName, @Load_Id
    FROM Insignia_staging_copy;

    -- Example for Customer Dimension (SCD Type 2)
    DECLARE @CurrentDate DATE = GETDATE();
    MERGE Dim_Customer AS target
    USING (SELECT DISTINCT CustomerID, CustomerName, CustomerAddress FROM Insignia_staging_copy) AS source
    ON (target.CustomerID = source.CustomerID AND target.IsCurrent = 1)
    WHEN MATCHED AND (target.CustomerName <> source.CustomerName OR target.CustomerAddress <> source.CustomerAddress) THEN
        UPDATE SET target.IsCurrent = 0, target.EndDate = @CurrentDate
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (CustomerID, CustomerName, CustomerAddress, StartDate, EndDate, IsCurrent, Lineage_Id)
        VALUES (source.CustomerID, source.CustomerName, source.CustomerAddress, @CurrentDate, NULL, 1, @Load_Id);

    -- Implement similar logic for other Dimensions...

    -- Step 3: Load data into Fact Table
    INSERT INTO Fact_Sales (ProductKey, CustomerKey, EmployeeKey, GeographyKey, DateKey, SalesAmount, Lineage_Id)
    SELECT 
        p.ProductKey,
        c.CustomerKey,
        e.EmployeeKey,
        g.GeographyKey,
        d.DateKey,
        s.SalesAmount,
        @Load_Id
    FROM Insignia_staging_copy s
    JOIN Dim_Product p ON s.ProductID = p.ProductID
    JOIN Dim_Customer c ON s.CustomerID = c.CustomerID AND c.IsCurrent = 1
    JOIN Dim_Employee e ON s.EmployeeID = e.EmployeeID AND e.IsCurrent = 1
    JOIN Dim_Geography g ON s.GeographyID = g.GeographyID
    JOIN Dim_Date d ON s.SaleDate = d.Date;

    -- Step 4: Insert Incremental Data
    TRUNCATE TABLE Insignia_staging_copy;
    INSERT INTO Insignia_staging_copy SELECT * FROM Insignia_incremental;

    -- Repeat Steps 2-3 for Incremental Data...

    -- Record Load in Lineage Table
    SET @RowsAtSource = (SELECT COUNT(*) FROM Insignia_staging);
    SET @RowsAtDestinationFact = (SELECT COUNT(*) FROM Fact_Sales);
    SET @LoadStatus = 1;

    INSERT INTO Lineage (Source_System, Load_Stat_Datetime, Load_EndDatetime, Rows_at_Source, Rows_at_destination_Fact, Load_Status)
    VALUES ('Insignia', @StartDatetime, GETDATE(), @RowsAtSource, @RowsAtDestinationFact, @LoadStatus);
END;
-- Databricks notebook source
USE CATALOG retails_sales_catalog;
USE SCHEMA silver;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DimProduct (

    ProductSK     BIGINT GENERATED ALWAYS AS IDENTITY,

    ProductID     INT,

    ProductName   STRING,

    Category      STRING,

    UnitPrice     DECIMAL(10,2),

    EffectiveDate DATE

)

USING DELTA

LOCATION 's3://retail-sales-datawarehouse-shivanand-409953608511-eu-north-1-an/processed/silver/DimProduct/';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DimStore (

    StoreSK   BIGINT GENERATED ALWAYS AS IDENTITY,

    StoreID   INT,

    StoreName STRING,

    Region    STRING

)

USING DELTA

LOCATION 's3://retail-sales-datawarehouse-shivanand-409953608511-eu-north-1-an/processed/pro/DimStore/';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DimCustomer (

    CustomerSK   BIGINT GENERATED ALWAYS AS IDENTITY,

    CustomerID   INT,

    CustomerName STRING,

    Email        STRING,

    City         STRING,

    Address      STRING,

    StartDate    DATE,

    EndDate      DATE,

    IsActive     INT

)

USING DELTA

LOCATION 's3://retail-sales-datawarehouse-shivanand-409953608511-eu-north-1-an/processed/pro/DimCustomer/';

-- COMMAND ----------

TRUNCATE TABLE DimProduct;
INSERT INTO DimProduct
(ProductID, ProductName, Category, UnitPrice, EffectiveDate)
SELECT DISTINCT
    CAST(ProductID AS INT),
    TRIM(ProductName),
    TRIM(Category),
    CAST(UnitPrice AS DECIMAL(10,2)),
    CURRENT_DATE()
FROM retails_sales_catalog.bronze.products_raw
WHERE ProductID   IS NOT NULL
AND   ProductName IS NOT NULL
AND   Category    IS NOT NULL
AND   UnitPrice   IS NOT NULL
AND   CAST(UnitPrice AS DECIMAL(10,2)) > 0;

-- COMMAND ----------

TRUNCATE TABLE DimStore;
INSERT INTO DimStore
(StoreID, StoreName, Region)
SELECT DISTINCT
    CAST(StoreID AS INT),
    TRIM(StoreName),
    TRIM(Region)
FROM retails_sales_catalog.bronze.stores_raw
WHERE StoreID   IS NOT NULL
AND   StoreName IS NOT NULL
AND   Region    IS NOT NULL;

-- COMMAND ----------



INSERT INTO DimCustomer
(CustomerID, CustomerName, Email, City, Address, StartDate, EndDate, IsActive)
SELECT DISTINCT
    CAST(CustomerID AS INT),
    INITCAP(TRIM(CustomerName)),
    LOWER(TRIM(Email)),
    TRIM(City),
    TRIM(Address),
    CURRENT_DATE(),
    CAST('9999-12-31' AS DATE),
    1
FROM retails_sales_catalog.bronze.customers_raw
WHERE CustomerID   IS NOT NULL
AND   CustomerName IS NOT NULL
AND   Email        IS NOT NULL
AND   Email        LIKE '%@%.%'
AND   City         IS NOT NULL
AND   Address      IS NOT NULL;

-- COMMAND ----------

SELECT 'DimCustomer' AS table_name, COUNT(*) AS row_count FROM DimCustomer
UNION ALL
SELECT 'DimProduct',  COUNT(*) FROM DimProduct
UNION ALL
SELECT 'DimStore',    COUNT(*) FROM DimStore;

-- COMMAND ----------

MERGE INTO retails_sales_catalog.silver.DimCustomer target

USING (

    SELECT
        CustomerID,
        INITCAP(TRIM(CustomerName)) AS CustomerName,
        LOWER(TRIM(Email)) AS Email,
        TRIM(City) AS City,
        TRIM(Address) AS Address

    FROM (

        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY CustomerID
                   ORDER BY LastUpdated DESC
               ) AS rn

        FROM retails_sales_catalog.bronze.customers_raw

    )

    WHERE rn = 1

) source

ON target.CustomerID = source.CustomerID
AND target.IsActive = 1

WHEN MATCHED AND (

       target.City <> source.City
    OR target.Address <> source.Address
    OR target.CustomerName <> source.CustomerName
    OR target.Email <> source.Email

)

THEN UPDATE SET

    target.EndDate = CURRENT_DATE(),
    target.IsActive = 0;

-- COMMAND ----------

INSERT INTO retails_sales_catalog.silver.DimCustomer
(
    CustomerID,
    CustomerName,
    Email,
    City,
    Address,
    StartDate,
    EndDate,
    IsActive
)

SELECT

    source.CustomerID,

    source.CustomerName,

    source.Email,

    source.City,

    source.Address,

    CURRENT_DATE(),

    DATE('9999-12-31'),

    1

FROM (

    SELECT
        CustomerID,
        INITCAP(TRIM(CustomerName)) AS CustomerName,
        LOWER(TRIM(Email)) AS Email,
        TRIM(City) AS City,
        TRIM(Address) AS Address,

        ROW_NUMBER() OVER (
            PARTITION BY CustomerID
            ORDER BY LastUpdated DESC
        ) AS rn

    FROM retails_sales_catalog.bronze.customers_raw

) source

WHERE rn = 1

AND NOT EXISTS (

    SELECT 1

    FROM retails_sales_catalog.silver.DimCustomer target

    WHERE target.CustomerID = source.CustomerID
    AND target.IsActive = 1
    AND target.City = source.City
    AND target.Address = source.Address
    AND target.CustomerName = source.CustomerName
    AND target.Email = source.Email
);

-- COMMAND ----------

SELECT *
FROM DimCustomer
WHERE IsActive = 1;

-- COMMAND ----------

SELECT *
FROM DimCustomer
WHERE IsActive = 0;

-- Databricks notebook source
USE CATALOG retails_sales_catalog;
USE SCHEMA gold;

-- COMMAND ----------

-- DBTITLE 1,Cell 2
CREATE TABLE IF NOT EXISTS FactSales
USING DELTA
LOCATION 's3://retail-sales-datawarehouse-shivanand-409953608511-eu-north-1-an/processed/pro/FactSales/';

-- COMMAND ----------

-- DBTITLE 1,Cell 3
truncate table FactSales;
INSERT INTO FactSales
(
    TransactionID,
    CustomerSK,
    ProductSK,
    StoreSK,
    Quantity,
    Amount,
    TxnDate
)

SELECT

    s.TransactionID,

    dc.CustomerSK,

    dp.ProductSK,

    ds.StoreSK,

    CAST(s.Quantity AS INT) AS Quantity,

    CAST(s.Quantity * dp.UnitPrice AS DECIMAL(10,2)) AS Amount,

    TO_DATE(s.TxnDate, 'dd-MM-yyyy') AS TxnDate

FROM bronze.sales_raw s

INNER JOIN silver.DimCustomer dc
    ON s.CustomerID = dc.CustomerID
   AND dc.IsActive = 1

INNER JOIN silver.DimProduct dp
    ON s.ProductID = dp.ProductID

INNER JOIN silver.DimStore ds
    ON s.StoreID = ds.StoreID

WHERE s.TransactionID IS NOT NULL
  AND s.CustomerID IS NOT NULL
  AND s.ProductID IS NOT NULL
  AND s.StoreID IS NOT NULL
  AND s.Quantity IS NOT NULL;

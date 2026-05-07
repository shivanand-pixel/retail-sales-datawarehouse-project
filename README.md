# retail-sales-datawarehouse-project

## Project Overview

This project is an end-to-end Retail Sales Data Warehouse solution developed using Databricks, AWS S3, Delta Lake, SQL, and PySpark. The project simulates a real-world enterprise ETL pipeline using Medallion Architecture (Bronze, Silver, Gold) and implements Slowly Changing Dimension Type 2 (SCD Type 2) for historical customer tracking

The solution processes retail transactional data incrementally, performs data cleansing and transformations, maintains customer history, and generates analytical fact tables for reporting and business intelligence.

---

# Architecture

The project follows a modern Lakehouse architecture using Medallion Architecture.

```text
AWS S3 → Bronze Layer → Silver Layer → Gold Layer
```

## Layers

### Bronze Layer

* Raw data ingestion from AWS S3
* Stores source data without transformations
* Preserves ingestion history

### Silver Layer

* Data cleansing and transformation
* Dimension table creation
* Deduplication and validations
* SCD Type 2 implementation

### Gold Layer

* Analytical fact table generation
* Business-ready reporting datasets
* Star schema implementation

---

# Technologies Used

| Technology           | Purpose                   |
| -------------------- | ------------------------- |
| AWS S3               | Cloud Data Lake Storage   |
| Databricks           | Data Engineering Platform |
| Delta Lake           | ACID-compliant Storage    |
| SQL                  | Data Transformations      |
| PySpark              | Automation and Processing |
| Databricks Workflows | Pipeline Orchestration    |
| GitHub               | Version Control           |

---

# Project Features

* End-to-End ETL Pipeline
* Incremental Data Processing
* Archive Mechanism for Daily Files
* Medallion Architecture
* SCD Type 2 Historical Tracking
* Fact and Dimension Modeling
* Star Schema Design
* Databricks Workflow Automation
* Cloud-Native Data Engineering Pipeline

---

# S3 Folder Structure

```text
raw/
│
├── customers/
├── product/
├── stores/
└── sales/

archive/
│
└── raw/

processed/
```

---

# Tables Created

## Bronze Layer

* customers_raw
* products_raw
* stores_raw
* sales_raw

## Silver Layer

* DimCustomer
* DimProduct
* DimStore

## Gold Layer

* FactSales

---

# SCD Type 2 Implementation

Implemented SCD Type 2 on the customer dimension to maintain historical customer changes.

Features:

* Historical tracking
* Active and inactive records
* StartDate and EndDate handling
* Customer version management

---

# FactSales Logic

FactSales stores transactional business metrics including:

* Quantity sold
* Sales amount
* Customer references
* Product references
* Store references

Amount calculation:

```text
Amount = Quantity × UnitPrice
```

---

# Incremental Processing

The pipeline supports incremental daily file processing using:

* archive mechanism
* latest file identification
* automated raw file management

Old files are moved to archive folders while latest files are processed.

---

# Workflow Automation

The ETL pipeline is automated using Databricks Workflows.

Pipeline sequence:

```text
Archive → Bronze → Silver → SCD Type 2 → Gold
```

---

# Key Concepts Implemented

* Medallion Architecture
* Lakehouse Architecture
* Delta Lake
* SCD Type 2
* Incremental ETL
* Star Schema Modeling
* Fact and Dimension Tables
* Workflow Automation
* Cloud Data Engineering

---

# Business Benefits

* Historical customer tracking
* Centralized retail analytics
* Automated ETL processing
* Scalable warehouse design
* Optimized analytical reporting

---

# Future Enhancements

* Real-time streaming ingestion
* CDC implementation
* Dashboard integration
* Monitoring and alerting
* Data quality framework
* Performance optimization

---

# Final Outcome

Successfully developed:

✅ Enterprise Retail Data Warehouse
✅ End-to-End ETL Pipeline
✅ Incremental Processing Framework
✅ SCD Type 2 Historical Tracking
✅ Gold Fact Table for Analytics
✅ Cloud-Native Lakehouse Architecture


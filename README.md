# End-to-End Data Engineering Pipeline using Databricks

## Overview
This project demonstrates an end-to-end data engineering pipeline built using Databricks and PySpark following the Medallion Architecture (Bronze, Silver, Gold).

## Tech Stack
- Azure Data Lake Gen2
- Databricks
- PySpark
- Delta Lake
- Unity Catalog

## Architecture
Bronze → Silver → Gold

## Pipeline Details

### Bronze Layer
- Data ingestion using Databricks Auto Loader
- Stores raw data in Delta format

### Silver Layer
- Data cleaning and transformations
- Handling nulls and duplicates
- Schema enforcement
- Business-level transformations

### Gold Layer
- Fact and Dimension tables
- Star schema design
- SCD Type 1 implementation
- Optimized tables for analytics

## Key Features
- Incremental data processing
- Idempotent pipeline design
- Delta Lake ACID transactions
- Parameterized notebooks

## Outcome
Built a scalable and production-ready data pipeline suitable for analytics and reporting use cases.

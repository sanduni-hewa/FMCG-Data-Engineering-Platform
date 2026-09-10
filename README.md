# FMCG Data Engineering Platform

An end-to-end Databricks lakehouse project that integrates historical parent-company data with incremental order data from an acquired company. The pipeline standardizes the two source schemas and produces a consolidated, analytics-ready fact table for consistent FMCG reporting.

![FMCG Data Engineering Platform architecture](fmcg-data-platform-architecture.png)

## Project Overview

When one FMCG company acquires another, their operational datasets may use different schemas, naming conventions, and ingestion patterns. This project demonstrates how Databricks, PySpark, SQL, Delta Lake, and AWS S3 can bring those datasets into a common analytical structure.

The solution follows a Bronze–Silver–Gold Medallion Architecture to preserve raw data, apply repeatable data-quality transformations, and deliver trusted data for downstream analytics.

## Business Problem

The parent company maintained historical fact data in an established structure, while the acquired company provided new order files using a different schema. Reporting across the combined organization required a dependable way to:

- Ingest historical and incremental data from multiple sources
- Align inconsistent column names and data types
- Remove duplicate and invalid records
- Preserve previously processed records during new loads
- Produce one consistent dataset for cross-company reporting

## Architecture

| Stage | Purpose | Key Processing |
| --- | --- | --- |
| Source systems | Supply operational data | Parent-company fact data and acquired-company order files |
| AWS S3 | Provide the landing zone | Store incoming source files for ingestion |
| Bronze | Preserve source data | Load raw records into Delta tables with minimal transformation |
| Silver | Improve data quality | Clean, validate, deduplicate, and standardize both schemas |
| Gold | Serve analytical data | Combine standardized records into one consolidated fact table |
| Analytics | Support reporting | Query business-ready data using Databricks SQL and dashboards |

## Data Flow

1. Historical parent-company data and acquired-company order files land in AWS S3.
2. Databricks ingests the source data into Bronze Delta tables.
3. PySpark transformations clean the records, handle nulls, remove duplicates, and standardize the schemas in the Silver layer.
4. Incremental processing identifies and processes newly arrived acquired-company orders.
5. The standardized datasets are aligned with the parent company's existing fact-table structure.
6. The records are loaded into a consolidated Gold-layer fact table.
7. Databricks SQL queries and dashboard visuals consume the curated data for FMCG analysis.

## Technology Stack

- **Databricks** — pipeline development, processing, and analytics
- **Apache Spark / PySpark** — distributed data transformations
- **SQL** — validation, transformation, and analytical queries
- **Delta Lake** — reliable lakehouse table storage
- **AWS S3** — cloud-based source-data landing zone
- **Databricks SQL** — dashboard queries and reporting

## Key Engineering Features

### Medallion Architecture

The pipeline separates raw ingestion, validated data, and analytical outputs across Bronze, Silver, and Gold layers. This structure makes the transformations easier to maintain, troubleshoot, and extend.

### Incremental Processing

New acquired-company order files are processed incrementally, avoiding unnecessary full reloads while retaining previously processed records.

### Schema Standardization

Source-specific fields are renamed, cast, and aligned with the parent company's fact-table structure before the datasets are consolidated.

### Data-Quality Transformations

The Silver layer applies repeatable checks and transformations, including:

- Duplicate removal
- Null-value handling
- Data-type validation
- Column-name standardization
- Business-rule validation

### Analytics-Ready Gold Layer

The final Gold-layer fact table provides a consistent structure for analyzing orders across the parent and acquired companies.

## Outcomes

- Processed and standardized **90K+ records**
- Integrated two differently structured company datasets
- Created one consolidated Gold-layer fact table for consistent reporting
- Automated data cleansing, deduplication, and schema alignment
- Produced analytics-ready data for SQL analysis and dashboard reporting

## Data Validation

The completed pipeline was validated by checking:

- Record counts across pipeline layers
- Duplicate business keys
- Nulls in required fields
- Column names and data types after schema alignment
- Successful inclusion of both companies in the consolidated output

Example validation pattern:

```sql
SELECT
    data_source,
    COUNT(*) AS record_count
FROM gold.consolidated_fact_orders
GROUP BY data_source;
```

> Update the example table and column names above if they differ from the names used in your implementation.

## Skills Demonstrated

- Designing an end-to-end batch data pipeline
- Applying the Medallion Architecture in Databricks
- Writing distributed transformations with PySpark
- Implementing incremental ingestion
- Resolving schema differences across business systems
- Building Delta Lake tables for reliable analytics
- Preparing curated data for BI consumption

## Repository Structure

```text
├── notebooks/       # Databricks ingestion and transformation notebooks
├── data/            # Sample or source datasets, when appropriate
├── images/          # Architecture and dashboard images
└── README.md        # Project documentation
```

> Adjust this section to match the folders in the repository before publishing.

## Future Enhancements

- Add automated data-quality expectations and failure alerts
- Orchestrate the complete workflow with scheduled Databricks Jobs
- Add pipeline monitoring and audit tables
- Implement automated tests for transformation logic
- Add CI/CD for notebook and pipeline deployment

## Acknowledgment

This project was completed as a hands-on implementation inspired by the Codebasics tutorial [End to End Data Engineering Project using Databricks Free Edition | FMCG Domain](https://www.youtube.com/watch?v=U6ZUKWdfSLY). This repository documents my implementation and understanding of the architecture, transformations, and data-engineering concepts demonstrated in the project.


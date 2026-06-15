# Project ApexRetail

ApexRetail is a large-scale retail data engineering project built using Databricks, PySpark, and Delta Lake following the Medallion Architecture (Bronze, Silver, and Gold layers).

The goal of this project was to simulate a real-world retail data platform capable of processing multi-gigabyte datasets while handling common production challenges such as data quality issues, duplicate records, data skew, incremental ingestion, and analytical reporting.

The project generates approximately 10GB of retail data and processes it through an end-to-end pipeline to produce analytics-ready datasets and business KPIs.

---

## Architecture

```text
Raw Data
    ↓
Bronze Layer
(Ingestion & Audit Logging)
    ↓
Silver Layer
(Cleaning, Validation, Deduplication)
    ↓
Gold Layer
(Business Aggregations & Reporting)
```

---

## Technologies Used

* Databricks
* Apache Spark (PySpark)
* Delta Lake
* Spark SQL
* Structured Streaming
* Auto Loader Pattern
* Delta Optimization (OPTIMIZE & ZORDER)

---

## Dataset Scale

| Dataset           | Records |
| ----------------- | ------- |
| Customers         | 1M+     |
| Products          | 50K+    |
| Orders            | 20M+    |
| Order Items       | 45M+    |
| Total Data Volume | ~10GB   |

The datasets are generated entirely using distributed Spark transformations such as `spark.range()`, `explode()`, and hash-based expressions without using Python loops.

---

## Real-World Data Quality Scenarios

To make the project closer to production systems, intentional data quality issues were introduced:

* Duplicate customer records
* Duplicate orders
* Invalid and null emails
* Missing phone numbers
* Missing city information
* Negative product prices
* Negative quantities
* Missing unit prices
* Revenue inconsistencies
* Large-scale data skew

These issues are detected and handled during Silver layer processing.

---

## Bronze Layer

The Bronze layer is responsible for ingesting raw datasets into Delta tables.

Features:

* Raw data ingestion
* Delta table creation
* Audit logging
* Incremental ingestion pattern
* Auto Loader implementation for large datasets

An audit table is maintained to track:

* Pipeline status
* Row counts
* Processing duration
* Error details

---

## Silver Layer

The Silver layer performs data cleansing and transformation.

Key operations include:

* Data validation
* Deduplication using Window Functions
* Email validation
* Null handling
* Standardization
* Data quality flagging

Instead of using `dropDuplicates()`, duplicate records are handled using:

```sql
ROW_NUMBER()
OVER(
PARTITION BY customer_id
ORDER BY updated_timestamp DESC
)
```

This ensures deterministic record selection.

---

## Handling Data Skew

One of the objectives of the project was to simulate skewed retail workloads where a small percentage of products generate a large percentage of transactions.

Optimization techniques used:

* Broadcast Joins
* Adaptive Query Execution (AQE)
* Partition Optimization
* Salting Strategy (Reference Implementation)

---

## Gold Layer

The Gold layer contains business-ready datasets and KPIs.

Generated outputs include:

* Customer Summary
* Product Performance
* Monthly Revenue Trends
* Category Performance
* Data Quality Dashboard

These tables can be consumed directly by BI tools and reporting systems.

---

## Delta Lake Optimizations

To improve query performance, Delta tables are optimized using:

* Partitioning by order date
* OPTIMIZE command
* ZORDER BY

Example:

```sql
OPTIMIZE fact_sales
ZORDER BY (customer_id, product_id)
```

This significantly improves query performance on frequently filtered columns.

---

## Key Data Engineering Concepts Demonstrated

* Medallion Architecture
* Delta Lake
* Distributed Data Processing
* Spark SQL
* Window Functions
* Broadcast Joins
* Data Skew Handling
* Auto Loader
* Structured Streaming
* Incremental Processing
* Data Quality Frameworks
* Delta Optimization
* Audit Logging

---

## Project Structure

```text
retail-medallion-pipeline/

├── notebooks/
│   ├── 01_big_data_generator.py
│   ├── 02_bronze_ingestion.py
│   ├── 03_silver_transformation.py
│   └── 04_gold_aggregates.py
│
├── config/
│   └── cluster_config.json
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Future Enhancements

Planned improvements include:

* CI/CD integration
* Airflow orchestration
* Data lineage tracking
* Automated monitoring and alerting
* Cloud object storage integration
* Real-time streaming pipelines

---

## Repository

Project Name: ApexRetail

GitHub Repository:
https://github.com/Bobbykumaar/Project-ApexRetail

---

## License

MIT License

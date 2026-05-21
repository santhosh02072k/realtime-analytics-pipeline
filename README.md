# E-Commerce Sales Analytics Pipeline (AWS)

![Stack](https://img.shields.io/badge/Stack-AWS%20Glue%20%2B%20Redshift%20%2B%20QuickSight-orange)
![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![License](https://img.shields.io/badge/License-MIT-green)

## Overview

A scalable ETL analytics platform built on AWS, processing e-commerce sales data through AWS Glue, storing in Amazon Redshift, and visualizing with QuickSight. Designed with star schema modeling for efficient multi-dimensional KPI analysis.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│              Raw Sales Data (S3)                     │
└──────────────────────┬──────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────┐
│              AWS Glue (PySpark ETL)                  │
│   Extract, transform, validate, load                 │
└──────────────────────┬──────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────┐
│              Amazon Redshift                         │
│   Star schema: fact_sales + dimensions               │
│   Partitioned and optimized for analytics            │
└──────────────────────┬──────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────┐
│              Amazon QuickSight                       │
│   Executive dashboards, KPI reporting                │
└─────────────────────────────────────────────────────┘
```

## Tech Stack

| Component | Technology |
|---|---|
| Storage | Amazon S3 |
| ETL | AWS Glue (PySpark) |
| Data Warehouse | Amazon Redshift |
| Visualization | Amazon QuickSight |
| Scheduling | AWS Glue Triggers |

## Project Structure

```
ecommerce-sales-analytics-aws/
├── README.md
├── glue_jobs/
│   └── etl_sales_pipeline.py    # AWS Glue PySpark ETL job
├── redshift/
│   └── create_tables.sql        # Redshift star schema DDL
├── queries/
│   └── sales_analytics.sql      # KPI reporting queries
└── data/
    └── sample_sales_data.py     # Generate sample data
```

## Data Model

- **fact_sales**: order_id, product_id, customer_id, date_id, quantity, amount, discount
- **dim_products**: product_id, name, category, price
- **dim_customers**: customer_id, name, region, segment
- **dim_dates**: date_id, date, year, month, quarter, day_of_week

## Sample Analytics

- Revenue by region and product category
- Daily/weekly sales trends
- Customer segmentation analysis
- Top performing products and categories

## Author

**Santhosh Ekambaram**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat&logo=linkedin)](https://linkedin.com/in/santhosh-ekambaram-1457241a2)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=flat&logo=github)](https://github.com/santhosh02072k)

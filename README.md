# Superstore Sales Analytics

End-to-end data pipeline and analytics dashboard built on Databricks, PySpark, Delta Lake, and SQL.

## Dataset

Kaggle Superstore Sales dataset — 9,994 retail order line items from 2014 to 2017.
Download: https://www.kaggle.com/datasets/vivek468/superstore-dataset-final

## Purpose of the dashboard

1. Ingests the raw CSV into Databricks (Bronze layer)
2. Cleans and enriches the data with PySpark — date parsing, profit margin calculation, discount tier classification, deduplication (Silver layer)
3. Builds aggregated summary tables — yearly trends, sub-category performance, customer lifetime value, monthly regional growth (Gold layer)
4. Writes all tables to Delta Lake with partitioning and Z-ORDER optimisation
5. Runs 7 SQL analytical queries using window functions (LAG, RANK, SUM OVER PARTITION BY)
6. Visualises findings in an interactive React dashboard

## Key findings

- Tables and Bookcases sub-categories lose money (-8.6% and -3.0% margin) despite significant sales volume
- Discounts above 20% flip the overall segment from +18% to -11% profit margin
- Central region has the third highest sales but the weakest margin at 7.9%
- Same Day shipping carries the highest profit margin despite being the least used ship mode
- Technology drives the most revenue; Office Supplies has the highest margin

## Project structure

```
superstore_analytics_notebook.py   Databricks notebook — full pipeline
SuperstoreDashboard.jsx            React dashboard
```

## How to run

1. Download `Sample - Superstore.csv` from Kaggle
2. Create a Databricks workspace
3. Upload the CSV: `databricks fs cp "Sample - Superstore.csv" dbfs:/mnt/raw/superstore/`
4. Import `superstore_analytics_notebook.py` as a notebook (File > Import)
5. Attach to a cluster running DBR 12.x or later and run all cells

## Stack

- Apache PySpark 3.x
- Databricks Runtime 12+
- Delta Lake
- Databricks SQL
- React + Recharts (dashboard)

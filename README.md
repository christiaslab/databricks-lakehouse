# Databricks Lakehouse

A small end-to-end lakehouse on Databricks following the **Medallion Architecture** (bronze → silver → gold) with PySpark, Delta Lake and Unity Catalog.

Source data: a bike retailer with two systems, **CRM** (customers, products, sales) and **ERP** (customer demographics, locations, product categories). Six CSV files, ~116k rows.

## Layout

```
data/               raw CSV files (source_crm/, source_erp/)
notebooks/
  00_init_lakehouse   schemas + volume
  01_bronze/          raw ingestion → bronze.*
  02_silver/          cleaning & standardization → silver.*
  03_gold/            star schema (dim_customers, dim_products, fact_sales) → gold.*
```

## Setup

1. Run `notebooks/00_init_lakehouse` once.
2. Upload `data/source_crm/*` to `/Volumes/workspace/bronze/src/src_crm/` and `data/source_erp/*` to `/Volumes/workspace/bronze/src/src_erp/`.
3. Run the layers in order: bronze → silver → gold.

## Credits

Based on the [Databricks Bootcamp 2026](https://github.com/DataWithBaraa/databricks_bootcamp_2026) by Baraa Khatib Salkini (MIT License).

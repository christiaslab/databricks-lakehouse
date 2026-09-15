# Databricks Lakehouse

[![CI](https://github.com/christiaslab/databricks-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/christiaslab/databricks-lakehouse/actions/workflows/ci.yml)

An end-to-end lakehouse on Databricks for a bike retailer: raw CSV exports from two source systems (CRM and ERP) are ingested, cleaned, quality-checked and modelled into a star schema that feeds an AI/BI dashboard. Everything — notebooks, job, dashboard, tests — is code in this repo and deployed with Databricks Asset Bundles.

## Architecture

```
        CRM (3 csv)  ERP (3 csv)
              │          │
              ▼          ▼
   ┌─────────────────────────────┐
   │  BRONZE   raw Delta copies  │  schema inferred, audit columns (_ingested_at, _source_file)
   └──────────────┬──────────────┘
                  ▼
   ┌─────────────────────────────┐
   │  SILVER   cleaned tables    │  trim, normalize codes, fix keys/dates/prices, rename
   └──────────────┬──────────────┘
                  ▼
   ┌─────────────────────────────┐
   │  DATA QUALITY GATE          │  14 checks → silver.dq_results; errors stop the job
   └──────────────┬──────────────┘
                  ▼
   ┌─────────────────────────────┐
   │  GOLD     star schema       │  dim_customers, dim_products, fact_sales
   └──────────────┬──────────────┘
                  ▼
        AI/BI dashboard "Bike Sales Overview"
```

The job DAG (`resources/lakehouse_job.yml`) runs the six silver notebooks in parallel, gates on data quality, then builds the dimensions in parallel and the fact last:

```
bronze ─┬─ silver_crm_cust_info ──────┐
        ├─ silver_crm_prd_info ───────┤
        ├─ silver_crm_sales_details ──┤                    ┌─ gold_dim_customers ─┐
        ├─ silver_erp_cust_az12 ──────┼─ silver_dq_checks ─┤                      ├─ gold_fact_sales
        ├─ silver_erp_loc_a101 ───────┤                    └─ gold_dim_products ──┘
        └─ silver_erp_px_cat_g1v2 ────┘
```

## Repository layout

```
data/                     raw CSV files (source_crm/, source_erp/) — uploaded once to a UC volume
notebooks/
  00_init_lakehouse       schemas + volume
  01_bronze/              config-driven ingestion of all sources
  02_silver/crm|erp/      one notebook per table, thin: read → shared transforms → write
  02_silver/silver_dq_checks   declarative quality checks + gate
  03_gold/                SQL star schema
src/lakehouse/
  transforms.py           reusable DataFrame transformations used by every silver notebook
  dq.py                   tiny data-quality framework (not_null, unique, references, ...)
tests/                    pytest unit tests, run on serverless via Databricks Connect
resources/
  lakehouse_job.yml       the pipeline job (tasks + dependencies)
  sales_dashboard.yml     AI/BI dashboard resource (+ .lvdash.json definition)
databricks.yml            bundle definition (targets)
.github/workflows/ci.yml  bundle validate + unit tests on every PR / push
```

## Data model (gold)

| Table | Grain | Notes |
|---|---|---|
| `dim_customers` | one row per customer | CRM master, enriched with ERP birth date, gender, country |
| `dim_products` | one row per current product | CRM master joined to ERP category lookup; historical versions excluded |
| `fact_sales` | one row per order line | natural keys replaced by dimension surrogate keys |

## Setup

Prerequisites: [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) and [uv](https://docs.astral.sh/uv/).

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
uv sync
```

1. Run `notebooks/00_init_lakehouse` once (creates `bronze`, `silver`, `gold` schemas and the `bronze.src` volume).
2. Upload `data/source_crm/*` to `/Volumes/workspace/bronze/src/src_crm/` and `data/source_erp/*` to `.../src_erp/`.
3. Deploy and run:

```bash
databricks bundle deploy -t dev
databricks bundle run lakehouse_job -t dev
```

The dashboard is deployed with the bundle and appears under **Dashboards** in the workspace.

## Development

```bash
uv run pytest -v                  # unit tests for transforms and dq (runs on serverless)
databricks bundle validate        # check job / dashboard definitions
```

Notebooks can be edited locally with the Databricks VS Code extension and run on serverless directly from the editor. The workspace Git folder mirrors `main`.

## Data quality

`silver_dq_checks` runs after silver and before gold. Checks are declared as a list — primary keys (not null, unique), foreign keys (every sale finds its customer and product) and business rules (positive quantities, order before ship, no future birth dates). Each run appends one row per check to `silver.dq_results`; any failed `error`-severity check raises and stops the job, so gold is never rebuilt on bad data.

## Credits

Source data and the original notebook walkthrough come from the [Databricks Bootcamp 2026](https://github.com/DataWithBaraa/databricks_bootcamp_2026) by Baraa Khatib Salkini (MIT License). This repo restructures it into a bundle-deployed project with shared modules, tests, a quality gate, CI and a dashboard.

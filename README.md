# Azure Sales Data Warehouse (ADF + ADLS + Databricks + Delta Lake)

An end-to-end ELT project that ingests sales data, transforms it using the Medallion pattern (Bronze/Silver/Gold), and publishes a **Gold Star Schema** for analytics (Synapse/Power BI ready).

## Tech Stack
- Azure Data Factory (ADF) – orchestration
- ADLS Gen2 – storage
- Azure Databricks – PySpark transformations
- Delta Lake – ACID tables (Bronze/Silver/Gold)
- Synapse / Power BI – consumption (optional)

## Architecture
See: `docs/architecture.md`

## Data Model (Star Schema)
See: `docs/data_model.md`

## Repo Structure
data_sample/ -> sample input CSVs
notebooks/ -> PySpark notebooks (Bronze/Silver/Gold + DQ)
docs/ -> architecture + data model
adf/ -> ADF pipeline templates (JSON)


## How to Run (Databricks)
1. Upload sample data to DBFS:
   - `dbfs:/FileStore/azure-sales-data-warehouse/data_sample/`

2. Run notebooks in order:
   - `00_setup_paths.py`
   - `01_bronze_ingest.py`
   - `02_silver_clean_dedup.py`
   - `03_gold_dimensions.py`
   - `04_gold_fact_sales.py`
   - `05_data_quality_checks.py`

Outputs are created under:
- Bronze: `dbfs:/FileStore/azure-sales-data-warehouse/bronze`
- Silver: `dbfs:/FileStore/azure-sales-data-warehouse/silver`
- Gold:   `dbfs:/FileStore/azure-sales-data-warehouse/gold`

## What This Demonstrates (Interview Talking Points)
- Medallion architecture (Bronze/Silver/Gold)
- Dedup using window functions (ROW_NUMBER)
- Fact table metrics (revenue/discount/profit)
- Partitioning for performance
- Data Quality checks (uniqueness, nulls, referential integrity)
- ADF orchestration patterns (template provided)

## Future Enhancements
- Watermark-based incremental loads + MERGE into Gold
- SCD Type 2 dimensions (customer/product changes)
- OPTIMIZE / ZORDER maintenance jobs
- CI/CD for ADF + Databricks notebooks

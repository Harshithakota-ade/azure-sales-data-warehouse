# Architecture - Azure Sales Data Warehouse (ADF + ADLS + Databricks + Delta)

## Goal
Build an end-to-end ELT pipeline that ingests sales data, applies cleaning + dedup, and publishes a **Gold Star Schema** (Fact + Dimensions) for analytics in Synapse/Power BI.

---

## High Level Flow
Source Files / DB
→ **ADF** (orchestration + incremental ingestion)
→ **ADLS Gen2** (raw/bronze/silver/gold)
→ **Databricks (PySpark)** transformations
→ **Delta Lake** tables (ACID + partitioning)
→ **Synapse/Power BI** consumption

---

## Storage Layout
- Raw: `/raw/sales/...`
- Bronze (Delta): `/bronze/...`
- Silver (Delta): `/silver/...`
- Gold (Delta Star Schema):
  - `/gold/dim_customer`
  - `/gold/dim_product`
  - `/gold/dim_store`
  - `/gold/dim_date`
  - `/gold/fact_sales`

---

## Pipeline Stages
### Bronze
- Land raw CSV extracts
- Add ingestion metadata (`_ingested_at`, `_source`)
- Persist as Delta for reliable downstream processing

### Silver
- Type casting and standardization
- Deduplicate using window functions (ROW_NUMBER)
- Enforce consistent schema

### Gold
- Create star schema tables
- `fact_sales` built from orders + order_items + product pricing
- Partition `fact_sales` by order_date for performance
- Data quality validation checks before publishing

---

## Why Delta Lake
- ACID transactions
- Reliable MERGE patterns for incremental
- Schema enforcement and consistent reads
- Performance via partitioning + OPTIMIZE/ZORDER (future enhancement)

---

## Future Enhancements
- True incremental loads (watermark + MERGE into Gold)
- SCD Type 2 dimensions for customer/product changes
- Add expectations framework (Deequ/Great Expectations)
- Add monitoring + alerts from ADF

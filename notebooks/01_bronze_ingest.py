# 01_bronze_ingest.py
# Ingest CSV -> Bronze Delta (raw landing in Delta)

from pyspark.sql import functions as F
from pyspark.sql.types import *

# Bring in paths
# In Databricks, run 00_setup_paths first or import it if you configure Python path.
# Here we assume you executed 00_setup_paths in the same cluster session.

def read_csv(path: str):
    return (spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(path))

# Input CSV paths
p_customers = f"{SAMPLE_DATA}/customers.csv"
p_products  = f"{SAMPLE_DATA}/products.csv"
p_stores    = f"{SAMPLE_DATA}/stores.csv"
p_orders    = f"{SAMPLE_DATA}/sales_orders.csv"
p_items     = f"{SAMPLE_DATA}/sales_order_items.csv"

df_customers = read_csv(p_customers).withColumn("_ingested_at", F.current_timestamp()).withColumn("_source", F.lit("sample_csv"))
df_products  = read_csv(p_products ).withColumn("_ingested_at", F.current_timestamp()).withColumn("_source", F.lit("sample_csv"))
df_stores    = read_csv(p_stores   ).withColumn("_ingested_at", F.current_timestamp()).withColumn("_source", F.lit("sample_csv"))
df_orders    = read_csv(p_orders   ).withColumn("_ingested_at", F.current_timestamp()).withColumn("_source", F.lit("sample_csv"))
df_items     = read_csv(p_items    ).withColumn("_ingested_at", F.current_timestamp()).withColumn("_source", F.lit("sample_csv"))

# Write to Bronze Delta
(df_customers.write.format("delta").mode("overwrite").save(T_BRONZE_CUSTOMERS))
(df_products .write.format("delta").mode("overwrite").save(T_BRONZE_PRODUCTS))
(df_stores   .write.format("delta").mode("overwrite").save(T_BRONZE_STORES))
(df_orders   .write.format("delta").mode("overwrite").save(T_BRONZE_ORDERS))
(df_items    .write.format("delta").mode("overwrite").save(T_BRONZE_ITEMS))

print("✅ Bronze ingest complete")

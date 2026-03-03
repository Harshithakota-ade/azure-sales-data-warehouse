# 00_setup_paths.py
# Central place for paths + configs

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# -----------------------------
# Choose storage mode
# -----------------------------
# For GitHub demo / local file testing (Databricks can still read /dbfs/ paths)
USE_DBFS = True

# If USE_DBFS=True, upload your data_sample folder to:
#   dbfs:/FileStore/azure-sales-data-warehouse/data_sample/
# Or use Repos path if you're using Databricks Repos.

if USE_DBFS:
    BASE_RAW = "dbfs:/FileStore/azure-sales-data-warehouse/raw"
    BASE_BRONZE = "dbfs:/FileStore/azure-sales-data-warehouse/bronze"
    BASE_SILVER = "dbfs:/FileStore/azure-sales-data-warehouse/silver"
    BASE_GOLD = "dbfs:/FileStore/azure-sales-data-warehouse/gold"
    SAMPLE_DATA = "dbfs:/FileStore/azure-sales-data-warehouse/data_sample"
else:
    # If running outside DBFS (rare), change to mounted ADLS paths:
    # /mnt/adls/raw etc.
    BASE_RAW = "/mnt/raw"
    BASE_BRONZE = "/mnt/bronze"
    BASE_SILVER = "/mnt/silver"
    BASE_GOLD = "/mnt/gold"
    SAMPLE_DATA = "/mnt/data_sample"

# Tables (Delta)
T_BRONZE_CUSTOMERS = f"{BASE_BRONZE}/customers"
T_BRONZE_PRODUCTS  = f"{BASE_BRONZE}/products"
T_BRONZE_STORES    = f"{BASE_BRONZE}/stores"
T_BRONZE_ORDERS    = f"{BASE_BRONZE}/sales_orders"
T_BRONZE_ITEMS     = f"{BASE_BRONZE}/sales_order_items"

T_SILVER_CUSTOMERS = f"{BASE_SILVER}/customers"
T_SILVER_PRODUCTS  = f"{BASE_SILVER}/products"
T_SILVER_STORES    = f"{BASE_SILVER}/stores"
T_SILVER_ORDERS    = f"{BASE_SILVER}/sales_orders"
T_SILVER_ITEMS     = f"{BASE_SILVER}/sales_order_items"

T_GOLD_DIM_CUSTOMER = f"{BASE_GOLD}/dim_customer"
T_GOLD_DIM_PRODUCT  = f"{BASE_GOLD}/dim_product"
T_GOLD_DIM_STORE    = f"{BASE_GOLD}/dim_store"
T_GOLD_DIM_DATE     = f"{BASE_GOLD}/dim_date"
T_GOLD_FACT_SALES   = f"{BASE_GOLD}/fact_sales"

print("✅ Path setup complete")
print("SAMPLE_DATA:", SAMPLE_DATA)

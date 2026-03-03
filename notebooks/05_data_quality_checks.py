# 05_data_quality_checks.py
# Basic DQ checks + KPI preview queries

from pyspark.sql import functions as F

def read_delta(path: str):
    return spark.read.format("delta").load(path)

fact = read_delta(T_GOLD_FACT_SALES)
dim_customer = read_delta(T_GOLD_DIM_CUSTOMER)
dim_product  = read_delta(T_GOLD_DIM_PRODUCT)
dim_store    = read_delta(T_GOLD_DIM_STORE)

def assert_zero(df, msg):
    c = df.count()
    if c != 0:
        raise Exception(f"❌ DQ FAILED: {msg} | count={c}")
    print(f"✅ {msg}")

# 1) Null key checks
assert_zero(fact.filter(F.col("order_id").isNull() | F.col("order_item_id").isNull()), "fact_sales keys not null")

# 2) Duplicate grain check (order_id + order_item_id must be unique)
dup = (fact.groupBy("order_id", "order_item_id").count().filter(F.col("count") > 1))
assert_zero(dup, "fact_sales grain uniqueness")

# 3) Negative numeric checks
assert_zero(fact.filter((F.col("quantity") <= 0) | (F.col("net_amount") < 0) | (F.col("profit_amount").isNull())), "No invalid quantity/net/profit")

# 4) Referential integrity (optional)
missing_customer = fact.join(dim_customer, "customer_id", "left_anti")
missing_product  = fact.join(dim_product,  "product_id",  "left_anti")
missing_store    = fact.join(dim_store,    "store_id",    "left_anti")

assert_zero(missing_customer, "All fact.customer_id exist in dim_customer")
assert_zero(missing_product,  "All fact.product_id exist in dim_product")
assert_zero(missing_store,    "All fact.store_id exist in dim_store")

print("\n📊 Sample KPI: Top Products by Net Amount")
(fact.groupBy("product_id")
     .agg(F.sum("net_amount").alias("total_net"))
     .orderBy(F.col("total_net").desc())
     .show(10, truncate=False))

print("\n📊 Sample KPI: Store Performance")
(fact.groupBy("store_id")
     .agg(F.sum("net_amount").alias("total_net"),
          F.sum("profit_amount").alias("total_profit"),
          F.countDistinct("order_id").alias("orders"))
     .orderBy(F.col("total_net").desc())
     .show(10, truncate=False))

print("✅ DQ checks passed + KPIs generated")

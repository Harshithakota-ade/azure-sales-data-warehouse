# 02_silver_clean_dedup.py
# Bronze -> Silver: casting, standardization, dedup (window functions)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def read_delta(path: str):
    return spark.read.format("delta").load(path)

# Read Bronze
b_customers = read_delta(T_BRONZE_CUSTOMERS)
b_products  = read_delta(T_BRONZE_PRODUCTS)
b_stores    = read_delta(T_BRONZE_STORES)
b_orders    = read_delta(T_BRONZE_ORDERS)
b_items     = read_delta(T_BRONZE_ITEMS)

# ---- Customers ----
customers = (b_customers
    .withColumn("created_at", F.to_date("created_at"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    .withColumn("email", F.lower(F.col("email")))
)

w_c = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc_nulls_last(), F.col("_ingested_at").desc())
customers_dedup = (customers
    .withColumn("rn", F.row_number().over(w_c))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# ---- Products ----
products = (b_products
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("cost_price", F.col("cost_price").cast("double"))
    .withColumn("created_at", F.to_date("created_at"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
)

w_p = Window.partitionBy("product_id").orderBy(F.col("updated_at").desc_nulls_last(), F.col("_ingested_at").desc())
products_dedup = (products
    .withColumn("rn", F.row_number().over(w_p))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# ---- Stores ----
stores = (b_stores
    .withColumn("opened_date", F.to_date("opened_date"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
)

w_s = Window.partitionBy("store_id").orderBy(F.col("updated_at").desc_nulls_last(), F.col("_ingested_at").desc())
stores_dedup = (stores
    .withColumn("rn", F.row_number().over(w_s))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# ---- Orders ----
orders = (b_orders
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    .withColumn("order_status", F.upper(F.col("order_status")))
    .withColumn("payment_method", F.upper(F.col("payment_method")))
)

w_o = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc_nulls_last(), F.col("_ingested_at").desc())
orders_dedup = (orders
    .withColumn("rn", F.row_number().over(w_o))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# ---- Order Items ----
items = (b_items
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("discount_pct", F.col("discount_pct").cast("double"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
)

w_i = Window.partitionBy("order_id", "order_item_id").orderBy(F.col("updated_at").desc_nulls_last(), F.col("_ingested_at").desc())
items_dedup = (items
    .withColumn("rn", F.row_number().over(w_i))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# Write Silver
(customers_dedup.write.format("delta").mode("overwrite").save(T_SILVER_CUSTOMERS))
(products_dedup .write.format("delta").mode("overwrite").save(T_SILVER_PRODUCTS))
(stores_dedup   .write.format("delta").mode("overwrite").save(T_SILVER_STORES))
(orders_dedup   .write.format("delta").mode("overwrite").save(T_SILVER_ORDERS))
(items_dedup    .write.format("delta").mode("overwrite").save(T_SILVER_ITEMS))

print("✅ Silver clean + dedup complete")

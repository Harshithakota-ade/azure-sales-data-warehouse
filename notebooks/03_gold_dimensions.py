# 03_gold_dimensions.py
# Silver -> Gold dimensions (customer, product, store, date)

from pyspark.sql import functions as F

def read_delta(path: str):
    return spark.read.format("delta").load(path)

s_customers = read_delta(T_SILVER_CUSTOMERS)
s_products  = read_delta(T_SILVER_PRODUCTS)
s_stores    = read_delta(T_SILVER_STORES)
s_orders    = read_delta(T_SILVER_ORDERS)

# dim_customer
dim_customer = (s_customers
    .select(
        "customer_id", "customer_name", "email", "phone", "city", "state", "country",
        F.col("created_at").alias("customer_created_date"),
        F.col("updated_at").alias("customer_updated_ts")
    )
)

# dim_product
dim_product = (s_products
    .select(
        "product_id", "product_name", "category", "brand",
        "unit_price", "cost_price",
        F.col("created_at").alias("product_created_date"),
        F.col("updated_at").alias("product_updated_ts")
    )
)

# dim_store
dim_store = (s_stores
    .select(
        "store_id", "store_name", "store_type", "city", "state", "country",
        "opened_date",
        F.col("updated_at").alias("store_updated_ts")
    )
)

# dim_date from order_date values (simple)
dates = (s_orders
    .select(F.col("order_date").alias("date"))
    .dropna()
    .distinct()
)

dim_date = (dates
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_of_week", F.date_format("date", "E"))
)

# Write Gold
(dim_customer.write.format("delta").mode("overwrite").save(T_GOLD_DIM_CUSTOMER))
(dim_product .write.format("delta").mode("overwrite").save(T_GOLD_DIM_PRODUCT))
(dim_store   .write.format("delta").mode("overwrite").save(T_GOLD_DIM_STORE))
(dim_date    .write.format("delta").mode("overwrite").save(T_GOLD_DIM_DATE))

print("✅ Gold dimensions created")

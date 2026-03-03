# 04_gold_fact_sales.py
# Build fact_sales by joining orders + items + product prices (revenue, discount, profit)

from pyspark.sql import functions as F

def read_delta(path: str):
    return spark.read.format("delta").load(path)

orders = read_delta(T_SILVER_ORDERS)
items  = read_delta(T_SILVER_ITEMS)
prod   = read_delta(T_SILVER_PRODUCTS)

# Join order header + line items
base = (items.alias("i")
    .join(orders.alias("o"), F.col("i.order_id") == F.col("o.order_id"), "inner")
    .join(prod.alias("p"), F.col("i.product_id") == F.col("p.product_id"), "left")
)

# Calculate metrics
fact_sales = (base
    .withColumn("order_date", F.col("o.order_date"))
    .withColumn("date_key", F.date_format(F.col("o.order_date"), "yyyyMMdd").cast("int"))
    .withColumn("gross_amount", F.col("i.quantity") * F.col("p.unit_price"))
    .withColumn("discount_amount", F.col("gross_amount") * (F.col("i.discount_pct")/100.0))
    .withColumn("net_amount", F.col("gross_amount") - F.col("discount_amount"))
    .withColumn("cost_amount", F.col("i.quantity") * F.col("p.cost_price"))
    .withColumn("profit_amount", F.col("net_amount") - F.col("cost_amount"))
    .select(
        F.col("o.order_id").alias("order_id"),
        F.col("i.order_item_id").cast("int").alias("order_item_id"),
        F.col("o.customer_id").alias("customer_id"),
        F.col("o.store_id").alias("store_id"),
        F.col("i.product_id").alias("product_id"),
        F.col("o.order_date").alias("order_date"),
        F.col("date_key").alias("date_key"),
        F.col("o.order_status").alias("order_status"),
        F.col("o.payment_method").alias("payment_method"),
        F.col("i.quantity").alias("quantity"),
        F.col("p.unit_price").alias("unit_price"),
        F.col("p.cost_price").alias("cost_price"),
        F.col("i.discount_pct").alias("discount_pct"),
        F.col("gross_amount").alias("gross_amount"),
        F.col("discount_amount").alias("discount_amount"),
        F.col("net_amount").alias("net_amount"),
        F.col("profit_amount").alias("profit_amount")
    )
)

# Partition by order_date for query performance
(fact_sales.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("order_date")
    .save(T_GOLD_FACT_SALES)
)

print("✅ Gold fact_sales created")

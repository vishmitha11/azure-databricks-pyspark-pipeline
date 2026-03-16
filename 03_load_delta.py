# Databricks notebook source
from pyspark.sql.functions import col, when, datediff, round, sum as spark_sum, count, avg

raw_path = "/Volumes/workspace/default/olist_raw_data/"

orders = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(raw_path + "olist_orders_dataset.csv")
payments = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(raw_path + "olist_order_payments_dataset.csv")
customers = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(raw_path + "olist_customers_dataset.csv")

# Transform
orders_clean = orders \
    .dropDuplicates(["order_id"]) \
    .dropna(subset=["order_id", "customer_id", "order_status"]) \
    .withColumn("delivery_days", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
    .withColumn("delivery_status",
        when(col("order_delivered_customer_date") <= col("order_estimated_delivery_date"), "ON TIME")
        .when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"), "LATE")
        .otherwise("PENDING"))

payments_agg = payments \
    .groupBy("order_id") \
    .agg(
        spark_sum("payment_value").alias("total_payment"),
        count("payment_sequential").alias("payment_count"),
        avg("payment_installments").alias("avg_installments")
    ) \
    .withColumn("total_payment", round(col("total_payment"), 2)) \
    .withColumn("payment_category",
        when(col("total_payment") < 100, "LOW")
        .when(col("total_payment") < 500, "MEDIUM")
        .otherwise("HIGH"))

final_df = orders_clean \
    .join(payments_agg, on="order_id", how="left") \
    .join(customers.select("customer_id", "customer_city", "customer_state"), on="customer_id", how="left")

print(f"Data ready: {final_df.count()} rows")

# COMMAND ----------

# Define Delta output path
delta_path = "/Volumes/workspace/default/olist_raw_data/delta/orders_final"

# Write to Delta format
final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(delta_path)

print("Data written to Delta Lake successfully!")

# COMMAND ----------

# Read back from Delta to verify
df_delta = spark.read.format("delta").load(delta_path)

print(f"Delta table row count: {df_delta.count()}")

# Summary analytics
print("\n=== Delivery Status Summary ===")
df_delta.groupBy("delivery_status").count().orderBy("count", ascending=False).show()

print("\n=== Payment Category Summary ===")
df_delta.groupBy("payment_category").count().orderBy("count", ascending=False).show()

print("\n=== Top 5 States by Orders ===")
df_delta.groupBy("customer_state").count().orderBy("count", ascending=False).show(5)
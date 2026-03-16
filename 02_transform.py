# Databricks notebook source
# Reload DataFrames
raw_path = "/Volumes/workspace/default/olist_raw_data/"

orders = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(raw_path + "olist_orders_dataset.csv")
payments = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(raw_path + "olist_order_payments_dataset.csv")
customers = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(raw_path + "olist_customers_dataset.csv")
order_items = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(raw_path + "olist_order_items_dataset.csv")

print("Data reloaded successfully!")

# COMMAND ----------

from pyspark.sql.functions import col, when, datediff, round

# Clean orders — drop nulls on critical columns
orders_clean = orders \
    .dropDuplicates(["order_id"]) \
    .dropna(subset=["order_id", "customer_id", "order_status"]) \
    .withColumn("delivery_days",
        datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
    .withColumn("delivery_status",
        when(col("order_delivered_customer_date") <= col("order_estimated_delivery_date"), "ON TIME")
        .when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"), "LATE")
        .otherwise("PENDING"))

print(f"Orders after cleaning: {orders_clean.count()} rows")
orders_clean.show(5, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum, count, avg

# Aggregate payments per order
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

print(f"Payments aggregated: {payments_agg.count()} rows")
payments_agg.show(5)

# COMMAND ----------

# Join orders + payments + customers
final_df = orders_clean \
    .join(payments_agg, on="order_id", how="left") \
    .join(customers.select("customer_id", "customer_city", "customer_state"), 
          on="customer_id", how="left")

# Data validation check
assert final_df.filter(col("order_id").isNull()).count() == 0, "Null order IDs found!"
assert final_df.filter(col("total_payment") < 0).count() == 0, "Negative payments found!"

print(f"Final joined DataFrame: {final_df.count()} rows")
print(f"Columns: {final_df.columns}")
final_df.show(5, truncate=False)
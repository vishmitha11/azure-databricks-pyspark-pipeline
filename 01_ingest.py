# Databricks notebook source
# Define paths
raw_path = "/Volumes/workspace/default/olist_raw_data/"

# Verify files are visible
files = dbutils.fs.ls(raw_path)
for f in files:
    print(f.name)

# COMMAND ----------

# Read all CSV files into DataFrames
customers = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(raw_path + "olist_customers_dataset.csv")
orders = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(raw_path + "olist_orders_dataset.csv")
payments = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(raw_path + "olist_order_payments_dataset.csv")
order_items = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(raw_path + "olist_order_items_dataset.csv")
products = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(raw_path + "olist_products_dataset.csv")
sellers = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(raw_path + "olist_sellers_dataset.csv")

print("All DataFrames loaded successfully!")
print(f"Orders: {orders.count()} rows")
print(f"Customers: {customers.count()} rows")
print(f"Payments: {payments.count()} rows")
print(f"Order Items: {order_items.count()} rows")

# COMMAND ----------

# Preview schemas and sample data
print("=== ORDERS SCHEMA ===")
orders.printSchema()

print("=== PAYMENTS SCHEMA ===")
payments.printSchema()

print("=== SAMPLE ORDERS ===")
orders.show(5, truncate=False)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, datediff
import pytest

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local") \
        .appName("pipeline-tests") \
        .getOrCreate()

def test_delivery_status_on_time(spark):
    # Create sample data
    data = [("order1", "2024-01-01", "2024-01-10", "2024-01-15")]
    df = spark.createDataFrame(data, 
        ["order_id", "purchase_date", "delivered_date", "estimated_date"])

    # Apply transformation
    df = df.withColumn("delivery_status",
        when(col("delivered_date") <= col("estimated_date"), "ON TIME")
        .when(col("delivered_date") > col("estimated_date"), "LATE")
        .otherwise("PENDING"))

    result = df.collect()[0]["delivery_status"]
    assert result == "ON TIME"

def test_delivery_status_late(spark):
    data = [("order2", "2024-01-01", "2024-01-20", "2024-01-15")]
    df = spark.createDataFrame(data,
        ["order_id", "purchase_date", "delivered_date", "estimated_date"])

    df = df.withColumn("delivery_status",
        when(col("delivered_date") <= col("estimated_date"), "ON TIME")
        .when(col("delivered_date") > col("estimated_date"), "LATE")
        .otherwise("PENDING"))

    result = df.collect()[0]["delivery_status"]
    assert result == "LATE"

def test_payment_category_low(spark):
    data = [("order1", 50.0)]
    df = spark.createDataFrame(data, ["order_id", "total_payment"])

    df = df.withColumn("payment_category",
        when(col("total_payment") < 100, "LOW")
        .when(col("total_payment") < 500, "MEDIUM")
        .otherwise("HIGH"))

    result = df.collect()[0]["payment_category"]
    assert result == "LOW"

def test_no_negative_payments(spark):
    data = [("order1", 50.0), ("order2", 200.0), ("order3", 600.0)]
    df = spark.createDataFrame(data, ["order_id", "total_payment"])

    negative_count = df.filter(col("total_payment") < 0).count()
    assert negative_count == 0

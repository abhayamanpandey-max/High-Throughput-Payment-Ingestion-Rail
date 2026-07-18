from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, window, count, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Initialize Spark Session optimized for Delta Lake
spark = SparkSession.builder \
    .appName("PaymentMedallionPipeline") \
    .getOrCreate()

# Define Input Schema matching the raw ingestion driver
raw_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("event_timestamp", StringType(), True)
])

# =====================================================================
# 1. BRONZE LAYER: Raw Data Ingestion (Delta format)
# =====================================================================
def load_bronze_layer():
    # Simulating loading raw streaming payloads from Kafka land zone
    # In production, substitute with spark.readStream.format("kafka")
    raw_df = spark.read.format("json").load("/mnt/landing/payment_events/")
    
    bronze_df = raw_df.withColumn("ingest_timestamp", current_timestamp()) \
                      .withColumn("source_system", col("metadata.source"))
                      
    # Write to Unity Catalog / Managed Delta Lake table
    bronze_df.write.format("delta").mode("append").saveAsTable("hive_metastore.default.payment_bronze")

# =====================================================================
# 2. SILVER LAYER: Data Cleansing & Contract Enforcement
# =====================================================================
def process_silver_layer():
    bronze_df = spark.read.table("hive_metastore.default.payment_bronze")
    
    # Extract, flatten, cast types, and flag malformed records
    silver_df = bronze_df.select(
        col("transaction_id").cast(StringType()),
        col("account_id").cast(StringType()),
        col("amount").cast(DoubleType()),
        col("currency").cast(StringType()),
        col("merchant_id").cast(StringType()),
        col("event_timestamp").cast("timestamp")
    ).filter(col("transaction_id").isNotNull() & (col("amount") >= 0))
    
    silver_df.write.format("delta").mode("append").saveAsTable("hive_metastore.default.payment_silver")

# =====================================================================
# 3. GOLD LAYER: Rolling Window Metrics & Anomaly Detection
# =====================================================================
def aggregate_gold_layer():
    silver_df = spark.read.table("hive_metastore.default.payment_silver")
    
    # Compute 5-minute rolling window tracking transaction frequency and velocity
    gold_df = silver_df.groupBy(
        window(col("event_timestamp"), "5 minutes"),
        col("account_id")
    ).agg(
        count("transaction_id").alias("tx_count_5m"),
        sum("amount").alias("total_spent_5m")
    ).withColumn(
        "is_anomaly", 
        (col("tx_count_5m") > 10) | (col("total_spent_5m") > 10000.0)
    )
    
    gold_df.write.format("delta").mode("overwrite").saveAsTable("hive_metastore.default.payment_gold")

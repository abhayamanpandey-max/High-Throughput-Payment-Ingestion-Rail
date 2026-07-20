import os
import sys

# ==============================================================================
# SENIOR DE ARCHITECTURE: CRITICAL WINDOWS ENVIRONMENT SETUP
# ==============================================================================
if sys.platform.startswith("win"):
    # 1. Dynamically wire Java and Hadoop paths directly into the running process
    os.environ["JAVA_HOME"] = r"C:\Program Files\Amazon Corretto\jdk17.0.19_10"
    os.environ["HADOOP_HOME"] = r"C:\Hadoop"
    
    hadoop_bin = r"C:\Hadoop\bin"
    java_bin = r"C:\Program Files\Amazon Corretto\jdk17.0.19_10\bin"
    
    # 2. Prepend BOTH to PATH so Windows has absolute clarity
    paths_to_add = []
    if os.path.exists(hadoop_bin):
        paths_to_add.append(hadoop_bin)
    if os.path.exists(java_bin):
        paths_to_add.append(java_bin)
        
    if paths_to_add:
        os.environ["PATH"] = os.pathsep.join(paths_to_add) + os.pathsep + os.environ.get("PATH", "")
        print(f"[+] Prioritized Hadoop and Java binaries in PATH")

    # 3. Inject library paths directly into PySpark launch args before the JVM starts
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--driver-java-options "-Djava.library.path=C:/Hadoop/bin" '
        '--conf spark.driver.extraJavaOptions="-Djava.library.path=C:/Hadoop/bin" '
        '--conf spark.executor.extraJavaOptions="-Djava.library.path=C:/Hadoop/bin" '
        'pyspark-shell'
    )

# Run Environment Debug Diagnostics
print(f"--- ENVIRONMENT DIAGNOSTICS ---")
print(f"DEBUG HADOOP_HOME: {os.environ.get('HADOOP_HOME')}")
print(f"DEBUG PATH HAS HADOOP: {'hadoop' in os.environ.get('PATH', '').lower()}")
winutils_path = os.path.join(os.environ.get('HADOOP_HOME', ''), 'bin', 'winutils.exe')
print(f"DEBUG WINUTILS EXISTS: {os.path.exists(winutils_path)}")
print(f"--------------------------------\n")

# Now it is safe to import PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, BooleanType

def run_delta_lakehouse_pipeline():
    print("[+] Initializing PySpark Session with Delta Lake extensions...")
    
    # Setup dedicated local temp directory to avoid Windows AppData locking issues
    local_tmp_dir = os.path.abspath("./spark-temp")
    os.makedirs(local_tmp_dir, exist_ok=True)
    
    spark = (SparkSession.builder
        .appName("PaymentRail-DeltaLakehouse")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0")
        .config("spark.driver.extraJavaOptions", "-Djava.library.path=C:/Hadoop/bin")
        .config("spark.executor.extraJavaOptions", "-Djava.library.path=C:/Hadoop/bin")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # Override temp directory to completely eliminate the ShutdownHookManager error
        .config("spark.local.dir", local_tmp_dir)

        # --- CRITICAL WINDOWS STREAMING BYPASS CONFIGS ---
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.RawLocalFs")
        .config("spark.hadoop.fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.local.RawLocalFs")
        # -------------------------------------------------
        .getOrCreate())

    print("[+] Spark session active. Subscribing to 'payment_bronze' Kafka stream...")
    
    # Match the schema emitted by the Flink anomaly pipeline
    payment_schema = StructType() \
        .add("transaction_id", StringType(), True) \
        .add("account_id", StringType(), True) \
        .add("amount", DoubleType(), True) \
        .add("merchant", StringType(), True) \
        .add("location", StringType(), True) \
        .add("ts", StringType(), True) \
        .add("is_anomaly", BooleanType(), True) \
        .add("anomaly_score", DoubleType(), True)

    # Read Stream from the Kafka Sink Topic populated by Flink
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "payment_bronze") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Get absolute paths with file:/// protocol wrapper to bypass Windows path parsing bugs
    data_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data"))

    bronze_dir = f"file:///{os.path.join(data_root, 'bronze').replace(os.sep, '/')}"
    bronze_checkpoint = f"file:///{os.path.join(data_root, 'bronze', '_checkpoints').replace(os.sep, '/')}"

    print(f"[+] Starting Bronze Layer ingestion to: {bronze_dir}")
    print(f"[+] Using checkpoint location: {bronze_checkpoint}")

    bronze_df = kafka_stream_df \
        .selectExpr(
            "CAST(key AS STRING) as kafka_key",
            "CAST(value AS STRING) as raw_payload",
            "topic as kafka_topic",
            "partition as kafka_partition",
            "offset as kafka_offset",
            "timestamp as kafka_timestamp"
        )

    bronze_query = bronze_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", bronze_checkpoint) \
        .start(bronze_dir)

    # Silver Layer: parse the raw payload saved in Bronze
    silver_df = bronze_df \
        .select(from_json(col("raw_payload"), payment_schema).alias("data")) \
        .select("data.*") \
        .withColumn("ts", to_timestamp(col("ts")))

    silver_dir = f"file:///{os.path.join(data_root, 'silver_payments').replace(os.sep, '/')}"
    silver_checkpoint = f"file:///{os.path.join(data_root, 'silver_payments', '_checkpoints').replace(os.sep, '/')}"
    
    print(f"[+] Starting Silver Layer ingestion to: {silver_dir}")
    print(f"[+] Using checkpoint location: {silver_checkpoint}")

    silver_query = silver_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", silver_checkpoint) \
        .start(silver_dir)

    print(f"[+] Ingestion active! Writing Delta tables to system.")
    
    # Keep the streaming context alive until manually stopped
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_delta_lakehouse_pipeline()

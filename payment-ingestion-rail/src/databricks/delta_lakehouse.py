"""
Local Delta Lakehouse Pipeline (Development Mode)
Reads from Kafka (payment_bronze topic) → writes Bronze + Silver Delta tables locally.

This module is for local development and testing WITHOUT a Databricks cloud workspace.
For production, use databricks_loader.py + silver_transform.sql / gold_transform.sql.

Environment variables (set via .env or shell):
  KAFKA_BOOTSTRAP_SERVERS  - default: localhost:9092
  JAVA_HOME                - JDK path (required for PySpark)
  HADOOP_HOME              - Hadoop path (required on Windows for winutils.exe)
"""

import os
import sys
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# PLATFORM-AGNOSTIC JAVA / HADOOP SETUP
# Reads from environment variables so this file works on Linux, macOS, and Windows
# without any hardcoded paths.  Set JAVA_HOME and HADOOP_HOME in your .env or shell.
# ============================================================================
def _configure_java_hadoop():
    """Validate and configure Java/Hadoop paths from environment variables."""
    java_home = os.environ.get("JAVA_HOME")
    hadoop_home = os.environ.get("HADOOP_HOME")

    if not java_home:
        logger.warning(
            "[WARN] JAVA_HOME is not set. PySpark may fail to start. "
            "Set JAVA_HOME in your .env file pointing to a JDK 11/17 installation."
        )
    if not hadoop_home and sys.platform.startswith("win"):
        logger.warning(
            "[WARN] HADOOP_HOME is not set on Windows. "
            "Download winutils from https://github.com/cdarlint/winutils and set HADOOP_HOME."
        )

    # Prepend binaries to PATH so the JVM can find native libraries
    paths_to_add = []
    for home, subdir in [(hadoop_home, "bin"), (java_home, "bin")]:
        if home:
            candidate = os.path.join(home, subdir)
            if os.path.exists(candidate):
                paths_to_add.append(candidate)

    if paths_to_add:
        os.environ["PATH"] = os.pathsep.join(paths_to_add) + os.pathsep + os.environ.get("PATH", "")
        logger.info("[INIT] Added to PATH: %s", paths_to_add)

    if hadoop_home:
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f'--driver-java-options "-Djava.library.path={hadoop_home}/bin" '
            f'--conf spark.driver.extraJavaOptions="-Djava.library.path={hadoop_home}/bin" '
            "pyspark-shell"
        )


_configure_java_hadoop()

# Now safe to import PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, BooleanType


def run_delta_lakehouse_pipeline():
    """
    Streaming pipeline: Kafka payment_bronze → local Delta Bronze + Silver tables.

    Bronze: stores raw Kafka envelope (key, value, offset, partition, timestamp)
    Silver: parses and casts the JSON payload into typed columns
    """
    logger.info("[START] Initialising PySpark Session with Delta Lake extensions...")

    local_tmp_dir = os.path.abspath("./spark-temp")
    os.makedirs(local_tmp_dir, exist_ok=True)

    hadoop_home = os.environ.get("HADOOP_HOME", "")
    hadoop_lib = os.path.join(hadoop_home, "bin").replace("\\", "/") if hadoop_home else ""

    spark = (
        SparkSession.builder
        .appName("PaymentRail-DeltaLakehouse-Dev")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "io.delta:delta-spark_2.12:3.1.0",
        )
        .config("spark.driver.extraJavaOptions",
                f"-Djava.library.path={hadoop_lib}" if hadoop_lib else "")
        .config("spark.executor.extraJavaOptions",
                f"-Djava.library.path={hadoop_lib}" if hadoop_lib else "")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.local.dir", local_tmp_dir)
        # Windows streaming bypass — safe to leave on Linux/macOS (no-op)
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl",
                "org.apache.hadoop.fs.local.RawLocalFs")
        .getOrCreate()
    )

    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info("[CONFIG] Kafka bootstrap: %s", kafka_servers)

    # Schema matches Flink output on payment_bronze topic
    payment_schema = (
        StructType()
        .add("transaction_id", StringType(), True)
        .add("account_id", StringType(), True)
        .add("amount", DoubleType(), True)
        .add("merchant", StringType(), True)
        .add("location", StringType(), True)
        .add("ts", StringType(), True)
        .add("is_anomaly", BooleanType(), True)
        .add("anomaly_score", DoubleType(), True)
    )

    kafka_stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", "payment_bronze")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Resolve absolute data paths using file:/// protocol for Spark compatibility
    data_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "data")
    )

    def _to_file_uri(path: str) -> str:
        return "file:///" + path.replace(os.sep, "/")

    bronze_dir = _to_file_uri(os.path.join(data_root, "bronze"))
    bronze_checkpoint = _to_file_uri(os.path.join(data_root, "bronze", "_checkpoints"))
    silver_dir = _to_file_uri(os.path.join(data_root, "silver_payments"))
    silver_checkpoint = _to_file_uri(os.path.join(data_root, "silver_payments", "_checkpoints"))

    logger.info("[BRONZE] Writing to: %s", bronze_dir)
    logger.info("[SILVER] Writing to: %s", silver_dir)

    # --- Bronze layer: raw Kafka envelope ---
    bronze_df = kafka_stream_df.selectExpr(
        "CAST(key AS STRING) as kafka_key",
        "CAST(value AS STRING) as raw_payload",
        "topic as kafka_topic",
        "partition as kafka_partition",
        "offset as kafka_offset",
        "timestamp as kafka_timestamp",
    )

    bronze_query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", bronze_checkpoint)
        .start(bronze_dir)
    )

    # --- Silver layer: parsed + typed ---
    silver_df = (
        bronze_df
        .select(from_json(col("raw_payload"), payment_schema).alias("data"))
        .select("data.*")
        .withColumn("ts", to_timestamp(col("ts")))
    )

    silver_query = (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", silver_checkpoint)
        .start(silver_dir)
    )

    logger.info("[RUNNING] Bronze and Silver streaming queries active. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    run_delta_lakehouse_pipeline()

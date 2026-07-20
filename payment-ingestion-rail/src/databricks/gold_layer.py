import os
import sys

# ============================================================================
# WINDOWS ENVIRONMENT SETUP FOR LOCAL SPARK / DELTA EXECUTION
# ============================================================================
if sys.platform.startswith("win"):
    os.environ["JAVA_HOME"] = r"C:\Program Files\Amazon Corretto\jdk17.0.19_10"
    os.environ["HADOOP_HOME"] = r"C:\Hadoop"

    hadoop_bin = r"C:\Hadoop\bin"
    java_bin = r"C:\Program Files\Amazon Corretto\jdk17.0.19_10\bin"

    paths_to_add = []
    if os.path.exists(hadoop_bin):
        paths_to_add.append(hadoop_bin)
    if os.path.exists(java_bin):
        paths_to_add.append(java_bin)

    if paths_to_add:
        os.environ["PATH"] = os.pathsep.join(paths_to_add) + os.pathsep + os.environ.get("PATH", "")
        print("[+] Prioritized Hadoop and Java binaries in PATH")

print("--- ENVIRONMENT DIAGNOSTICS ---")
print(f"DEBUG HADOOP_HOME: {os.environ.get('HADOOP_HOME')}")
print(f"DEBUG PATH HAS HADOOP: {'hadoop' in os.environ.get('PATH', '').lower()}")
winutils_path = os.path.join(os.environ.get('HADOOP_HOME', ''), 'bin', 'winutils.exe')
print(f"DEBUG WINUTILS EXISTS: {os.path.exists(winutils_path)}")
print("--------------------------------\n")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, sum, avg, max as spark_max
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, BooleanType


def run_gold_layer_pipeline():
    print("[+] Initializing PySpark Session for Gold layer aggregation...")

    local_tmp_dir = os.path.abspath("./spark-temp")
    os.makedirs(local_tmp_dir, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("PaymentRail-GoldLayer")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.driver.extraJavaOptions", "-Djava.library.path=C:/Hadoop/bin")
        .config("spark.executor.extraJavaOptions", "-Djava.library.path=C:/Hadoop/bin")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.local.dir", local_tmp_dir)
        .getOrCreate()
    )

    data_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data"))
    silver_dir = os.path.join(data_root, "silver_payments")
    gold_dir = os.path.join(data_root, "gold")
    gold_checkpoint = os.path.join(data_root, "gold", "_checkpoints")

    print(f"[+] Reading Silver layer from: {silver_dir}")
    print(f"[+] Writing Gold layer to: {gold_dir}")
    print(f"[+] Using checkpoint location: {gold_checkpoint}")

    silver_df = spark.read.format("delta").load(silver_dir)

    gold_df = silver_df.groupBy(
        window(col("ts"), "5 minutes"),
        col("account_id")
    ).agg(
        count("transaction_id").alias("tx_count_5m"),
        sum("amount").alias("total_spent_5m"),
        avg("amount").alias("avg_amount_5m"),
        spark_max("amount").alias("max_amount_5m"),
        spark_max(col("is_anomaly").cast("int")).alias("has_anomaly")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("account_id"),
        col("tx_count_5m"),
        col("total_spent_5m"),
        col("avg_amount_5m"),
        col("max_amount_5m"),
        ((col("tx_count_5m") > 10) | (col("total_spent_5m") > 10000.0) | (col("has_anomaly") > 0)).alias("is_anomaly")
    )

    gold_df.write.format("delta").mode("overwrite").save(gold_dir)

    print("[+] Gold layer materialized successfully.")


if __name__ == "__main__":
    run_gold_layer_pipeline()

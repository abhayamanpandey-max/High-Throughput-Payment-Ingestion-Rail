"""
Gold Layer Aggregation (Local Dev Mode)
Reads Silver Delta table → computes 5-minute windowed anomaly metrics → writes Gold Delta table.

This is the local-dev equivalent of gold_transform.sql (which runs on Databricks).
Use this file to validate Gold logic without a cloud workspace.

Environment variables (set via .env or shell):
  JAVA_HOME   - required for PySpark
  HADOOP_HOME - required on Windows for winutils.exe
"""

import os
import sys
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# PLATFORM-AGNOSTIC JAVA / HADOOP SETUP
# All paths driven by environment variables — no hardcoded Windows paths.
# ============================================================================
def _configure_java_hadoop():
    """Validate and configure Java/Hadoop paths from environment variables."""
    java_home = os.environ.get("JAVA_HOME")
    hadoop_home = os.environ.get("HADOOP_HOME")

    if not java_home:
        logger.warning(
            "[WARN] JAVA_HOME is not set. PySpark may fail to start."
        )
    if not hadoop_home and sys.platform.startswith("win"):
        logger.warning(
            "[WARN] HADOOP_HOME is not set on Windows. "
            "Set HADOOP_HOME to your winutils directory."
        )

    paths_to_add = []
    for home, subdir in [(hadoop_home, "bin"), (java_home, "bin")]:
        if home:
            candidate = os.path.join(home, subdir)
            if os.path.exists(candidate):
                paths_to_add.append(candidate)

    if paths_to_add:
        os.environ["PATH"] = (
            os.pathsep.join(paths_to_add) + os.pathsep + os.environ.get("PATH", "")
        )
        logger.info("[INIT] Added to PATH: %s", paths_to_add)


_configure_java_hadoop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, sum, avg, max as spark_max


def run_gold_layer_pipeline():
    """
    Batch job: read Silver Delta → aggregate into 5-minute windows → write Gold Delta.

    Gold layer schema:
      window_start  TIMESTAMP
      window_end    TIMESTAMP
      account_id    STRING
      tx_count_5m   BIGINT    — transaction count in the 5-min window
      total_spent_5m DECIMAL  — total amount transacted
      avg_amount_5m  DECIMAL
      max_amount_5m  DECIMAL
      is_anomaly     BOOLEAN  — true if count > 10 OR total > 10,000 OR any tx flagged
    """
    logger.info("[START] Initialising PySpark Session for Gold layer aggregation...")

    local_tmp_dir = os.path.abspath("./spark-temp")
    os.makedirs(local_tmp_dir, exist_ok=True)

    hadoop_home = os.environ.get("HADOOP_HOME", "")
    hadoop_lib = os.path.join(hadoop_home, "bin").replace("\\", "/") if hadoop_home else ""

    spark = (
        SparkSession.builder
        .appName("PaymentRail-GoldLayer-Dev")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.driver.extraJavaOptions",
                f"-Djava.library.path={hadoop_lib}" if hadoop_lib else "")
        .config("spark.executor.extraJavaOptions",
                f"-Djava.library.path={hadoop_lib}" if hadoop_lib else "")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.local.dir", local_tmp_dir)
        .getOrCreate()
    )

    data_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "data")
    )
    silver_dir = os.path.join(data_root, "silver_payments")
    gold_dir = os.path.join(data_root, "gold")

    logger.info("[READ]  Silver layer: %s", silver_dir)
    logger.info("[WRITE] Gold layer:   %s", gold_dir)

    silver_df = spark.read.format("delta").load(silver_dir)

    gold_df = (
        silver_df.groupBy(
            window(col("ts"), "5 minutes"),
            col("account_id"),
        )
        .agg(
            count("transaction_id").alias("tx_count_5m"),
            sum("amount").alias("total_spent_5m"),
            avg("amount").alias("avg_amount_5m"),
            spark_max("amount").alias("max_amount_5m"),
            spark_max(col("is_anomaly").cast("int")).alias("has_anomaly"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("account_id"),
            col("tx_count_5m"),
            col("total_spent_5m"),
            col("avg_amount_5m"),
            col("max_amount_5m"),
            (
                (col("tx_count_5m") > 10)
                | (col("total_spent_5m") > 10_000.0)
                | (col("has_anomaly") > 0)
            ).alias("is_anomaly"),
        )
    )

    gold_df.write.format("delta").mode("overwrite").save(gold_dir)
    logger.info("[OK] Gold layer materialised successfully. Rows: %d", gold_df.count())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    run_gold_layer_pipeline()

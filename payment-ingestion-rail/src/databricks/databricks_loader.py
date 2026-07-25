"""
Databricks Bronze Loader
Consumes anomaly-scored payment events from Kafka (payment_bronze topic)
and batch-writes them into a Databricks Delta table.

This decouples Flink (real-time processing) from Databricks ingestion,
so a slow/unavailable warehouse never backpressures the streaming job.
"""

import os
import json
import logging
import time
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer
from databricks import sql as databricks_sql
from urllib.parse import urlparse

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('databricks_loader.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DatabricksBronzeLoader:
    def __init__(self):
        self.server_hostname = os.environ["DATABRICKS_SERVER_HOSTNAME"]
        self.http_path = os.environ["DATABRICKS_HTTP_PATH"]
        self.token = os.environ["DATABRICKS_TOKEN"]
        self.kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = os.getenv("KAFKA_BRONZE_TOPIC", "payment_bronze")

        self.consumer = Consumer({
            "bootstrap.servers": self.kafka_servers,
            "group.id": "databricks-bronze-loader",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # commit only after successful write
        })
        self.consumer.subscribe([self.topic])

        self.batch_size = int(os.getenv("LOADER_BATCH_SIZE", 500))
        self.batch_timeout_seconds = int(os.getenv("LOADER_BATCH_TIMEOUT", 5))

    def _get_connection(self):
    # --- TEMPORARY DEBUG LOGS ---
        print("\n" + "="*50)
        print(f"DEBUG raw server_hostname: '{self.server_hostname}'")
        print(f"DEBUG raw http_path:       '{self.http_path}'")
        print("="*50 + "\n")

    # Sanitize the hostname in case https:// or trailing slashes were included
        clean_hostname = (
            self.server_hostname
            .replace("https://", "")
            .replace("http://", "")
            .strip("/")
         )
    # Dynamically extract host safely regardless of how it's formatted in .env
        raw_host = self.server_hostname
        if not raw_host.startswith(("http://", "https://")):
            raw_host = f"https://{raw_host}"    

    # Ensure http_path starts with a slash
        clean_hostname = urlparse(raw_host).netloc or self.server_hostname
        clean_http_path = self.http_path if self.http_path.startswith("/") else f"/{self.http_path}"

        return databricks_sql.connect(
            server_hostname=clean_hostname,
            http_path=clean_http_path,
            access_token=self.token
        )

    # def _get_connection(self):
    #     return databricks_sql.connect(
    #         server_hostname=self.server_hostname,
    #         http_path=self.http_path,
    #         access_token=self.token,
    #     )
    
    def _write_batch(self, batch):
        if not batch:
            return

        rows = [
            (
                r["transaction_id"], r["account_id"], r["amount"],
                r["merchant"], r["location"], r["ts"],
                r["is_anomaly"], r["anomaly_score"],
            )
            for r in batch
        ]

        insert_sql = """
            INSERT INTO payment_pipeline.bronze.payments
            (transaction_id, account_id, amount, merchant, location, ts, is_anomaly, anomaly_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(insert_sql, rows)

        logger.info(f"[LOAD] Wrote {len(batch)} rows to payment_pipeline.bronze.payments")

    def run(self):
        logger.info(f"[START] Databricks loader consuming '{self.topic}' -> Delta Bronze table")
        batch = []
        last_flush = time.time()

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    if batch and (time.time() - last_flush) > self.batch_timeout_seconds:
                        self._write_batch(batch)
                        self.consumer.commit()
                        batch = []
                        last_flush = time.time()
                    continue

                if msg.error():
                    logger.error(f"[ERROR] Kafka error: {msg.error()}")
                    continue

                try:
                    record = json.loads(msg.value())
                    batch.append(record)
                except json.JSONDecodeError as e:
                    logger.warning(f"[WARN] Skipping malformed message: {e}")
                    continue

                if len(batch) >= self.batch_size:
                    self._write_batch(batch)
                    self.consumer.commit()
                    batch = []
                    last_flush = time.time()

        except KeyboardInterrupt:
            logger.warning("[STOP] Interrupted by user")
            if batch:
                self._write_batch(batch)
                self.consumer.commit()
        finally:
            self.consumer.close()
            logger.info("[OK] Consumer closed cleanly")


if __name__ == "__main__":
    loader = DatabricksBronzeLoader()
    loader.run()
    
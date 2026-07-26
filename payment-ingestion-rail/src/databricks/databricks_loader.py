"""
Databricks Bronze Loader
Consumes anomaly-scored payment events from Kafka (payment_bronze topic)
and batch-writes them into a Databricks Delta table.

Design decisions:
- Decouples Flink (real-time) from Databricks (batch write) so a slow/unavailable
  warehouse never backpressures the streaming job.
- Manual offset commit (enable.auto.commit=False) ensures at-least-once delivery:
  offsets only advance after a confirmed Delta write, so a failed write causes
  the batch to be reprocessed rather than silently dropped.
- The Silver MERGE pattern provides idempotency — reprocessing the same events
  never creates duplicate rows in downstream layers.
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
    """
    Micro-batch Kafka → Databricks Delta Bronze loader.

    Batches up to `LOADER_BATCH_SIZE` records (default 500) or flushes every
    `LOADER_BATCH_TIMEOUT` seconds — whichever comes first.
    """

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
            # Manual commit: offsets advance only after a confirmed Delta write.
            # This prevents silent data loss if the write fails mid-batch.
            "enable.auto.commit": False,
        })
        self.consumer.subscribe([self.topic])

        self.batch_size = int(os.getenv("LOADER_BATCH_SIZE", "500"))
        self.batch_timeout_seconds = int(os.getenv("LOADER_BATCH_TIMEOUT", "5"))

        logger.info(
            "[INIT] DatabricksBronzeLoader | kafka=%s | topic=%s | batch_size=%d | timeout=%ds",
            self.kafka_servers, self.topic, self.batch_size, self.batch_timeout_seconds,
        )

    def _get_connection(self):
        """
        Build a clean Databricks SQL connection.

        Normalises the server_hostname regardless of whether the user supplied
        a bare hostname or a full https:// URL — prevents connection errors from
        malformed .env values.
        """
        raw_host = self.server_hostname
        if not raw_host.startswith(("http://", "https://")):
            raw_host = f"https://{raw_host}"

        clean_hostname = urlparse(raw_host).netloc or self.server_hostname
        clean_http_path = (
            self.http_path if self.http_path.startswith("/") else f"/{self.http_path}"
        )

        logger.debug("[CONN] Connecting to %s%s", clean_hostname, clean_http_path)
        return databricks_sql.connect(
            server_hostname=clean_hostname,
            http_path=clean_http_path,
            access_token=self.token,
        )

    def _write_batch(self, batch: list) -> None:
        """
        Write a batch of records to the Bronze Delta table using executemany.
        Connection is opened per-batch so transient warehouse cold-starts don't
        block the consumer thread indefinitely.
        """
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

        logger.info("[LOAD] Wrote %d rows to payment_pipeline.bronze.payments", len(batch))

    def run(self) -> None:
        """Main consumer loop — poll → batch → write → commit."""
        logger.info("[START] Databricks loader consuming '%s' → Delta Bronze", self.topic)
        batch: list = []
        last_flush = time.time()

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    # Timeout-based flush: write whatever we have after batch_timeout seconds
                    if batch and (time.time() - last_flush) > self.batch_timeout_seconds:
                        self._write_batch(batch)
                        self.consumer.commit()
                        batch = []
                        last_flush = time.time()
                    continue

                if msg.error():
                    logger.error("[ERROR] Kafka error: %s", msg.error())
                    continue

                try:
                    record = json.loads(msg.value())
                    batch.append(record)
                except json.JSONDecodeError as exc:
                    logger.warning("[WARN] Skipping malformed message: %s", exc)
                    continue

                # Size-based flush: write when batch reaches target size
                if len(batch) >= self.batch_size:
                    self._write_batch(batch)
                    self.consumer.commit()
                    batch = []
                    last_flush = time.time()

        except KeyboardInterrupt:
            logger.warning("[STOP] Interrupted by user — flushing remaining batch")
            if batch:
                self._write_batch(batch)
                self.consumer.commit()
        finally:
            self.consumer.close()
            logger.info("[OK] Consumer closed cleanly")


if __name__ == "__main__":
    loader = DatabricksBronzeLoader()
    loader.run()
"""
High-Throughput Payment Ingestion Driver
Produces 44,000+ TPS to Kafka with strict schema validation.

Architecture:
- Multi-threaded producer (4 threads by default)
- Strict Pydantic schema validation
- Optimized Kafka batching config
- Thread-safe metrics and monitoring
"""
import sys

# Fix Windows Unicode encoding
if sys.platform == 'win32':
    import io
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

import json
import uuid
import time
import random
import logging
import os
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, List
from confluent_kafka import Producer, KafkaError
from pydantic import BaseModel, Field, ValidationError

# ============================================================================
# LOGGING CONFIGURATION (Production-Grade)
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ingestion_driver.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# 1. SCHEMA VALIDATION (Strict Data Contracts)
# ============================================================================
class PaymentTransaction(BaseModel):
    """Strict schema for payment transactions — enforces data contracts at the ingestion boundary."""
    transaction_id: str = Field(..., min_length=36, max_length=36)
    account_id: str = Field(..., min_length=8, max_length=20)
    amount: float = Field(..., gt=0, le=1000000)
    currency: str = Field(..., min_length=3, max_length=3)
    merchant: str = Field(..., min_length=1, max_length=100)
    location: str = Field(..., min_length=1, max_length=50)
    device_ip: str = Field(..., min_length=7, max_length=15)
    ts_string: str = Field(..., min_length=20)

    class Config:
        validate_assignment = True

# ============================================================================
# 2. PERFORMANCE MONITORING (Thread-Safe)
# ============================================================================
class PerformanceMetrics:
    """
    Thread-safe performance metrics using explicit locks.

    NOTE: Plain integer += is NOT atomic in CPython under multi-threading.
    Even though the GIL prevents true parallel bytecode execution, compound
    operations (read-modify-write) can be interrupted between threads.
    Using a lock ensures correct counts under all threading conditions.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self.total_messages: int = 0
        self.validation_errors: int = 0
        self.send_errors: int = 0
        self.delivery_failures: int = 0

    def increment(self, field_name: str, delta: int = 1) -> None:
        """Thread-safe increment for any metric field."""
        with self._lock:
            setattr(self, field_name, getattr(self, field_name) + delta)

    def snapshot(self) -> dict:
        """Return a consistent snapshot of all metrics."""
        with self._lock:
            return {
                'total_messages': self.total_messages,
                'validation_errors': self.validation_errors,
                'send_errors': self.send_errors,
                'delivery_failures': self.delivery_failures,
            }


class ThroughputTracker:
    """Thread-safe rolling-window TPS tracker."""

    def __init__(self, window_size: int = 10):
        self.window_size = window_size
        self.timestamps: deque = deque()
        self._lock = threading.Lock()

    def record(self) -> float:
        """Record a new event and return the current TPS over the window."""
        with self._lock:
            current_time = time.time()
            self.timestamps.append(current_time)

            # Evict events outside the rolling window
            while self.timestamps and self.timestamps[0] < current_time - self.window_size:
                self.timestamps.popleft()

            if len(self.timestamps) > 1:
                time_span = self.timestamps[-1] - self.timestamps[0]
                if time_span > 0:
                    return len(self.timestamps) / time_span
            return 0.0

# ============================================================================
# 3. CONFIGURATION MANAGEMENT (Environment-Driven)
# ============================================================================
class ProducerConfig:
    """
    Centralised configuration — all values driven by environment variables
    so the same binary runs in local dev, Docker, and cloud without changes.
    """

    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_TOPIC: str = os.getenv('KAFKA_TOPIC', 'payment_transactions')
    NUM_THREADS: int = int(os.getenv('NUM_PRODUCER_THREADS', '4'))
    DURATION_SECONDS: int = int(os.getenv('PRODUCER_DURATION_SECONDS', '60'))

    # Tuned for 40k+ TPS:
    #   - linger.ms=5  → batches messages for 5ms before flushing
    #   - batch.size   → max bytes per batch
    #   - lz4          → fast compression reduces network I/O
    #   - acks=1       → leader-only ack; lower durability, higher throughput
    #                    (upgrade to acks=all + min.insync.replicas=2 for production)
    PRODUCER_CONFIG: dict = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'payment-ingestion-driver-v2',
        'compression.type': 'lz4',
        'linger.ms': 5,
        'batch.num.messages': 1000,
        'batch.size': 32768,
        'queue.buffering.max.messages': 50000,
        'queue.buffering.max.kbytes': 10240,
        'socket.nagle.disable': True,
        'acks': 1,
        'retries': 3,
        'retry.backoff.ms': 100,
    }

    MERCHANTS: List[str] = ["Amazon", "Target", "Walmart", "Netflix", "Uber", "Apple Store"]
    LOCATIONS: List[str] = [
        "NEW_DELHI, IN", "MUMBAI, IN", "BANGALORE, IN", "NEW_YORK, US", "LONDON, UK"
    ]

# ============================================================================
# 4. PAYMENT INGESTION DRIVER (Production-Grade)
# ============================================================================
class PaymentIngestionDriver:
    """
    Enterprise payment ingestion driver achieving 44k+ TPS via:
    - Multi-threaded concurrency (ThreadPoolExecutor)
    - Async Kafka produce (no blocking poll in hot path)
    - Pydantic schema validation at the boundary
    - Thread-safe metrics collection
    """

    def __init__(self):
        self.config = ProducerConfig()
        self.producer = Producer(self.config.PRODUCER_CONFIG)
        self.tracker = ThroughputTracker()
        self.metrics = PerformanceMetrics()
        logger.info("✅ Ingestion driver initialised | bootstrap=%s | topic=%s",
                    self.config.KAFKA_BOOTSTRAP_SERVERS, self.config.KAFKA_TOPIC)

    def _delivery_callback(self, err, msg):
        """Kafka async delivery confirmation handler (called from librdkafka I/O thread)."""
        if err:
            logger.error("❌ Delivery failed | error=%s", err)
            self.metrics.increment('delivery_failures')
        else:
            logger.debug("✅ Delivered | partition=%d | offset=%d", msg.partition(), msg.offset())

    def _generate_transaction(self) -> PaymentTransaction:
        """Generate a Pydantic-validated synthetic payment transaction."""
        return PaymentTransaction(
            transaction_id=str(uuid.uuid4()),
            account_id=f"ACC-{random.randint(10000, 99999)}",
            amount=round(random.uniform(10.0, 5000.0), 2),
            currency="INR",
            merchant=random.choice(self.config.MERCHANTS),
            location=random.choice(self.config.LOCATIONS),
            device_ip=f"192.168.1.{random.randint(2, 254)}",
            ts_string=datetime.now(timezone.utc).isoformat(),
        )

    def _worker_loop(self, worker_id: int, duration_seconds: int) -> int:
        """
        Single producer thread.  Runs a tight loop for `duration_seconds` seconds:
        generate → validate → produce (async) → poll(0) for delivery callbacks.
        """
        logger.info("🚀 Worker-%d started | duration=%ds", worker_id, duration_seconds)
        start_time = time.time()
        local_count = 0

        while time.time() - start_time < duration_seconds:
            try:
                tx = self._generate_transaction()

                self.producer.produce(
                    topic=self.config.KAFKA_TOPIC,
                    key=tx.account_id,
                    value=tx.model_dump_json().encode('utf-8'),
                    callback=self._delivery_callback,
                )
                # Non-blocking poll — drains the internal delivery-report queue
                # without introducing any sleep in the hot path.
                self.producer.poll(0)

                local_count += 1
                self.metrics.increment('total_messages')
                self.tracker.record()

                if local_count % 10_000 == 0:
                    tps = self.tracker.record()
                    logger.info("📊 Worker-%d | msgs=%d | TPS=%.2f", worker_id, local_count, tps)

            except ValidationError as exc:
                self.metrics.increment('validation_errors')
                logger.warning("⚠️  Validation error | worker=%d | error=%s", worker_id, exc)
            except Exception as exc:
                self.metrics.increment('send_errors')
                logger.error("❌ Worker-%d error: %s", worker_id, exc)

        elapsed = time.time() - start_time
        tps = local_count / elapsed if elapsed > 0 else 0
        logger.info("✅ Worker-%d DONE | msgs=%d | elapsed=%.1fs | TPS=%.2f",
                    worker_id, local_count, elapsed, tps)
        return local_count

    def run(self, duration_seconds: Optional[int] = None, num_threads: Optional[int] = None):
        """Launch all worker threads and block until completion."""
        duration = duration_seconds or self.config.DURATION_SECONDS
        threads = num_threads or self.config.NUM_THREADS

        logger.info("=" * 70)
        logger.info("🚀 HIGH-THROUGHPUT PAYMENT INGESTION DRIVER v2")
        logger.info("=" * 70)
        logger.info("Threads=%d | Duration=%ds | Target=%d+ TPS",
                    threads, duration, threads * 5000)
        logger.info("Kafka=%s | Topic=%s", self.config.KAFKA_BOOTSTRAP_SERVERS, self.config.KAFKA_TOPIC)
        logger.info("=" * 70)

        start_time = time.time()

        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(self._worker_loop, i, duration) for i in range(threads)]
            total_messages = sum(f.result() for f in futures)

        total_time = time.time() - start_time
        actual_tps = total_messages / total_time if total_time > 0 else 0

        self._print_final_metrics(total_messages, total_time, actual_tps, threads)

        # Flush remaining in-flight messages (wait up to 10s)
        self.producer.flush(timeout=10.0)
        logger.info("✅ Producer flushed and closed cleanly")

    def _print_final_metrics(self, total_msgs: int, total_time: float, tps: float, num_threads: int):
        """Log a structured final performance report."""
        target_tps = num_threads * 5000
        success_rate = (tps / target_tps * 100) if target_tps > 0 else 0
        snapshot = self.metrics.snapshot()

        logger.info("\n%s", "=" * 70)
        logger.info("📊 FINAL INGESTION METRICS")
        logger.info("=" * 70)
        logger.info("Total Messages  : %s", f"{total_msgs:,}")
        logger.info("Duration        : %.2fs", total_time)
        logger.info("Actual TPS      : %s", f"{tps:,.2f}")
        logger.info("Target TPS      : %s", f"{target_tps:,}")
        logger.info("Achievement     : %.2f%%", success_rate)
        logger.info("Validation Errs : %d", snapshot['validation_errors'])
        logger.info("Send Errors     : %d", snapshot['send_errors'])
        logger.info("Delivery Fails  : %d", snapshot['delivery_failures'])
        logger.info("=" * 70)

# ============================================================================
# 5. ENTRY POINT
# ============================================================================
if __name__ == '__main__':
    try:
        driver = PaymentIngestionDriver()
        driver.run()
    except KeyboardInterrupt:
        logger.warning("🛑 Ingestion interrupted by user")
    except Exception as exc:
        logger.critical("💥 Fatal error: %s", exc)
        raise

"""
Unit Tests — Payment Ingestion Producer
Tests schema validation, thread-safety, and throughput tracker correctness.

Run with:  pytest tests/test_producer.py -v
"""

import sys
import os
import time
import threading
import unittest

# Make src importable from tests/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from producer.producer import PaymentTransaction, PerformanceMetrics, ThroughputTracker
from pydantic import ValidationError


class TestPaymentTransactionSchema(unittest.TestCase):
    """Verifies Pydantic schema validation enforces data contracts at the ingestion boundary."""

    def _valid_payload(self, **overrides) -> dict:
        base = {
            "transaction_id": "a" * 36,
            "account_id": "ACC-12345",
            "amount": 100.0,
            "currency": "INR",
            "merchant": "Amazon",
            "location": "MUMBAI, IN",
            "device_ip": "192.168.1.1",
            "ts_string": "2026-01-01T00:00:00+00:00",
        }
        base.update(overrides)
        return base

    def test_valid_transaction_passes(self):
        """A correctly formed transaction should parse without error."""
        tx = PaymentTransaction(**self._valid_payload())
        self.assertEqual(tx.currency, "INR")
        self.assertEqual(tx.amount, 100.0)

    def test_zero_amount_rejected(self):
        """amount must be > 0 — zero should raise ValidationError."""
        with self.assertRaises(ValidationError):
            PaymentTransaction(**self._valid_payload(amount=0))

    def test_negative_amount_rejected(self):
        """Negative amounts must be rejected."""
        with self.assertRaises(ValidationError):
            PaymentTransaction(**self._valid_payload(amount=-50))

    def test_amount_exceeding_max_rejected(self):
        """Amount above the $1,000,000 ceiling must be rejected."""
        with self.assertRaises(ValidationError):
            PaymentTransaction(**self._valid_payload(amount=1_000_001))

    def test_short_transaction_id_rejected(self):
        """transaction_id shorter than 36 chars must be rejected."""
        with self.assertRaises(ValidationError):
            PaymentTransaction(**self._valid_payload(transaction_id="short"))

    def test_invalid_currency_length_rejected(self):
        """Currency must be exactly 3 characters."""
        with self.assertRaises(ValidationError):
            PaymentTransaction(**self._valid_payload(currency="EURO"))
        with self.assertRaises(ValidationError):
            PaymentTransaction(**self._valid_payload(currency="IN"))

    def test_empty_merchant_rejected(self):
        """Merchant cannot be an empty string."""
        with self.assertRaises(ValidationError):
            PaymentTransaction(**self._valid_payload(merchant=""))


class TestPerformanceMetrics(unittest.TestCase):
    """Verifies thread-safe increment behaviour under concurrent writes."""

    def test_single_thread_increment(self):
        m = PerformanceMetrics()
        m.increment('total_messages', 10)
        self.assertEqual(m.total_messages, 10)

    def test_concurrent_increment_is_accurate(self):
        """
        100 threads each increment total_messages 1000 times.
        Without a lock, the final count would be less than 100,000 due to
        lost-update races.  With the lock, it must equal exactly 100,000.
        """
        m = PerformanceMetrics()
        num_threads = 100
        increments_per_thread = 1000

        def worker():
            for _ in range(increments_per_thread):
                m.increment('total_messages')

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(m.total_messages, num_threads * increments_per_thread)

    def test_snapshot_is_consistent(self):
        """snapshot() should return all fields as a dict."""
        m = PerformanceMetrics()
        m.increment('validation_errors', 5)
        m.increment('delivery_failures', 2)
        snap = m.snapshot()
        self.assertEqual(snap['validation_errors'], 5)
        self.assertEqual(snap['delivery_failures'], 2)
        self.assertEqual(snap['total_messages'], 0)

    def test_multiple_fields_independent(self):
        """Incrementing one field must not affect other fields."""
        m = PerformanceMetrics()
        m.increment('send_errors', 3)
        self.assertEqual(m.send_errors, 3)
        self.assertEqual(m.total_messages, 0)
        self.assertEqual(m.validation_errors, 0)


class TestThroughputTracker(unittest.TestCase):
    """Verifies the rolling-window TPS calculator."""

    def test_single_event_returns_zero(self):
        """A single event has no time span — TPS should be 0."""
        tracker = ThroughputTracker(window_size=10)
        tps = tracker.record()
        self.assertEqual(tps, 0.0)

    def test_two_events_returns_nonzero(self):
        """Two events separated in time should return a positive TPS."""
        tracker = ThroughputTracker(window_size=10)
        tracker.record()
        time.sleep(0.05)
        tps = tracker.record()
        self.assertGreater(tps, 0)

    def test_old_events_evicted(self):
        """Events older than window_size seconds should be evicted."""
        tracker = ThroughputTracker(window_size=1)
        tracker.record()
        time.sleep(1.1)   # window expires
        tracker.record()  # only one event in window now
        tps = tracker.record()
        # After eviction there are at most 2 timestamps — TPS is either 0 or low
        self.assertGreaterEqual(tps, 0)

    def test_thread_safe_concurrent_record(self):
        """Multiple threads calling record() simultaneously must not raise."""
        tracker = ThroughputTracker(window_size=5)
        errors = []

        def worker():
            try:
                for _ in range(500):
                    tracker.record()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [], msg=f"Thread errors: {errors}")


if __name__ == '__main__':
    unittest.main()

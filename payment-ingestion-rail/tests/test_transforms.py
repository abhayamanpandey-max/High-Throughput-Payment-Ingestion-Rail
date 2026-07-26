"""
Unit Tests — Databricks SQL Transforms
Tests Silver deduplication logic and Gold aggregation correctness
using in-memory Python equivalents of the SQL transforms.

These tests validate the LOGIC without requiring a real Databricks connection.

Run with:  pytest tests/test_transforms.py -v
"""

import sys
import os
import unittest

# ---------------------------------------------------------------------------
# Lightweight pandas-based replications of the SQL transforms
# (mirrors what silver_transform.sql and gold_transform.sql do)
# ---------------------------------------------------------------------------
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


@unittest.skipUnless(HAS_PANDAS, "pandas not installed — skipping transform tests")
class TestSilverDeduplication(unittest.TestCase):
    """
    Tests the Silver layer MERGE logic:
    - Keeps only the latest version of each transaction_id
    - Drops rows with NULL account_id, NULL transaction_id, or amount <= 0
    """

    def _silver_transform(self, df: "pd.DataFrame") -> "pd.DataFrame":
        """Python reimplementation of silver_transform.sql deduplication logic."""
        # Drop invalid rows (mirrors WHERE clause in SQL)
        clean = df[
            (df["amount"] > 0)
            & df["account_id"].notna()
            & df["transaction_id"].notna()
        ].copy()

        # Deduplicate: keep latest row per transaction_id (mirrors ROW_NUMBER + ORDER BY ingested_at DESC)
        clean = (
            clean
            .sort_values("ingested_at", ascending=False)
            .drop_duplicates(subset=["transaction_id"], keep="first")
            .reset_index(drop=True)
        )
        return clean

    def _make_df(self, rows: list) -> "pd.DataFrame":
        import pandas as pd
        from datetime import datetime, timedelta
        base_time = datetime(2026, 1, 1, 12, 0, 0)
        records = []
        for i, row in enumerate(rows):
            records.append({
                "transaction_id": row.get("transaction_id", f"TX-{i:04d}"),
                "account_id": row.get("account_id", f"ACC-{i:05d}"),
                "amount": row.get("amount", 100.0),
                "is_anomaly": row.get("is_anomaly", False),
                "ingested_at": base_time + timedelta(seconds=i),
            })
        return pd.DataFrame(records)

    def test_deduplication_keeps_latest(self):
        """Duplicate transaction_id → only the most recent row survives."""
        df = self._make_df([
            {"transaction_id": "TX-DUPE", "amount": 100.0},
            {"transaction_id": "TX-DUPE", "amount": 200.0},   # newer, higher ingested_at
        ])
        result = self._silver_transform(df)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["amount"], 200.0)

    def test_zero_amount_filtered(self):
        """Rows with amount = 0 must be dropped."""
        df = self._make_df([
            {"transaction_id": "TX-ZERO", "amount": 0},
            {"transaction_id": "TX-OK",   "amount": 50.0},
        ])
        result = self._silver_transform(df)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["transaction_id"], "TX-OK")

    def test_null_account_id_filtered(self):
        """Rows with NULL account_id must be dropped."""
        import pandas as pd
        df = self._make_df([
            {"transaction_id": "TX-NULL", "account_id": None, "amount": 100.0},
            {"transaction_id": "TX-GOOD", "account_id": "ACC-12345", "amount": 100.0},
        ])
        df.loc[df["transaction_id"] == "TX-NULL", "account_id"] = None
        result = self._silver_transform(df)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["transaction_id"], "TX-GOOD")

    def test_unique_transactions_all_kept(self):
        """N unique transactions → N rows in Silver."""
        n = 10
        df = self._make_df([{"transaction_id": f"TX-{i:04d}", "amount": float(i + 1)} for i in range(n)])
        result = self._silver_transform(df)
        self.assertEqual(len(result), n)


@unittest.skipUnless(HAS_PANDAS, "pandas not installed — skipping transform tests")
class TestGoldAggregation(unittest.TestCase):
    """
    Tests the Gold layer aggregation logic:
    - 5-minute windows per account
    - Anomaly flag triggers on count > 10 OR total > 10,000 OR any tx flagged
    """

    def _gold_transform(self, df: "pd.DataFrame") -> "pd.DataFrame":
        """Python reimplementation of gold_layer.py aggregation logic."""
        import pandas as pd
        df = df.copy()
        df["ts"] = pd.to_datetime(df["ts"])
        df["window_start"] = df["ts"].dt.floor("5min")
        df["window_end"] = df["window_start"] + pd.Timedelta(minutes=5)

        agg = (
            df.groupby(["window_start", "window_end", "account_id"])
            .agg(
                tx_count_5m=("transaction_id", "count"),
                total_spent_5m=("amount", "sum"),
                avg_amount_5m=("amount", "mean"),
                max_amount_5m=("amount", "max"),
                has_anomaly=("is_anomaly", "max"),
            )
            .reset_index()
        )

        agg["is_anomaly"] = (
            (agg["tx_count_5m"] > 10)
            | (agg["total_spent_5m"] > 10_000)
            | (agg["has_anomaly"] == True)
        )
        return agg

    def _make_df(self, rows: list) -> "pd.DataFrame":
        import pandas as pd
        return pd.DataFrame(rows)

    def test_basic_aggregation(self):
        """3 transactions in the same window → 1 Gold row."""
        from datetime import datetime
        df = self._make_df([
            {"transaction_id": "TX-1", "account_id": "ACC-001", "amount": 100.0, "is_anomaly": False, "ts": "2026-01-01 12:00:00"},
            {"transaction_id": "TX-2", "account_id": "ACC-001", "amount": 200.0, "is_anomaly": False, "ts": "2026-01-01 12:01:00"},
            {"transaction_id": "TX-3", "account_id": "ACC-001", "amount": 300.0, "is_anomaly": False, "ts": "2026-01-01 12:04:00"},
        ])
        result = self._gold_transform(df)
        self.assertEqual(len(result), 1)
        self.assertAlmostEqual(result.iloc[0]["total_spent_5m"], 600.0)
        self.assertEqual(result.iloc[0]["tx_count_5m"], 3)
        self.assertFalse(result.iloc[0]["is_anomaly"])

    def test_high_transaction_count_triggers_anomaly(self):
        """More than 10 transactions in a 5-minute window → is_anomaly = True."""
        rows = [
            {"transaction_id": f"TX-{i}", "account_id": "ACC-001",
             "amount": 50.0, "is_anomaly": False,
             "ts": f"2026-01-01 12:0{i % 5}:00"}
            for i in range(11)
        ]
        result = self._gold_transform(self._make_df(rows))
        self.assertTrue(result["is_anomaly"].any())

    def test_high_total_triggers_anomaly(self):
        """Total > 10,000 in a window → is_anomaly = True."""
        df = self._make_df([
            {"transaction_id": "TX-BIG", "account_id": "ACC-001",
             "amount": 11_000.0, "is_anomaly": False, "ts": "2026-01-01 12:00:00"},
        ])
        result = self._gold_transform(df)
        self.assertTrue(result.iloc[0]["is_anomaly"])

    def test_flagged_tx_propagates_to_gold(self):
        """A single flagged transaction in the window → window is_anomaly = True."""
        df = self._make_df([
            {"transaction_id": "TX-OK",  "account_id": "ACC-001", "amount": 50.0,  "is_anomaly": False, "ts": "2026-01-01 12:00:00"},
            {"transaction_id": "TX-BAD", "account_id": "ACC-001", "amount": 100.0, "is_anomaly": True,  "ts": "2026-01-01 12:01:00"},
        ])
        result = self._gold_transform(df)
        self.assertTrue(result.iloc[0]["is_anomaly"])

    def test_different_accounts_get_separate_rows(self):
        """Two accounts in the same window → two separate Gold rows."""
        df = self._make_df([
            {"transaction_id": "TX-A", "account_id": "ACC-001", "amount": 100.0, "is_anomaly": False, "ts": "2026-01-01 12:00:00"},
            {"transaction_id": "TX-B", "account_id": "ACC-002", "amount": 200.0, "is_anomaly": False, "ts": "2026-01-01 12:01:00"},
        ])
        result = self._gold_transform(df)
        self.assertEqual(len(result), 2)
        accounts = set(result["account_id"].tolist())
        self.assertIn("ACC-001", accounts)
        self.assertIn("ACC-002", accounts)


if __name__ == '__main__':
    unittest.main()

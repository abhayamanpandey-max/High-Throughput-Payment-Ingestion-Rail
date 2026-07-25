CREATE SCHEMA IF NOT EXISTS payment_pipeline.silver;

CREATE TABLE IF NOT EXISTS payment_pipeline.silver.payments_cleaned (
    transaction_id   STRING,
    account_id       STRING,
    amount           DOUBLE,
    merchant         STRING,
    location         STRING,
    ts               TIMESTAMP,
    is_anomaly       BOOLEAN,
    anomaly_score    DOUBLE,
    processed_at     TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;

-- Incremental merge: dedupe by transaction_id, drop invalid rows
MERGE INTO payment_pipeline.silver.payments_cleaned AS target
USING (
    SELECT transaction_id, account_id, amount, merchant, location, ts, is_anomaly, anomaly_score
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY ingested_at DESC) AS rn
        FROM payment_pipeline.bronze.payments
        WHERE amount > 0 AND account_id IS NOT NULL AND transaction_id IS NOT NULL
    )
    WHERE rn = 1
) AS source
ON target.transaction_id = source.transaction_id
WHEN NOT MATCHED THEN INSERT *;

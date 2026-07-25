CREATE SCHEMA IF NOT EXISTS payment_pipeline.gold;

CREATE TABLE IF NOT EXISTS payment_pipeline.gold.anomaly_metrics (
    window_start       TIMESTAMP,
    window_end         TIMESTAMP,
    account_id         STRING,
    transaction_count  BIGINT,
    total_amount       DOUBLE,
    avg_amount         DOUBLE,
    anomaly_count      BIGINT,
    max_anomaly_score  DOUBLE,
    computed_at        TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;

INSERT INTO payment_pipeline.gold.anomaly_metrics
SELECT
    date_trunc('minute', ts) AS window_start,
    date_trunc('minute', ts) + INTERVAL 1 MINUTE AS window_end,
    account_id,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomaly_count,
    MAX(anomaly_score) AS max_anomaly_score,
    current_timestamp() AS computed_at
FROM payment_pipeline.silver.payments_cleaned
WHERE ts >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY date_trunc('minute', ts), account_id;

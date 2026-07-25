# 💳 High-Throughput Real-Time Payment Ingestion & Anomaly Mitigation Rail

[![Tech Stack](https://shields.io)](#tech-stack)
[![License: MIT](https://shields.io)](https://opensource.org)


## Architecture Overview

![Payment Pipeline Architecture](./docs/payment_pipeline_architecture.png)

### Data Flow
- **Ingestion:** 5,000 TPS via Docker multi-threaded driver
- **Event Bus:** Apache Kafka distributes events
- **Processing:** PyFlink with sub-200ms latency
- **Storage:** Databricks Delta Lake (Medallion: Bronze → Silver → Gold)
- **Orchestration:** Apache Airflow manages pipeline lifecycle
- **Analytics:** Streamlit dashboards for real-time monitoring

-----
### Tech Stack: Docker, Apache Kafka, PyFlink, Databricks (Delta Lake), Apache Airflow

----

### Resume Bullets
**• Engineered a Docker multi-container environment orchestrating Apache Kafka brokers and PyFlink cluster nodes with isolated network tiers, ensuring seamless environment parity between local development and production execution.**

**• Load-tested and benchmarked a multi-threaded Python ingestion driver generating up to 5,000 TPS into distributed Kafka topics, enforcing strict schema validation and data contracts to eliminate downstream data drift at the ingestion boundary.**

**• Built a distributed PyFlink streaming pipeline using stateful processing and sliding time-windows to compute real-time anomaly metrics at sub-200ms latency, writing results into a 3-tier Medallion architecture (Bronze/Silver/Gold) on Databricks via Delta Lake Sink with ACID guarantees.**

**• Authored Apache Airflow DAGs to automate data lifecycle scheduling, backpressure recovery routines, and cluster health alerts, reducing manual intervention for pipeline fault recovery.**

**• Engineered a real-time monitoring dashboard tracking Kafka consumer lag, Flink checkpoint status, and end-to-end throughput, enabling instant detection of performance degradation across the pipeline.**

----

### Core Architecture: 🏗️ Complete System Architecture (Mapped to Resume Bullets)

**Mapped to Resume Bullets**

- BULLET 1: Docker Multi-Container Environment & Isolated Network Tiers
- BULLET 2: Multi-Threaded Ingestion Driver (5,000 TPS)
    - ↳ Enforces Schema Validation & Data Contracts
- BULLET 2: Apache Kafka Brokers (Event Streaming Bus)
- BULLET 3: Distributed PyFlink Streaming Engine (Sub-200ms Latency)
    - ↳ Stateful Processing, 10-Min Sliding Windows
- BULLET 3: Databricks / Delta Lake Lakehouse Storage (ACID Sinks)
    - ↳ Bronze Layer: Raw JSON payloads from Kafka Stream
    - ↳ Silver Layer: Cleaned, validated transactions
    - ↳ Gold Layer: Aggregated real-time anomaly metrics
- BULLET 4: Apache Airflow DAG Scheduler
    - ↳ Handles data lifecycle, backpressure, & cluster health
- BULLET 5: Real-Time Streamlit Monitoring Dashboard
    - ↳ Tracks Consumer Lag, Flink Checkpoints, & Throughput

**Step-by-Step Pipeline**

- `[INGESTION TIER]` ──► Multi-threaded Python Driver Transaction Generator (5,000 TPS)
- `[EVENT STREAM BUS]` ──► Apache Kafka Broker (KRaft Mode, partitioned by account_id)
- `[PROCESSING TIER]` ──► Stateful PyFlink Streaming App (Sliding Windows, RocksDB State)
- `[IN-MEMORY CACHE]` ──► Databricks Delta Lake (Medallion: Bronze → Silver → Gold) (Stores live transaction metrics & risk flags)
- `[FRONTEND DASHBOARD]` ──► Streamlit Real-Time Dashboard (Refreshes dynamically via Redis)

----  
### Engineering Impact: Load-tested and benchmarked ingestion at up to 5,000 TPS with sub-200ms processing latency and isolated network tiers.

------
--

## 📊 Medallion Data Schema Design

### 1. Bronze Layer (Raw Event Storage)
Stores raw, immutable JSON strings alongside critical platform auditing fields.
* **`kafka_offset`** (BIGINT): Message track identifier.
* **`ingest_timestamp`** (TIMESTAMP): Log insertion baseline clock.
* **`source_system`** (STRING): Origin route path identifier.
* **`raw_payload`** (JSON/VARIANT): Unparsed payload.

### 2. Silver Layer (Cleaned & Flattened)
Enforces precise schema contracts, flattens payloads, and handles casting.
* **`transaction_id`** (STRING/UUID): Primary structural transaction key.
* **`account_id`** (STRING): Masked individual client routing reference.
* **`amount`** (DECIMAL): Exact financial itemized transaction amount.
* **`currency`** (STRING): ISO-standardized 3-letter currency code (e.g., `USD`).
* **`event_timestamp`** (TIMESTAMP): Original source device logging clock.

### 3. Gold Layer (Business Analytics & Metrics)
Rolling aggregations powering fraud alerting systems.
* **`window_start` / `window_end`** (TIMESTAMP): 5-minute rolling tracking frame blocks.
* **`tx_count_5m`** (INT): Volume metrics over the current temporal frame.
* **`total_spent_5m`** (DECIMAL): Volumetric monetary aggregation totals.
* **`is_anomaly`** (BOOLEAN): Flag triggering when transaction velocity exceeds 10 occurrences per window.

---

## 🛠️ Codebase Structure

```text
├── dags/
│   └── payment_pipeline_orchestration.py   
├── src/
│   ├── notebooks/
│   │   ├── 01_Bronze.py                    
│   │   ├── 02_Silver.py                    
│   │   └── 03_Gold.py                      
│   ├── drivers/
│   │   └── ingestion_driver.py             
│   └── app/
│       └── streamlit_app.py                <-- Added
├── docs/
│   └── architecture.png                    
├── docker-compose.yml                      <-- Added
└── README.md

```

---

## 🚀 Execution & Deployment Guide

### Prerequisites
* Docker & Docker Compose
* Active Databricks Workspace (with Unity Catalog enabled)
* Apache Airflow 2.x Environment

### Quickstart Setup
1. **Clone the Repository**:
   ```bash
   git clone https://github.com
   cd payment-streaming-pipeline
   ```
2. **Spin Up Ingestion Infra (Kafka & Drivers)**:
   ```bash
   docker-compose up -d
   ```
3. **Deploy Airflow Workflows**:
   Copy `dags/payment_pipeline_orchestration.py` to your local Airflow deployment directory to begin tracking multi-hop Medallion routines.

----
   
## Local vs Cloud Execution

### Development (Local Delta Lake)
```bash
# Quick iteration without cloud costs
python src/databricks/delta_lakehouse.py  # Tests locally against data/bronze|silver|gold
```

### Production (Databricks Cloud)
```bash
# Full end-to-end: Flink → Kafka → Databricks
python src/databricks/databricks_loader.py  # Consumes from Kafka, writes to Databricks
# Then: Run silver_transform.sql and gold_transform.sql in Databricks SQL Editor
```

### Architecture
- **Bronze:** Raw payment events (Kafka → Databricks)
- **Silver:** Deduplicated, cleaned (SQL merge pattern)
- **Gold:** Minute-windowed anomaly metrics (aggregations)

All three tiers support both local Delta files (for testing) and Databricks (production).
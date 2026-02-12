# Crypto Portfolio & Rewards Analytics — Lambda Architecture

A production-grade data platform that unifies **real-time** and **batch** processing to deliver crypto portfolio performance analytics and rewards computation. Built with **Hadoop, Spark, Flink, Airflow, and Kafka** — demonstrating a complete Lambda Architecture for financial data at scale.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Layer Deep-Dive](#layer-deep-dive)
  - [Ingestion Layer](#ingestion-layer)
  - [Speed Layer (Real-Time)](#speed-layer-real-time)
  - [Batch Layer (Offline)](#batch-layer-offline)
  - [Serving Layer](#serving-layer)
  - [Orchestration](#orchestration)
- [Data Models](#data-models)
- [Setup & Installation](#setup--installation)
- [Running the Pipeline](#running-the-pipeline)
- [Key Engineering Decisions](#key-engineering-decisions)
- [Performance & Optimizations](#performance--optimizations)
- [Future Enhancements](#future-enhancements)
- [Author](#author)

---

## Overview

### Problem Statement

Crypto platforms need to deliver two things simultaneously:

1. **Real-time views** — live portfolio P&L, instant reward calculations as transactions happen, and streaming price feeds.
2. **Batch analytics** — historical portfolio performance, end-of-day reconciliation, reward tier re-computation, and deep analytical queries.

A single processing paradigm cannot serve both needs well. Batch systems have latency; streaming systems struggle with complex historical reprocessing. The **Lambda Architecture** solves this by running both in parallel and merging their outputs at query time.

### What This Project Does

- Ingests live crypto market data and simulated user transactions (trades, cashback, staking rewards).
- **Speed Layer (Flink):** Computes real-time 1-min/5-min OHLCV aggregations, live portfolio P&L, and streaming reward tier assignments.
- **Batch Layer (Spark on Hadoop):** Runs daily jobs to build a star-schema data warehouse with historical portfolio snapshots, reward summaries, and reconciled transaction records.
- **Serving Layer:** Merges batch and real-time views via a query API, powering a unified analytics dashboard.
- **Orchestration (Airflow):** Schedules and monitors all batch pipelines with data quality checks and SLA alerting.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                  │
│  ┌──────────────────────┐     ┌──────────────────────────────────┐      │
│  │ Crypto Exchange APIs  │     │ Simulated User Transactions      │      │
│  │ (Binance WebSocket)   │     │ (Trades, Rewards, Staking)       │      │
│  └──────────┬───────────┘     └──────────────┬───────────────────┘      │
└─────────────┼────────────────────────────────┼──────────────────────────┘
              │                                │
              ▼                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                                  │
│                    Apache Kafka (3 topics)                               │
│         ┌────────────┬─────────────────┬──────────────┐                 │
│         │ raw.prices │ raw.transactions│ raw.rewards   │                 │
│         └─────┬──────┴────────┬────────┴──────┬───────┘                 │
└───────────────┼───────────────┼───────────────┼─────────────────────────┘
                │               │               │
        ┌───────┴───────┐ ┌────┴────┐   ┌──────┴──────┐
        ▼               ▼ ▼         ▼   ▼             ▼
┌──────────────┐  ┌──────────────────────────────────────────┐
│ SPEED LAYER  │  │             BATCH LAYER                   │
│              │  │                                            │
│ Apache Flink │  │  HDFS (Raw) → Spark ETL → Hive (Star     │
│ (Scala)      │  │  Schema)                                   │
│              │  │                                            │
│ • 1m/5m OHLCV│  │  • Daily portfolio snapshots               │
│ • Live P&L   │  │  • Historical reward summaries             │
│ • Reward tier│  │  • Reconciled transactions                 │
│   streaming  │  │  • Asset correlation matrices              │
│              │  │                                            │
│     ┌──┐     │  │              ┌──┐                          │
│     │  ▼     │  │              │  ▼                          │
│  ┌──────┐   │  │  ┌─────────────────────┐                   │
│  │Redis │   │  │  │  Hive / Parquet     │                   │
│  │(RT)  │   │  │  │  (Batch Serving)    │                   │
│  └──┬───┘   │  │  └─────────┬───────────┘                   │
└─────┼───────┘  └────────────┼───────────────────────────────┘
      │                       │
      ▼                       ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         SERVING LAYER                                   │
│                                                                         │
│  Query API (FastAPI) ──► Merge batch + real-time ──► Grafana Dashboard  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘

            ┌─────────────────────────────────────────┐
            │           ORCHESTRATION                  │
            │  Apache Airflow                          │
            │  • DAGs for daily Spark batch jobs       │
            │  • Data quality validation sensors       │
            │  • SLA monitoring & Slack alerting       │
            └─────────────────────────────────────────┘
```

See `docs/architecture.mermaid` for the interactive diagram.

---

## Tech Stack

| Component       | Technology            | Language     | Purpose                                    |
|----------------|-----------------------|--------------|---------------------------------------------|
| Message Broker  | Apache Kafka 3.x      | —            | Event streaming & decoupling                |
| Stream Engine   | Apache Flink 1.18     | Scala 2.12   | Real-time aggregations & reward computation |
| Batch Engine    | Apache Spark 3.5      | Scala / PySpark | Offline ETL, star schema construction   |
| Storage         | Hadoop HDFS 3.x       | —            | Distributed raw data lake                   |
| Data Warehouse  | Apache Hive 3.x       | SQL          | Batch serving layer (Parquet-backed)        |
| Cache           | Redis 7.x             | —            | Real-time serving store                     |
| Orchestration   | Apache Airflow 2.8    | Python       | Pipeline scheduling, monitoring, alerting   |
| API             | FastAPI               | Python       | Query serving layer                         |
| Dashboard       | Grafana / Superset    | —            | Visualization                               |
| Containers      | Docker + Docker Compose | —          | Local development environment               |

---

## Project Structure

```
crypto-lambda-analytics/
│
├── docker/                          # Docker configs for all services
│   ├── docker-compose.yml
│   ├── flink/
│   ├── spark/
│   ├── hadoop/
│   ├── kafka/
│   ├── airflow/
│   └── redis/
│
├── ingestion/                       # Kafka producers
│   ├── price_producer.py            # Binance WebSocket → Kafka
│   ├── transaction_simulator.py     # Simulated trades & rewards
│   └── schemas/                     # Avro/JSON schemas
│       ├── price_event.avsc
│       ├── transaction_event.avsc
│       └── reward_event.avsc
│
├── speed-layer/                     # Flink streaming jobs (Scala)
│   ├── build.sbt
│   └── src/main/scala/
│       ├── CryptoStreamingJob.scala         # Entry point
│       ├── functions/
│       │   ├── OHLCVWindowFunction.scala    # 1m/5m candlestick aggregation
│       │   ├── PortfolioPnLFunction.scala   # Live P&L per user
│       │   └── RewardTierFunction.scala     # Streaming reward tier assignment
│       ├── models/
│       │   ├── PriceEvent.scala
│       │   ├── TransactionEvent.scala
│       │   └── PortfolioState.scala
│       └── sinks/
│           └── RedisSink.scala              # Write to Redis serving store
│
├── batch-layer/                     # Spark batch jobs
│   ├── build.sbt                    # (if Scala)
│   └── src/
│       ├── daily_portfolio_snapshot.py      # PySpark: daily portfolio valuation
│       ├── reward_summary.py                # PySpark: daily reward aggregation
│       ├── transaction_reconciliation.py    # PySpark: reconcile RT vs batch
│       └── schemas/
│           └── star_schema.sql              # Hive DDL for fact/dim tables
│
├── serving-layer/                   # Query API
│   ├── app.py                       # FastAPI application
│   ├── merge.py                     # Batch + RT view merger logic
│   └── requirements.txt
│
├── orchestration/                   # Airflow DAGs
│   ├── dags/
│   │   ├── daily_batch_pipeline.py          # Main daily DAG
│   │   └── data_quality_checks.py           # DQ validation DAG
│   └── plugins/
│       └── slack_alert.py                   # SLA failure notifications
│
├── tests/
│   ├── test_flink_functions.py
│   ├── test_spark_jobs.py
│   └── test_serving_api.py
│
├── docs/
│   ├── architecture.mermaid
│   ├── data_models.md
│   └── runbook.md
│
├── scripts/
│   ├── setup.sh                     # One-command local setup
│   └── seed_data.sh                 # Generate sample data
│
├── .gitignore
├── Makefile                         # make build, make test, make up
└── README.md
```

---

## Layer Deep-Dive

### Ingestion Layer

**Kafka Producer** (`ingestion/price_producer.py`)

Connects to the Binance WebSocket API and publishes real-time price ticks to the `raw.prices` Kafka topic. A separate simulator generates realistic user transaction events (buys, sells, staking deposits, cashback rewards) into `raw.transactions` and `raw.rewards`.

Key design choices:
- **Avro schemas** for all events with a Schema Registry — enforces data contracts between producers and consumers.
- **Kafka partitioning** by `asset_symbol` for prices and `user_id` for transactions — ensures ordered processing per entity.
- **Exactly-once semantics** enabled via Kafka idempotent producers.

```python
# Example: price_producer.py (simplified)
async def stream_prices():
    async with websockets.connect(BINANCE_WS_URL) as ws:
        async for message in ws:
            price_event = parse_binance_tick(message)
            producer.produce(
                topic="raw.prices",
                key=price_event.symbol,
                value=serialize_avro(price_event),
            )
```

---

### Speed Layer (Real-Time)

**Apache Flink** — written in **Scala** for type safety and performance in the JVM streaming ecosystem.

Three core streaming jobs:

**1. OHLCV Window Aggregation**
Consumes `raw.prices`, applies tumbling windows (1-min, 5-min) to produce candlestick data per asset. Uses Flink's event-time processing with watermarks to handle late-arriving data.

```scala
// OHLCVWindowFunction.scala (simplified)
priceStream
  .keyBy(_.symbol)
  .window(TumblingEventTimeWindows.of(Time.minutes(1)))
  .allowedLateness(Time.seconds(30))
  .aggregate(new OHLCVAggregator(), new OHLCVWindowFunction())
  .addSink(new RedisSink("ohlcv:1m"))
```

**2. Live Portfolio P&L**
Maintains per-user portfolio state using Flink's keyed state. On every price update, recalculates unrealized P&L for all holdings of that asset across users.

**3. Reward Tier Assignment**
Consumes transactions and applies business rules to assign reward tiers in real-time (e.g., Bronze/Silver/Gold/Platinum based on 30-day rolling spend). Uses Flink's `ProcessFunction` with timers for window expiration.

All results are sunk into **Redis** with TTLs appropriate to each use case.

---

### Batch Layer (Offline)

**Apache Spark on Hadoop HDFS** — a mix of PySpark and Spark SQL.

Daily batch pipeline (orchestrated by Airflow):

| Step | Job | Input | Output |
|------|-----|-------|--------|
| 1 | Raw Ingestion | Kafka → HDFS | Parquet files in `raw/` zone |
| 2 | Portfolio Snapshot | `raw/prices` + `raw/transactions` | `fact_portfolio_daily` |
| 3 | Reward Summary | `raw/rewards` + `raw/transactions` | `fact_reward_daily` |
| 4 | Reconciliation | Speed layer Redis dumps + batch results | `fact_reconciliation` |

**Star Schema Design** (stored in Hive, backed by Parquet on HDFS):

```
              ┌──────────────────┐
              │  dim_user        │
              │  user_id (PK)    │
              │  tier             │
              │  signup_date      │
              │  kyc_status       │
              └────────┬─────────┘
                       │
┌──────────────┐       │       ┌──────────────────────┐
│ dim_asset    │       │       │ dim_date             │
│ asset_id(PK) ├───┐   │   ┌───┤ date_id (PK)         │
│ symbol       │   │   │   │   │ date, month, quarter │
│ name         │   │   │   │   └──────────────────────┘
│ category     │   │   │   │
└──────────────┘   │   │   │
                   ▼   ▼   ▼
              ┌──────────────────────────────────┐
              │  fact_portfolio_daily             │
              │  snapshot_id (PK)                 │
              │  user_id (FK) → dim_user          │
              │  asset_id (FK) → dim_asset        │
              │  date_id (FK) → dim_date          │
              │  quantity_held                    │
              │  avg_cost_basis                   │
              │  market_value                     │
              │  unrealized_pnl                   │
              │  realized_pnl                     │
              └──────────────────────────────────┘

              ┌──────────────────────────────────┐
              │  fact_reward_daily                │
              │  reward_id (PK)                   │
              │  user_id (FK) → dim_user          │
              │  date_id (FK) → dim_date          │
              │  reward_type (cashback/staking)   │
              │  amount_earned                    │
              │  tier_at_time                     │
              │  rolling_30d_spend                │
              └──────────────────────────────────┘
```

**Spark optimizations demonstrated:**
- Broadcast joins for small dimension tables
- Partition pruning on date columns
- Adaptive query execution (AQE) for skew handling
- Delta-style incremental processing (process only new partitions)

---

### Serving Layer

**FastAPI** application that serves as the unified query interface:

```python
@app.get("/portfolio/{user_id}")
async def get_portfolio(user_id: str):
    # 1. Get latest batch snapshot from Hive/Parquet
    batch_view = query_hive(f"""
        SELECT * FROM fact_portfolio_daily
        WHERE user_id = '{user_id}' AND date_id = '{yesterday()}'
    """)
    
    # 2. Get real-time delta from Redis
    rt_delta = redis.hgetall(f"portfolio:pnl:{user_id}")
    
    # 3. Merge: batch provides the base, RT provides the delta
    merged = merge_views(batch_view, rt_delta)
    return merged
```

The merge logic follows the Lambda principle: the batch view is the "source of truth" baseline, and the real-time view provides incremental updates since the last batch run.

---

### Orchestration

**Apache Airflow** manages the entire batch pipeline lifecycle:

```python
# daily_batch_pipeline.py (simplified DAG)
with DAG("daily_crypto_batch", schedule="0 2 * * *", catchup=False) as dag:
    
    check_raw_data = HdfsSensor(
        task_id="wait_for_raw_data",
        filepath="/data/raw/prices/dt={{ ds }}",
    )
    
    build_portfolio = SparkSubmitOperator(
        task_id="daily_portfolio_snapshot",
        application="batch-layer/src/daily_portfolio_snapshot.py",
        conf={"spark.sql.adaptive.enabled": "true"},
    )
    
    build_rewards = SparkSubmitOperator(
        task_id="reward_summary",
        application="batch-layer/src/reward_summary.py",
    )
    
    data_quality = PythonOperator(
        task_id="run_dq_checks",
        python_callable=run_data_quality_checks,
    )
    
    reconcile = SparkSubmitOperator(
        task_id="reconciliation",
        application="batch-layer/src/transaction_reconciliation.py",
    )
    
    check_raw_data >> [build_portfolio, build_rewards] >> data_quality >> reconcile
```

---

## Setup & Installation

### Prerequisites

- Docker & Docker Compose v2+
- Java 11+
- Scala 2.12 (for Flink jobs)
- Python 3.10+
- sbt (Scala build tool)

### Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/<your-username>/crypto-lambda-analytics.git
cd crypto-lambda-analytics

# 2. Start all infrastructure services
make up

# 3. Build Flink streaming jobs
cd speed-layer && sbt assembly && cd ..

# 4. Seed sample data
make seed

# 5. Start the streaming pipeline
make stream

# 6. Trigger a batch run
make batch

# 7. Open the dashboard
open http://localhost:3000    # Grafana
open http://localhost:8080    # Airflow UI
```

### Service Ports

| Service        | URL                        |
|---------------|----------------------------|
| Kafka UI       | http://localhost:9021       |
| Flink Dashboard| http://localhost:8081       |
| Airflow        | http://localhost:8080       |
| Spark History  | http://localhost:18080      |
| HDFS NameNode  | http://localhost:9870       |
| Grafana        | http://localhost:3000       |
| Serving API    | http://localhost:8000/docs  |
| Redis          | localhost:6379              |

---

## Key Engineering Decisions

| Decision | Rationale |
|----------|-----------|
| **Lambda over Kappa** | The project intentionally uses Lambda to demonstrate both batch and streaming proficiency. In production, a Kappa architecture (Flink-only) might suffice, but Lambda better showcases the full Hadoop/Spark/Flink stack. |
| **Flink in Scala** | Demonstrates JVM-native streaming. Flink's Scala API provides stronger type safety for stateful processing. Also signals versatility beyond Python-only workflows. |
| **Spark batch in PySpark** | Shows Python proficiency alongside Scala. PySpark is more common for batch ETL in practice. |
| **Redis for RT serving** | Sub-millisecond reads for real-time views. TTLs naturally expire stale data. |
| **Parquet + Hive for batch serving** | Columnar storage with predicate pushdown. Hive provides SQL interface for ad-hoc queries. |
| **Avro for Kafka schemas** | Schema evolution support critical for production data pipelines. |
| **Star schema** | Optimized for analytical queries. Familiar pattern for data warehouse consumers. |

---

## Performance & Optimizations

### Flink (Speed Layer)
- **RocksDB state backend** for large keyed state (portfolio positions across millions of users)
- **Incremental checkpointing** to minimize checkpoint duration
- **Watermark strategies** tuned per source (bounded out-of-orderness for exchange data)
- **Async I/O** for Redis sink to avoid backpressure

### Spark (Batch Layer)
- **Adaptive Query Execution (AQE)** for dynamic partition coalescing and skew join handling
- **Broadcast joins** for dimension tables under 100MB
- **Partition pruning** on date columns — queries only scan relevant partitions
- **Z-ordering** on frequently filtered columns (user_id, asset_id)
- **Incremental processing** — each run only processes new HDFS partitions since the last successful run

### Hadoop (Storage)
- **Parquet with Snappy compression** — 60-70% storage reduction vs raw JSON
- **Partition layout**: `/data/warehouse/fact_portfolio_daily/dt=YYYY-MM-DD/`
- **Small file compaction** job to merge small Kafka-produced files

---

## Future Enhancements

- **Migrate to Kappa Architecture** — use Flink for both batch and streaming with Flink SQL
- **Add CDC ingestion** — use Debezium for database change capture
- **ML integration** — anomaly detection on transaction patterns using Spark MLlib
- **Data lineage** — integrate Apache Atlas or OpenLineage for end-to-end tracking
- **Cost attribution** — tag compute resources per pipeline for chargeback reporting
- **Governance** — add column-level access control via Apache Ranger

---

## Author

**[Your Name]**

Data Engineer | [LinkedIn](https://linkedin.com/in/yourprofile) | [GitHub](https://github.com/yourusername)

Built as a portfolio project demonstrating production-grade data platform engineering with a focus on real-time and offline processing for crypto/fintech use cases.

---

*This project is for educational and portfolio purposes. It uses publicly available market data and simulated transactions. No real financial data is processed.*

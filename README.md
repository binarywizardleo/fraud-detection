# FinGuard Fraud Detection Platform

A real-time fraud detection and alerting platform built on Databricks Lakeflow using Spark Declarative Pipelines (SDP). The platform ingests transaction streams and fraud watchlist data, cleans and conforms them through a medallion architecture, generates business-level fraud and high-value alerts, and delivers real-time email notifications to affected customers.

---

## Repository Structure

```
fraud-detection/
├── README.md                          ← This file
├── ARCHITECTURE.md                    ← Architecture, design & trade-off documentation
├── .gitignore
│
├── customer-master-data-pl/           ← Customer Master Data Pipeline (separate pipeline)
│   └── customer-master-data-root/
│       └── silver/
│           └── customers_silver.py    ← Customer dimension (single source of truth)
│
├── fraude-detection-pl/               ← Fraud Detection Pipeline (main pipeline)
│   ├── fraud-detection-root/          ← Pipeline source code root (glob-included)
│   │   ├── bronze/                    ← Raw ingestion layer
│   │   │   ├── transactions_bronze.py
│   │   │   └── fraud_watchlist_bronze.py
│   │   ├── silver/                    ← Cleansed & conformed layer
│   │   │   ├── transactions_silver.py
│   │   │   └── fraud_watchlist_silver.py
│   │   ├── gold/                      ← Business-level aggregations & alerts
│   │   │   ├── fraud_card_alerts_gold.py
│   │   │   ├── high_value_transaction_alerts_gold.py
│   │   │   ├── customer_risk_profile.py
│   │   │   ├── fraud_trends_by_category.py
│   │   │   └── daily_transaction_summary.py
│   │   └── alerts/                    ← Email notification sinks
│   │       ├── fraud_card_alert_email.py
│   │       └── high_value_transaction_alert_email.py
│   └── explorations/                  ← Ad-hoc analysis notebooks (not in pipeline)
│
└── _archived_old_files/               ← Archived legacy scripts (not in pipeline)
```

---

## Pipelines

### 1. Customer Master Data Pipeline (`customer-master-data-pl`)

| Property | Value |
| --- | --- |
| Purpose | Maintains the customer dimension as a single source of truth |
| Owner | Data Platform / Customer Data Team |
| Consumers | Fraud detection, marketing, analytics, customer 360 |
| Source | External operational database (via Lakeflow Connect) |
| Target Table | `fraud_detection.silver.customers` |

This pipeline produces `fraud_detection.silver.customers` — the authoritative customer dimension table consumed by all downstream pipelines. It applies strict data quality rules (drop on null customer_id or invalid age) and standardizes fields (uppercase country codes, trimmed strings, parsed dates).

### 2. Fraud Detection Pipeline (`fraude-detection-pl`)

| Property | Value |
| --- | --- |
| Purpose | Real-time fraud detection, alerting, and reporting |
| Owner | Fraud Detection Team |
| Pipeline Type | Serverless, Photon-enabled, WORKSPACE |
| Catalog | `workspace` |
| Schema | `default` |
| Channel | CURRENT |
| Publishing Mode | DEFAULT (Unity Catalog) |

This is the main pipeline. It ingests streaming transaction data from Kafka and fraud watchlist files from cloud storage, processes them through bronze → silver → gold layers, generates real-time alerts, sends email notifications, and produces reporting aggregates.

---

## Data Flow

```
  Kafka Topic          Cloud Files (Volume)
  (transactions)       (fraud watchlist JSON)
       │                      │
       ▼                      ▼
  ┌─────────────────────────────────────────┐
  │              BRONZE LAYER               │
  │  transactions_bronze    fraud_watchlist │
  │  (Streaming Table)      _bronze (ST)    │
  └────────────┬──────────────┬─────────────┘
               │              │
               ▼              ▼
  ┌─────────────────────────────────────────┐
  │              SILVER LAYER               │
  │  transactions_silver    fraud_watchlist │
  │  (Streaming Table)      _silver (ST)    │
  └────────────┬──────────────┬─────────────┘
               │              │
               │    ┌─────────┘
               │    │
               ▼    ▼
  ┌─────────────────────────────────────────┐    ┌──────────────────────┐
  │               GOLD LAYER                │    │  customer-master-data │
  │                                         │◄───┤  fraud_detection.     │
  │  fraud_card_alerts (ST)                 │    │  silver.customers     │
  │  high_value_transaction_alerts (ST)     │    │  (batch dimension)    │
  │  customer_risk_profile (MV)             │    └──────────────────────┘
  │  fraud_trends_by_category (MV)         │
  │  daily_transaction_summary (MV)         │
  └────────────────────┬────────────────────┘
                       │
                       ▼
  ┌─────────────────────────────────────────┐
  │            ALERTS LAYER                  │
  │  fraud_card_alert_email (ForEachBatch)   │
  │  high_value_alert_email (ForEachBatch)   │
  │  → Gmail SMTP email delivery            │
  └─────────────────────────────────────────┘
```

---

## Layer-by-Layer Description

### Bronze Layer (Raw Ingestion)

Minimally transforms source data, preserving raw payload with ingestion metadata.

| Dataset | Type | Source | Target Table |
| --- | --- | --- | --- |
| `transactions_bronze` | Streaming Table | Kafka (SASL_SSL) | `fraud_detection.bronze.transactions` |
| `fraud_watchlist_bronze` | Streaming Table | Auto Loader (cloud JSON files) | `fraud_detection.bronze.fraud_watchlist` |

**Key characteristics:**
* Kafka stream: casts binary key/value to string, preserves partition/offset/topic/timestamp, adds `ingestion_timestamp`
* Auto Loader: schema inference with `rescue` mode for schema evolution, preserves file path and modification time metadata
* Rate-limited: `maxOffsetsPerTrigger=100` for controlled ingestion

### Silver Layer (Cleansed & Conformed)

Parses, validates, and standardizes bronze data with data quality expectations.

| Dataset | Type | Source | Target Table |
| --- | --- | --- | --- |
| `transactions_silver` | Streaming Table | `bronze.transactions` | `fraud_detection.silver.transactions` |
| `fraud_watchlist_silver` | Streaming Table | `bronze.fraud_watchlist` | `fraud_detection.silver.fraud_watchlist` |

**Key characteristics:**
* Transactions: parses JSON payload via `from_json()` with explicit `StructType` schema, enforces 5 data quality expectations (3 drop, 2 warn)
* Fraud watchlist: trims all strings, uppercases action/country, converts timestamps, preserves file metadata for lineage
* Both are streaming tables that maintain incremental processing semantics

### Gold Layer (Business Alerts & Reporting)

Two types of gold datasets: streaming tables for real-time alert detection and materialized views for reporting.

#### Streaming Tables (Real-Time Alerts)

| Dataset | Type | Source | Target Table |
| --- | --- | --- | --- |
| `fraud_card_alerts_gold` | Streaming Table | silver.transactions + silver.fraud_watchlist + silver.customers | `fraud_detection.gold.fraud_card_alerts` |
| `high_value_transaction_alerts_gold` | Streaming Table | silver.transactions + silver.customers | `fraud_detection.gold.high_value_transaction_alerts` |

* **fraud_card_alerts**: Stream-stream join (10-min watermarks) between transactions and fraud watchlist on `card_number == entity_id`, enriched with customer data. Produces per-transaction fraud alerts with watchlist context (risk level, reason, action).
* **high_value_transaction_alerts**: Stream-static join between transactions and customers. Filters where `amount >= transaction_limit`. Generates unique `alert_id` via MD5 hash. Includes full transaction + customer context.

#### Materialized Views (Reporting Aggregates)

| Dataset | Type | Source | Target Table |
| --- | --- | --- | --- |
| `customer_risk_profile` | Materialized View | silver.customers + silver.transactions + gold alerts | `fraud_detection.gold.customer_risk_profile` |
| `fraud_trends_by_category` | Materialized View | gold.fraud_card_alerts | `fraud_detection.gold.fraud_trends_by_category` |
| `daily_transaction_summary` | Materialized View | silver.transactions + gold alerts | `fraud_detection.gold.daily_transaction_summary` |

* **customer_risk_profile**: Per-customer comprehensive profile with demographics, transaction behavior, alert history, and computed risk tier (HIGH/MEDIUM/LOW). Clustered by `customer_id`.
* **fraud_trends_by_category**: Daily fraud trends by merchant category with risk level breakdowns (high/medium/low counts), affected customers, and amount-over-limit metrics. Partitioned by `alert_date`.
* **daily_transaction_summary**: Daily transaction summary with volume, channel breakdown (online/POS/mobile/ATM), international vs domestic split, status breakdown, enriched with fraud and high-value alert counts. Partitioned by `transaction_date`.

### Alerts Layer (Email Notifications)

| Dataset | Type | Source | Sink |
| --- | --- | --- | --- |
| `fraud_card_alert_email` | ForEachBatch Sink | gold.fraud_card_alerts | Gmail SMTP |
| `high_value_alert_email` | ForEachBatch Sink | gold.high_value_transaction_alerts | Gmail SMTP |

* Uses `@dp.foreach_batch_sink` to process each micro-batch and send HTML email alerts
* Uses `@dp.append_flow` to stream gold alert tables into the email sinks
* Credentials retrieved via `dbutils.secrets.get(scope="fraud-detection", key="gmail_api_key")`
* Sender: `binary.wizard.leo@gmail.com`, recipient: customer email from gold tables

---

## Databricks Secrets

The pipeline requires the following secrets in the `fraud-detection` Databricks secret scope:

| Secret Key | Used By | Purpose |
| --- | --- | --- |
| `bootstrap_server` | transactions_bronze | Kafka bootstrap server address |
| `topic_name` | transactions_bronze | Kafka topic name for transactions |
| `api_key` | transactions_bronze | Kafka SASL API key |
| `api_secret` | transactions_bronze | Kafka SASL API secret |
| `gmail_api_key` | alerts (both) | Gmail SMTP app password |

---

## Unity Catalog Tables

All tables are published to the `fraud_detection` catalog:

| Schema | Table | Type |
| --- | --- | --- |
| `bronze` | `transactions` | Streaming Table |
| `bronze` | `fraud_watchlist` | Streaming Table |
| `silver` | `transactions` | Streaming Table |
| `silver` | `fraud_watchlist` | Streaming Table |
| `silver` | `customers` | Streaming Table (from customer-master-data-pl) |
| `gold` | `fraud_card_alerts` | Streaming Table |
| `gold` | `high_value_transaction_alerts` | Streaming Table |
| `gold` | `customer_risk_profile` | Materialized View |
| `gold` | `fraud_trends_by_category` | Materialized View |
| `gold` | `daily_transaction_summary` | Materialized View |

---

## Source Data Locations

| Source | Location | Format |
| --- | --- | --- |
| Kafka transactions | Configured via secrets | Streaming (JSON payload) |
| Fraud watchlist files | `/Volumes/fraud_detection/source/fraud_watchlist/fraud/` | JSON |
| Watchlist schema location | `/Volumes/fraud_detection/source/schema/fraud_watchlist/` | Auto Loader schema |

---

## Getting Started

### Prerequisites

* Databricks workspace with Unity Catalog enabled
* Serverless compute enabled
* `fraud_detection` catalog with `bronze`, `silver`, and `gold` schemas
* Databricks secret scope named `fraud-detection` with all required secrets
* Kafka cluster accessible from Databricks with SASL_SSL
* Fraud watchlist JSON files in the specified Volume path

### Running the Pipelines

1. **Customer Master Data Pipeline** — Run `customer-master-data-pl` first to materialize `fraud_detection.silver.customers`
2. **Fraud Detection Pipeline** — Run `fraude-detection-pl`. Use the pipeline editor or trigger an update via the Databricks API

Both pipelines use serverless compute with Photon acceleration. The fraud detection pipeline is configured for triggered (non-continuous) execution.

### Pipeline Configuration

The fraud detection pipeline uses a **glob library pattern**:

```
/Workspace/Users/binary.wizard.leo@gmail.com/fraud-detection/fraude-detection-pl/fraud-detection-root/**
```

All Python files under `fraud-detection-root/` are automatically included as pipeline source code. To add a new dataset, place a `.py` file in the appropriate layer folder.

---

## Technologies

* **Databricks Lakeflow** — Pipeline orchestration and execution
* **Spark Declarative Pipelines (SDP)** — Declarative ETL framework (formerly DLT)
* **Unity Catalog** — Data governance and table management
* **Serverless Compute** — Auto-scaling, auto-managed compute
* **Photon** — Databricks vectorized query engine
* **Kafka** — Streaming transaction source
* **Auto Loader** — Incremental file ingestion from cloud storage
* **Python** — All pipeline code is written in Python using `pyspark.pipelines` API
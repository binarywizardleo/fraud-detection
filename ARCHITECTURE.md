# Architecture, Design & Trade-Offs

## Fraud Detection Platform

This document describes the architecture, design decisions, trade-offs, and rationale behind the Fraud Detection Platform. It is intended for data engineers, architects, and technical stakeholders who need to understand not just *what* the system does, but *why* it was designed this way.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Multi-Pipeline Segregation](#2-multi-pipeline-segregation)
3. [Medallion Layer Architecture](#3-medallion-layer-architecture)
4. [Streaming vs Batch Design Choices](#4-streaming-vs-batch-design-choices)
5. [Separation of Concerns](#5-separation-of-concerns)
6. [Ownership & Operational Separation](#6-ownership--operational-separation)
7. [Downstream Dependencies](#7-downstream-dependencies)
8. [Data Quality Strategy](#8-data-quality-strategy)
9. [Security & Secrets Management](#9-security--secrets-management)
10. [Trade-Offs: Pros & Cons](#10-trade-offs-pros--cons)
11. [Future Considerations](#11-future-considerations)

---

## 1. System Overview

The FinGuard Fraud Detection Platform is a real-time fraud detection system built on Databricks Lakeflow Spark Declarative Pipelines (SDP). It processes financial transaction streams against fraud watchlists, generates two categories of alerts (fraud card and high-value transaction), delivers email notifications to affected customers, and produces reporting aggregates for dashboards.

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     EXTERNAL SOURCES                            │
│  Kafka (transactions)    Cloud Files (watchlist)    Postgres    │
└────────┬──────────────────────┬─────────────────────┬──────────┘
         │                      │                     │
         ▼                      ▼                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                   PIPELINE 1: customer-master-data-pl           │
│                   bronze.customers → silver.customers           │
│                   (Customer dimension - single source of truth) │
└────────────────────────────────┬────────────────────────────────┘
                                 │ (cross-pipeline dependency)
         ┌───────────────────────┘
         │
┌─────────────────────────────────────────────────────────────────┐
│                   PIPELINE 2: fraude-detection-pl               │
│                                                                 │
│  BRONZE                    SILVER                   GOLD          │
│  transactions_bronze       transactions_silver      fraud_card_alerts (ST)
│  fraud_watchlist_bronze    fraud_watchlist_silver   high_value_alerts (ST)
│                                                     customer_risk_profile (MV)
│                                                     fraud_trends_by_category (MV)
│                                                     daily_transaction_summary (MV)
│                                                                 │
│  ALERTS LAYER                                                   │
│  fraud_card_alert_email (ForEachBatch → Gmail SMTP)             │
│  high_value_alert_email (ForEachBatch → Gmail SMTP)             │
└─────────────────────────────────────────────────────────────────┘
```

### Key Design Principles

* **Declarative over imperative**: All transformations are expressed declaratively via SDP decorators — the framework manages checkpointing, retries, and orchestration
* **Medallion architecture**: Strict bronze → silver → gold layering with clear responsibilities per layer
* **Pipeline segregation**: Customer master data is owned by a separate pipeline, not coupled to the fraud detection domain
* **Streaming-first**: Bronze and silver layers use streaming tables for low-latency ingestion; gold layer uses both streaming tables (alerts) and materialized views (reporting)
* **Serverless compute**: All pipelines run on serverless with Photon for auto-scaling and cost optimization

---

## 2. Multi-Pipeline Segregation

### Why Multiple Pipelines?

The platform is split into two independent Lakeflow SDP pipelines:

| Pipeline | Purpose | Owner | Tables Produced |
| --- | --- | --- | --- |
| `customer-master-data-pl` | Customer dimension (master data) | Data Platform / Customer Data Team | `fraud_detection.silver.customers` |
| `fraude-detection-pl` | Fraud detection, alerting, reporting | Fraud Detection Team | Bronze/Silver/Gold tables for transactions, watchlist, alerts, and reports |

### Rationale

**Customer data is master data, not domain-specific data.** It serves fraud detection, marketing, analytics, customer 360, and other domain pipelines. Embedding customer transformations inside the fraud detection pipeline would create several problems:

1. **Coupling**: Every schema change to customer data would require deploying the fraud detection pipeline, even if the fraud logic is unchanged. This increases blast radius and deployment risk.

2. **Ownership ambiguity**: If customer data lives inside the fraud pipeline, who owns its quality? The fraud team or the customer data team? Separate pipelines enforce clear ownership boundaries.

3. **Reusability**: Other pipelines (marketing, analytics) need the same conformed customer dimension. A dedicated pipeline produces a single, canonical version that all consumers reference, eliminating the "which customer table is correct?" problem.

4. **Independent SLAs**: Customer data may have different refresh cadences and quality requirements than fraud detection. A separate pipeline allows each team to set their own schedules.

5. **Failure isolation**: If the customer data pipeline fails, the fraud detection pipeline can continue running against the last successfully materialized customer snapshot. The stream-static join pattern (batch read of `silver.customers`) naturally supports this — the fraud pipeline reads the latest committed state, not a live dependency.

### Cross-Pipeline Dependency

The fraud detection pipeline depends on `fraud_detection.silver.customers` produced by `customer-master-data-pl`. This is a **table-level dependency**, not a code-level dependency. The fraud pipeline reads the published Unity Catalog table via `spark.read.table()` — there is no import or direct code reference between the two pipelines.

```
Pipeline 1:  customer-master-data-pl  →  fraud_detection.silver.customers  (Unity Catalog table)
                                                        │
                                                        │ (table-level read, no code coupling)
                                                        ▼
Pipeline 2:  fraude-detection-pl  reads  fraud_detection.silver.customers  via spark.read.table()
```

This means:
* Pipeline 2 can be developed, tested, and deployed independently of Pipeline 1
* Pipeline 2 reads the latest committed state of the customers table at refresh time
* Schema changes to the customers table are a **contract change** requiring cross-team coordination (documented in the `customers_silver.py` schema contract docstring)

---

## 3. Medallion Layer Architecture

### Why Multiple Layers?

The medallion architecture (bronze → silver → gold) is a progressive refinement pattern where each layer has a single, well-defined responsibility:

```
RAW → CLEANSED → BUSINESS
(what arrived) → (what's valid) → (what's actionable)
```

### Bronze Layer — "Capture Everything, Transform Nothing"

**Purpose**: Ingest raw source data with minimal transformation. Preserve the original payload for reprocessing and auditing.

**Design decisions:**
* Kafka messages are stored as-is (key, value, topic, partition, offset, timestamp) with only binary-to-string casting and an `ingestion_timestamp` added
* Fraud watchlist files are ingested via Auto Loader with schema inference and `rescue` mode — unexpected fields are captured in a rescued data column, not dropped
* No filtering, no validation, no business logic — bronze is the historical record

**Why this matters**: If a silver or gold transformation has a bug, you can always re-derive from bronze without re-reading from the source system. Bronze is immutable history.

### Silver Layer — "One Clean, Conformed Version of Each Entity"

**Purpose**: Parse, validate, and standardize bronze data into a conformed, queryable form. Apply data quality rules.

**Design decisions:**
* Transactions: JSON payload is parsed via `from_json()` with an explicit `StructType` schema (not schema inference) — this provides type safety and early detection of schema drift
* Fraud watchlist: all strings trimmed, country/action uppercased, timestamps converted to proper `timestamp` type
* Data quality expectations enforce required fields (drop on null) and flag suspicious values (warn)
* File metadata (source file path, modification time) is preserved for lineage tracking

**Why this matters**: Silver is the contract. Every gold dataset and every downstream consumer trusts that silver data is valid, typed, and conformed. This eliminates the need for every consumer to re-validate or re-parse raw data.

### Gold Layer — "Business Logic, Alerts, and Reporting"

**Purpose**: Apply domain-specific business logic, generate alerts, and produce reporting aggregates.

**Design decisions:**
* Alert detection (fraud_card_alerts, high_value_transaction_alerts) uses **streaming tables** for real-time, incremental processing — each new transaction is evaluated against watchlists and limits as it arrives
* Reporting aggregates (customer_risk_profile, fraud_trends_by_category, daily_transaction_summary) use **materialized views** with batch reads — they recompute aggregations over the full dataset on each refresh, which is correct for analytics but not latency-sensitive
* Stream-stream joins in fraud_card_alerts use 10-minute watermarks to bound state and prevent unbounded memory growth
* High-value alerts generate a deterministic `alert_id` via MD5(transaction_id + transaction_timestamp) for idempotent tracking

**Why this matters**: Gold is where business value is created. By separating alert detection (streaming, real-time) from reporting (batch, recomputed), each concern is served by the appropriate processing model with the right latency, cost, and consistency trade-offs.

### Layer Summary Table

| Layer | Responsibility | Data Quality | Processing Model | Re-processing |
| --- | --- | --- | --- | --- |
| Bronze | Raw capture | None | Streaming (append-only) | Full refresh from source |
| Silver | Cleanse & conform | Strict (drop/warn) | Streaming (incremental) | Re-derive from bronze |
| Gold (alerts) | Real-time alert detection | Business rules | Streaming (incremental) | Re-derive from silver |
| Gold (reports) | Aggregated analytics | None (trusted input) | Batch (MV recompute) | Recompute from silver/gold |

---

## 4. Streaming vs Batch Design Choices

### Why Streaming Tables for Bronze/Silver/Alerts?

Transactions arrive continuously via Kafka. Fraud detection must evaluate each transaction in near-real-time against the watchlist. Streaming tables (`@dp.table` with `spark.readStream`) provide:

* **Incremental processing**: Only new data is processed per trigger, not the full historical dataset
* **Low latency**: Alerts are generated as transactions arrive, not on a batch schedule
* **Automatic checkpointing**: SDP manages offsets and checkpoints — no manual state management
* **Backpressure handling**: `maxOffsetsPerTrigger=100` prevents overwhelming downstream stages

### Why Materialized Views for Reporting?

Reporting aggregates (customer risk profiles, daily summaries, fraud trends) need to be **correct over the full dataset**, not just incremental changes. A customer's risk tier depends on their entire transaction history and all alerts ever generated. Materialized views (`@dp.materialized_view` with `spark.read`) provide:

* **Full recomputation**: Aggregations are computed over the complete dataset, ensuring correctness even with late-arriving data
* **Incremental refresh on serverless**: On serverless pipelines, materialized views support automatic incremental refresh — only changed rows are processed when possible, falling back to full recompute when needed
* **Batch reads from streaming tables**: Materialized views read streaming tables via `spark.read.table()` (batch), which is the correct pattern for gold-layer aggregation from streaming sources

### Stream-Stream vs Stream-Static Joins

| Join Type | Where Used | Why |
| --- | --- | --- |
| Stream-stream (with watermark) | fraud_card_alerts: transactions ✕ fraud_watchlist | Both sources are streaming (Kafka + Auto Loader). Watermarks (10 min) bound state for the join window |
| Stream-static | high_value_transaction_alerts: transactions ✕ customers | Transactions are streaming; customers is a batch dimension table (from another pipeline). Static dimension is read once per trigger |
| Batch-batch | All materialized views | Full dataset aggregation for reporting — no streaming semantics needed |

### Watermark Strategy

The fraud_card_alerts gold table uses 10-minute watermarks on both the transaction stream (`transaction_timestamp`) and the watchlist stream (`effective_from`). This means:

* **State is bounded**: The join state holds at most 10 minutes of data, preventing memory exhaustion
* **Late data tolerance**: Transactions arriving within 10 minutes of the latest seen timestamp will still match
* **Trade-off**: Data arriving more than 10 minutes late may be dropped from the join. For a fraud detection system, this is an acceptable trade-off — a 10-minute-old fraud alert is far less actionable than a real-time one

---

## 5. Separation of Concerns

### By Layer

Each medallion layer has exactly one concern:

* **Bronze**: Ingestion mechanics (Kafka, Auto Loader) — no business logic
* **Silver**: Data quality and conformance — no business rules, no joins across domains
* **Gold**: Business logic and analytics — no ingestion concerns, no raw parsing
* **Alerts**: Notification delivery — no business logic, just consumes gold alert tables and sends emails

### By File

Each dataset is defined in exactly one file, named after the dataset. This makes it trivial to find the code for any table:

* Want to understand how `fraud_detection.gold.fraud_card_alerts` works? → `gold/fraud_card_alerts_gold.py`
* Want to understand how `fraud_detection.bronze.transactions` is ingested? → `bronze/transactions_bronze.py`

### By Pipeline

* **customer-master-data-pl**: Owns customer data quality and conformance — no fraud logic
* **fraude-detection-pl**: Owns fraud detection logic — no customer data management

### Alerts vs Gold Tables

The alert detection logic (what constitutes a fraud alert) lives in the gold layer. The email notification logic (how to send an alert) lives in the alerts layer. This separation means:
* The alert detection logic can change without touching email code
* The email delivery mechanism can change (e.g., switch from SMTP to a notification API) without touching detection logic
* New notification channels (SMS, push, Slack) can be added by creating new alert files that read the same gold tables

---

## 6. Ownership & Operational Separation

### Team Ownership

| Pipeline | Owner | Responsibilities |
| --- | --- | --- |
| `customer-master-data-pl` | Data Platform / Customer Data Team | Customer data quality, schema contracts, refresh cadence |
| `fraude-detection-pl` | Fraud Detection Team | Alert detection rules, notification delivery, reporting aggregates |

### Operational Independence

* **Independent deployment**: Each pipeline can be updated, deployed, and rolled back independently
* **Independent scheduling**: Each pipeline has its own refresh cadence (triggered or scheduled)
* **Independent compute**: Each pipeline runs on its own serverless compute cluster — no resource contention
* **Independent monitoring**: Pipeline failures, data quality metrics, and run history are tracked per pipeline
* **Shared tables**: The dependency is through Unity Catalog tables (not code), so each team manages their own pipeline code repository

### Schema Contract

The `fraud_detection.silver.customers` table is a **published contract** between the Customer Data Team (producer) and the Fraud Detection Team (consumer). The `customers_silver.py` docstring explicitly documents:

* This table provides a stable, versioned schema contract for all consumers
* Breaking schema changes require cross-team coordination and migration planning
* All consumers can trust that `customer_id` and `age` are valid (strict drop enforcement)

---

## 7. Downstream Dependencies

### Dependency Graph

```
customer-master-data-pl
  └── fraud_detection.silver.customers  (CONTRACT)
      └── consumed by fraude-detection-pl (batch read)

fraude-detection-pl
  ├── fraud_detection.bronze.transactions
  │   └── fraud_detection.silver.transactions
  │       ├── fraud_detection.gold.fraud_card_alerts (stream-stream join + customers)
  │       │   ├── fraud_detection.gold.fraud_trends_by_category (MV aggregate)
  │       │   └── alerts/fraud_card_alert_email (ForEachBatch sink)
  │       └── fraud_detection.gold.high_value_transaction_alerts (stream-static join + customers)
  │           ├── fraud_detection.gold.customer_risk_profile (MV, 3 joins)
  │           ├── fraud_detection.gold.daily_transaction_summary (MV, 2 joins)
  │           └── alerts/high_value_alert_email (ForEachBatch sink)

  └── fraud_detection.bronze.fraud_watchlist
      └── fraud_detection.silver.fraud_watchlist
          └── fraud_detection.gold.fraud_card_alerts (stream-stream join with transactions)
```

### Dependency Types

| Dependency | Type | Coupling Level |
| --- | --- | --- |
| bronze → silver (same pipeline) | Code-level (streaming read) | Tight — same pipeline, same deploy |
| silver → gold (same pipeline) | Code-level (streaming/batch read) | Tight — same pipeline, same deploy |
| gold → alerts (same pipeline) | Code-level (append_flow → sink) | Tight — same pipeline, same deploy |
| silver.customers → gold (cross-pipeline) | Table-level (batch read) | Loose — independent deploy, contract-bound |
| gold alerts → gold MVs (same pipeline) | Code-level (batch read) | Tight — same pipeline, same deploy |

### Impact of Failure

| Failure | Impact on Fraud Detection |
| --- | --- |
| `customer-master-data-pl` fails | Fraud pipeline reads last committed `silver.customers` snapshot — continues running with potentially stale customer data |
| Kafka unavailable | Bronze transactions stop ingesting — no new alerts generated, but existing data and MVs remain queryable |
| Fraud watchlist files not updated | No new watchlist entries — fraud alerts continue against existing watchlist |
| Email delivery fails | Alerts are still detected and stored in gold tables; only notification delivery is affected |
| Gold MV refresh fails | Alert detection continues (streaming tables); reporting dashboards show stale data until next refresh |

---

## 8. Data Quality Strategy

### Layered Quality Enforcement

| Layer | Strategy | Implementation |
| --- | --- | --- |
| Bronze | None | Raw data preserved as-is |
| Silver (transactions) | 3 drop + 2 warn | `@dp.expect_or_drop` for null IDs/timestamps; `@dp.expect` for amount > 0 and currency not null |
| Silver (watchlist) | 2 drop + 1 warn | `@dp.expect_or_drop` for null watchlist_id/entity_id; `@dp.expect` for effective_from |
| Silver (customers) | 2 drop + 2 warn | `@dp.expect_or_drop` for null customer_id and invalid age; `@dp.expect` for email and card_number |
| Gold | Business rules | Filter-based (amount >= transaction_limit for high-value; card_number == entity_id for fraud) |

### Why Drop in Silver but Warn for Soft Constraints?

* **Drop** is used for fields that make a record unusable downstream (e.g., a transaction without a `transaction_id` cannot be joined, tracked, or alerted on). These records would cause errors in gold-layer joins and aggregations.
* **Warn** is used for fields that are important but not fatal (e.g., a transaction with `amount <= 0` is suspicious but technically processable — it might be a reversal or a data error worth investigating but not dropping).
* **Customer data uses strict drop** because customer data quality issues cascade to ALL consumers (fraud, marketing, analytics). A bad customer record affects every downstream join.

---

## 9. Security & Secrets Management

All credentials are stored in Databricks secrets and referenced via `dbutils.secrets.get()`. No credentials are hardcoded in source files.

| Secret Scope | Keys | Used By |
| --- | --- | --- |
| `fraud-detection` | `bootstrap_server`, `topic_name`, `api_key`, `api_secret` | Bronze Kafka ingestion |
| `fraud-detection` | `gmail_api_key` | Alerts email sinks |

### Kafka Authentication

Kafka uses SASL_SSL with PLAIN mechanism. The JAAS configuration string is constructed dynamically using `kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule` to avoid classpath conflicts with Databricks runtime Kafka libraries.

### Email Credentials

Gmail SMTP credentials (app password) are retrieved outside the ForEachBatch handler function for serialization compatibility — if the pipeline runs on databricks-connect, the handler function must be serializable and must not reference `dbutils` directly.

---

## 10. Trade-Offs: Pros & Cons

### Trade-Off 1: Streaming Tables vs Materialized Views for Gold Layer

| Aspect | Streaming Tables (Alerts) | Materialized Views (Reports) |
| --- | --- | --- |
| **Latency** | Real-time (per micro-batch) | Refresh-dependent (minutes to hours) |
| **Correctness** | Append-only; late data may miss watermark window | Full recompute; always correct over complete dataset |
| **Cost** | Lower per-trigger (incremental) | Higher per-refresh (full scan) but infrequent |
| **State management** | Watermark-bounded state | No state — recomputed from scratch |
| **Use case** | Alerting (must be immediate) | Reporting (must be complete) |

**Decision**: Use streaming tables for alert detection (latency-critical) and materialized views for reporting (correctness-critical). This is the correct split — attempting to use streaming tables for aggregates would require complex stateful windowing, and attempting to use materialized views for alerts would add unnecessary latency.

### Trade-Off 2: Stream-Stream Join with Watermarks vs Full Scan

| Aspect | Stream-Stream Join (current) | Full Scan Batch Join |
| --- | --- | --- |
| **Latency** | Real-time per transaction | Batch-scheduled |
| **State** | Bounded by 10-min watermark | Unbounded (full scan) |
| **Late data** | May drop data > 10 min late | Captures all historical data |
| **Cost** | Lower per-trigger | Higher per-batch |
| **Complexity** | Higher (watermark tuning) | Lower (simple join) |

**Decision**: Stream-stream join with 10-minute watermarks. For fraud detection, real-time alerting is more valuable than catching very late data. If late data becomes a concern, the watermark window can be increased or a batch backfill process can be added.

### Trade-Off 3: Separate Customer Data Pipeline vs Embedded Customer Logic

| Aspect | Separate Pipeline | Embedded in Fraud Pipeline |
| --- | --- | --- |
| **Reusability** | High — multiple consumers | None — locked to fraud pipeline |
| **Ownership** | Clear — Customer Data Team owns it | Ambiguous — fraud team owns customer data |
| **Deployment independence** | Yes — independent deploy/update | No — coupled to fraud pipeline schedule |
| **Failure isolation** | Yes — fraud pipeline uses last snapshot | No — customer data failure breaks fraud pipeline |
| **Complexity** | Slightly higher (two pipelines to manage) | Lower (one pipeline) |
| **Schema contract** | Explicit — documented in code | Implicit — buried in pipeline logic |

**Decision**: Separate pipeline. The reusability and ownership benefits far outweigh the minor operational complexity of managing two pipelines. The customer dimension is shared master data.

### Trade-Off 4: Glob Library Pattern vs Explicit File Listing

| Aspect | Glob Pattern (`**`) | Explicit File List |
| --- | --- | --- |
| **New file onboarding** | Automatic — just drop a file in the folder | Manual — must update pipeline settings |
| **Accidental inclusion** | Possible — any `.py` file is picked up | Not possible — only listed files included |
| **Maintenance** | Low — no settings changes needed | Higher — settings must stay in sync |

**Decision**: Glob pattern. The convenience of dropping a new dataset file into the appropriate layer folder outweighs the risk of accidental inclusion. Empty files are harmless, and the directory structure (`bronze/`, `silver/`, `gold/`, `alerts/`) provides natural organization.

### Trade-Off 5: ForEachBatch Sink for Email vs External Notification System

| Aspect | ForEachBatch Sink (current) | External Notification API |
| --- | --- | --- |
| **Complexity** | Low — Python smtplib in pipeline | Higher — external service, API, retry logic |
| **Latency** | Direct — email sent per micro-batch | Indirect — through external queue/service |
| **Reliability** | Basic — try/catch per email, no retry queue | Higher — dead-letter queue, retry policies |
| **Cost** | None (runs in pipeline compute) | Additional service cost |
| **Scalability** | Limited — sequential per batch | Higher — async, parallel |

**Decision**: ForEachBatch sink. For the current alert volume (fraud detection on streaming transactions), sending emails directly from the pipeline is sufficient. If volume grows significantly, migrating to an external notification service (e.g., AWS SES, SendGrid, a notification microservice) with a dead-letter queue would be the next evolution.

### Trade-Off 6: Serverless vs Classic Compute

| Aspect | Serverless | Classic Compute |
| --- | --- | --- |
| **Startup time** | Seconds | Minutes |
| **Scaling** | Automatic, fine-grained | Manual or basic autoscaling |
| **Cost model** | Pay-per-use (per second) | Pay-per-hour (even when idle) |
| **Incremental MV refresh** | Supported (automatic) | Not supported (always full recompute) |
| **Control** | Less granular config | Full cluster customization |

**Decision**: Serverless. The pipeline needs auto-scaling for variable streaming loads, fast startup for triggered runs, and incremental MV refresh for reporting aggregates. Serverless provides all three. Classic compute would require manual scaling and force full recomputation of materialized views on every refresh.

---

## 11. Future Considerations

### Potential Enhancements

* **Continuous mode**: The pipeline is currently triggered (not continuous). For true real-time fraud detection with sub-second latency, consider switching to continuous mode with real-time update flows (requires DBR 18.1+ preview channel)
* **Auto CDC**: If the fraud watchlist source supports change data capture (updates to existing records), consider replacing the streaming append table with Auto CDC for proper upsert semantics (SCD Type 1 or 2)
* **Alert deduplication**: Currently each transaction generates a separate alert. Adding deduplication (e.g., `dropDuplicatesWithinWatermark` on `transaction_id`) would prevent duplicate alerts from Kafka redelivery
* **Multiple notification channels**: The alerts layer is designed for extensibility — new files can be added for SMS, push notifications, or Slack by creating new ForEachBatch sinks that read the same gold alert tables
* **Data retention policies**: Bronze and silver streaming tables grow indefinitely. Consider adding retention policies (e.g., `ALTER TABLE ... SET TBLPROPERTIES('delta.logRetentionDuration'='interval 30 days')`) or a vacuum strategy
* **Dashboard integration**: The gold materialized views (customer_risk_profile, fraud_trends_by_category, daily_transaction_summary) are designed for direct consumption by AI/BI dashboards or SQL queries
* **Schema registry**: The transaction schema is defined as a Python `StructType`. If the source system evolves, consider integrating with a schema registry (e.g., Confluent Schema Registry) for automated schema management

### Scalability Considerations

* The `maxOffsetsPerTrigger=100` setting in bronze Kafka ingestion controls throughput. If the pipeline needs to handle higher volumes, this can be increased
* The 10-minute watermark window in fraud_card_alerts may need adjustment for higher-volume streams where state size becomes a concern
* Materialized views with 3+ joins (customer_risk_profile) may benefit from decomposition into intermediate private MVs if refresh times become excessive

---

## Appendix: Pipeline Configuration Summary

### fraude-detection-pl

| Setting | Value |
| --- | --- |
| Pipeline Type | WORKSPACE |
| Serverless | Enabled |
| Photon | Enabled |
| Channel | CURRENT |
| Catalog | `workspace` |
| Schema | `default` |
| Continuous | No (triggered) |
| Publishing Mode | DEFAULT (Unity Catalog) |
| Library Pattern | Glob: `fraud-detection-root/**` |

### customer-master-data-pl

| Setting | Value |
| --- | --- |
| Pipeline Type | WORKSPACE |
| Target Table | `fraud_detection.silver.customers` |
| Source | `fraud_detection.bronze.customers` (via Lakeflow Connect) |
| Quality | Strict (drop on invalid customer_id/age) |
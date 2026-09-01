# Source Data Preparation

This folder contains all the scripts and modules needed to generate and publish synthetic credit card transaction data to a Confluent Kafka topic, and to generate fraud watchlist JSON files into a Databricks Unity Catalog volume. The generated data feeds the **FinGuard Fraud Detection Pipeline** (`fraude-detection-pl`).

---

## Folder Structure

```
source_data_preparation/
├── README.md                          ← This file
├── .env                               ← Kafka & generator settings (gitignored)
├── .env.example                       ← Template for .env
├── requirements.txt                   ← Python package dependencies
├── producer_normal.py                ← Script 1: Stream normal transactions to Kafka
├── producer_fraud_transaction.py       ← Script 2: Stream high-value fraud transactions to Kafka
├── config.py                          ← Loads settings from .env (Settings dataclass)
├── models.py                          ← Data models (Customer, Merchant, Transaction)
├── utils.py                           ← Shared utilities (ID generation, JSON validation, etc.)
├── customer_generator.py              ← Generates realistic customer records → CSV
├── merchant_generator.py             ← Generates realistic merchant records → CSV
├── fraud_engine.py                    ← Evaluates fraud risk score per transaction
├── transaction_generator.py           ← Generates transactions from customers + merchants
└── fraud_watchlist/
    ├── fraud_watchlist_generator.py   ← Script 3: Writes watchlist JSON files to UC Volume
    └── fraud_watchlist.csv            ← Source data for watchlist generation
```

---
## Scripts Overview

### 1. `producer_normal.py` — Normal Transaction Producer

Streams realistic credit card transactions to a Kafka topic at a configurable rate. Transactions include a mix of normal spending patterns across customer segments (Regular, Gold, Platinum, Corporate) and merchant categories. Fraud injection is set to 0% for this producer — all transactions are legitimate.

**Key behavior:**
* Generates 1,000 customers and 200 merchants (configurable via `.env`)
* Produces transactions at `TRANSACTIONS_PER_SECOND` rate (default: 5/sec)
* Each transaction gets a `TXN` prefix in its ID (e.g., `TXN0000123`)
* Amount ranges from ~1.00 to ~99,999.99
* Runs continuously until interrupted (Ctrl+C or SIGTERM)
* Uses idempotent Kafka producer with acks=all and 5 retries

### 2. `producer_fraud_transaction.py` — Fraud Transaction Producer

Produces a single high-value fraud transaction to the Kafka topic. The amount is forced above 100,001.00, which triggers the `HIGH_VALUE_TRANSACTION` fraud rule in the fraud engine. The transaction ID uses a `TXF` prefix to distinguish it from normal transactions.

**Key behavior:**
* Generates the same customer/merchant pool as the normal producer
* Produces exactly one transaction per run (not continuous)
* Forces amount to at least 100,001.00
* Transaction ID format: `TXF######`
* Uses the same fraud engine to evaluate risk score

### 3. `fraud_watchlist/fraud_watchlist_generator.py` — Watchlist JSON Generator

Reads the `fraud_watchlist.csv` file and writes each row as an individual JSON file to a Databricks Unity Catalog volume. The pipeline's Auto Loader bronze layer picks up these JSON files incrementally.

**Key behavior:**
* Reads `fraud_watchlist.csv` from the same folder
* Writes one JSON file per row to `/Volumes/fraud_detection/source/fraud_watchlist/fraud/`
* Checks for existing files and resumes from the last processed `watchlist_id` (idempotent)
* Waits 5 seconds between each file to simulate real-time watchlist updates
* Must run in a Databricks notebook context (uses `dbutils.fs`)

---

## Setup Instructions

### Step 1: Install Python Dependencies

These scripts use `confluent-kafka` for Kafka connectivity and `faker` for generating realistic customer/merchant data. Install all dependencies:

```bash
pip install -r requirements.txt
```

**Dependencies:**

| Package | Purpose |
| --- | --- |
| `confluent-kafka` >= 2.6.0 | Kafka producer client |
| `faker` >= 30.0.0 | Realistic name/address generation |
| `python-dotenv` >= 1.0.0 | Load `.env` configuration |
| `pandas` >= 2.2.0 | CSV read/write for generated data |
| `numpy` >= 2.0.0 | Random number generation for amounts |

### Step 2: Configure Environment Variables

Copy `.env.example` to `.env` and fill in your Confluent Kafka credentials:

```bash
cp .env.example .env
```

Edit `.env` with your values:

```ini
# Confluent Kafka cluster (required)
BOOTSTRAP_SERVERS=pkc-xxxxx.confluent.cloud:9092
API_KEY=your_confluent_api_key
API_SECRET=your_confluent_api_secret

# Kafka topic (required)
TOPIC_NAME=credit-card-transaction

# Generator settings (optional - defaults shown)
TRANSACTIONS_PER_SECOND=5
FRAUD_PERCENTAGE=0.08
TOTAL_CUSTOMERS=1000
TOTAL_MERCHANTS=200
RANDOM_SEED=42
```

> **Security note:** The `.env` file contains Kafka API keys and is gitignored. Never commit it to version control. Use `.env.example` as the template for new environments.

---

## How to Run

### Running the Normal Transaction Producer

The normal producer runs continuously, streaming transactions to Kafka at the configured rate. Run it in a terminal:

```bash
python producer_normal.py
```

**Expected output:**
```
2026-09-01 10:00:00 - INFO - Starting normal producer for topic=credit-card-transaction
2026-09-01 10:00:00 - INFO - Produced TXN0000001 | Amount=1250.00
2026-09-01 10:00:00 - INFO - Produced TXN0000002 | Amount=890.50
...
```

Press `Ctrl+C` to stop. The producer flushes all pending messages before exiting.

### Running the Fraud Transaction Producer

The fraud producer sends a single high-value transaction. Run it multiple times to inject multiple fraud transactions:

```bash
python producer_fraud_transaction.py
```

**Expected output:**
```
2026-09-01 10:05:00 - INFO - Starting fraud producer for topic=credit-card-transaction
2026-09-01 10:05:00 - INFO - Produced TXF456789 | Amount=150000.00
2026-09-01 10:05:00 - INFO - Flushing producer and exiting.
```

### Running the Fraud Watchlist Generator

This script must run inside a **Databricks notebook** (it uses `dbutils.fs` to write to Unity Catalog volumes). To run it:

1. Open the `fraud_watchlist/fraud_watchlist_generator.py` file in a Databricks notebook
2. Ensure the `fraud_watchlist/fraud_watchlist.csv` file is accessible in the same directory
3. Run the notebook cell

The script will:
* Check existing JSON files in `/Volumes/fraud_detection/source/fraud_watchlist/fraud/`
* Skip already-processed watchlist entries (resumes from the last `watchlist_id`)
* Write each new entry as a separate JSON file with a 5-second interval

**Expected output:**
```
Found 30 existing files in /Volumes/fraud_detection/source/fraud_watchlist/fraud/
Scanning for latest watchlist_id...
Latest watchlist_id found: wl000030 (numeric: 30)
Resuming from next record. 30 rows remaining to process.

Starting to process 30 rows from fraud_watchlist.csv
Writing JSON files to: /Volumes/fraud_detection/source/fraud_watchlist/fraud/

Entire row being written: {'watchlist_id': 'wl000031', ...}
Row 1/30: Written fraud_watchlist_20260901_100523_123456_0.json
  Watchlist ID: wl000031
  Waiting 5 seconds...

...
Completed! Generated 30 JSON files in /Volumes/fraud_detection/source/fraud_watchlist/fraud/
```

---

## Recommended Execution Order

For a fresh end-to-end pipeline test, follow this order:

| Step | Script | Where to Run | Purpose |
| --- | --- | --- | --- |
| 1 | `pip install -r requirements.txt` | Local terminal | Install dependencies |
| 2 | Configure `.env` | Local file edit | Set Kafka credentials |
| 3 | `fraud_watchlist_generator.py` | Databricks notebook | Load fraud watchlist into UC Volume |
| 4 | `producer_normal.py` | Local terminal | Start streaming normal transactions |
| 5 | `producer_fraud_transaction.py` | Local terminal (separate window) | Inject fraud transactions |
| 6 | Run `fraude-detection-pl` pipeline | Databricks pipeline editor | Process and alert on fraud |

Steps 4 and 5 can run concurrently — the fraud producer injects high-value transactions alongside the normal stream.

---

## Configuration Reference

All configuration is managed through the `.env` file. Settings are loaded by `config.py` via `python-dotenv`.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `BOOTSTRAP_SERVERS` | Yes | — | Confluent Kafka bootstrap server address |
| `API_KEY` | Yes | — | Confluent Kafka SASL API key |
| `API_SECRET` | Yes | — | Confluent Kafka SASL API secret |
| `TOPIC_NAME` | Yes | `credit_card_transactions` | Kafka topic to produce transactions to |
| `TRANSACTIONS_PER_SECOND` | No | `5` | Rate of transaction generation (normal producer) |
| `FRAUD_PERCENTAGE` | No | `0.08` | Probability of fraud injection (0.0 = no fraud, 1.0 = always fraud) |
| `TOTAL_CUSTOMERS` | No | `1000` | Number of synthetic customers to generate |
| `TOTAL_MERCHANTS` | No | `200` | Number of synthetic merchants to generate |
| `RANDOM_SEED` | No | `42` | Seed for reproducible random generation |

---

## Module Reference

### `config.py`

Defines the `Settings` frozen dataclass and `load_settings()` function. Reads from `.env` using `python-dotenv`, validates required fields, and returns a typed settings object.

### `models.py`

Dataclass definitions for the three core entities:

* **`Customer`** — 20 fields including demographics, email, financial profile, spending preferences, and card details
* **`Merchant`** — 7 fields including category, location, risk level, and blacklist status
* **`Transaction`** — 18 fields including amount, currency, channel, device, location, and fraud scoring
* **`StatsSnapshot`** — Runtime statistics for monitoring

### `customer_generator.py`

`CustomerGenerator` class that creates realistic Indian customer profiles using `Faker` (en_IN locale). Generates Luhn-valid card numbers, assigns customer segments (Regular/Gold/Platinum/Corporate) with weighted probabilities, and derives spending ranges from segment and income. Persists results to `data/customers.csv`.

### `merchant_generator.py`

`MerchantGenerator` class that creates merchant records across 12 categories (Grocery, Fuel, Restaurant, Hotel, Electronics, Jewellery, ATM, Pharmacy, Shopping, Airline, Entertainment, Travel). Assigns risk levels (LOW 65%, MEDIUM 25%, HIGH 10%) and blacklists every 20th merchant. Persists to `data/merchants.csv`.

### `fraud_engine.py`

`FraudEngine` class that evaluates each transaction against 8 fraud rules:

| Rule | Weight | Trigger |
| --- | --- | --- |
| `HIGH_VALUE_TRANSACTION` | 40 | Amount > 100,000 |
| `IMPOSSIBLE_TRAVEL` | 50 | Delhi → London within 20 minutes |
| `NEW_DEVICE` | 20 | Device ID ≠ trusted device |
| `HIGH_RISK_MERCHANT` | 25 | Merchant risk level = HIGH |
| `BLACKLISTED_MERCHANT` | 60 | Merchant is blacklisted |
| `INTERNATIONAL_TRANSACTION` | 25 | Transaction is international |
| `VELOCITY_FRAUD` | 45 | 5+ transactions in 30 seconds |
| `CARD_TESTING` | 30 | 3+ small transactions ($5-$20) in 60 seconds |

The final fraud score is capped at 100. The engine maintains per-customer transaction history (deque, max 50) for velocity and pattern detection.

### `transaction_generator.py`

`TransactionGenerator` class that orchestrates transaction creation:
* Selects a random customer and a merchant matching the customer's segment preferences
* Generates amount based on customer spending range and merchant category
* Determines international status, currency, transaction type, payment channel, and device
* Passes the transaction through `FraudEngine` for risk scoring

### `utils.py`

Shared utility functions:
* `utc_now_iso()` — Current UTC timestamp in ISO 8601 format
* `ensure_parent_dir(path)` — Creates parent directories if needed
* `validate_json_payload(payload)` — Validates that a dict is JSON-serializable
* `generate_id(prefix, index, width)` — Generates zero-padded IDs (e.g., `CUST000042`)
* `serialize_json(data)` — Compact JSON serialization
* `clamp(value, low, high)` — Clamps a value to a range
* `weighted_choice(options)` — Weighted random selection

---

## Data Output Locations

| Output | Location | Format |
| --- | --- | --- |
| Customer records | `data/customers.csv` (local, auto-created) | CSV |
| Merchant records | `data/merchants.csv` (local, auto-created) | CSV |
| Kafka transactions | Confluent Kafka topic (from `.env`) | JSON messages |
| Fraud watchlist JSON files | `/Volumes/fraud_detection/source/fraud_watchlist/fraud/` | JSON (one per row) |

---

## Troubleshooting

**`ValueError: Missing required environment variables`**
→ The `.env` file is missing or `BOOTSTRAP_SERVERS`, `API_KEY`, or `API_SECRET` are not set. Copy `.env.example` to `.env` and fill in the values.

**`KafkaError: Broker transport failure`**
→ The Confluent Kafka cluster is unreachable. Verify the `BOOTSTRAP_SERVERS` address and network connectivity. Check that your API key/secret are valid.

**`ModuleNotFoundError: No module named 'confluent_kafka'`**
→ Dependencies not installed. Run `pip install -r requirements.txt`.

**`fraud_watchlist_generator.py` fails with `NameError: name 'dbutils' is not defined`**
→ This script requires a Databricks notebook runtime. It cannot run locally — run it inside a Databricks notebook.

**`fraud_watchlist_generator.py` reports `All records have already been processed!`**
→ All watchlist entries from the CSV have already been written to the volume. To re-process, either add new rows to the CSV or delete existing JSON files from the volume.

---

## Security Notes

* The `.env` file contains Confluent Kafka API credentials and is gitignored (see `.gitignore` at repo root)
* Kafka authentication uses SASL_SSL with PLAIN mechanism
* The fraud watchlist generator writes to a Unity Catalog volume — ensure the pipeline owner has `WRITE VOLUME` permissions on `/Volumes/fraud_detection/source/fraud_watchlist/`
* Never share the `.env` file. Use `.env.example` as the template for onboarding new team members
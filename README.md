# Ledgerline

[![CI](https://github.com/AndrewLhn/Ledgerline/actions/workflows/ci.yml/badge.svg)](https://github.com/AndrewLhn/Ledgerline/actions/workflows/ci.yml)
[![End-to-end](https://github.com/AndrewLhn/Ledgerline/actions/workflows/e2e.yml/badge.svg)](https://github.com/AndrewLhn/Ledgerline/actions/workflows/e2e.yml)

Local event pipeline for retail operations:

~~~text
Kafka -> Airflow -> Iceberg / MinIO -> Trino -> dbt
~~~

Ledgerline ingests versioned operational events (orders, payments, refunds,
inventory adjustments), validates every event against a JSON Schema contract,
appends valid records to Iceberg, routes invalid ones to a Kafka DLQ with a
replay path, and builds a daily payment-to-revenue reconciliation guarded by
dbt tests. Everything runs on Docker Compose with synthetic data.

## Components

| Component | Role |
| --- | --- |
| Kafka | Event transport and dead-letter topic |
| Airflow | Batch ingestion, retries, table maintenance |
| Iceberg + MinIO | Raw event storage and audit trail |
| Trino | SQL query engine |
| dbt | Event models, quality checks, and reconciliation |
| Prometheus + Grafana | Lag, DLQ, and batch metrics (optional profile) |
| GitHub Actions | Unit tests, linting, coverage, and E2E workflow |

## Event contract

The current contract supports:

~~~text
order_created
payment_captured
refund_issued
inventory_adjusted
~~~

Each event contains a source event ID (UUID), type, schema version, occurrence
time, source system, correlation ID, and event-specific data. Monetary amounts
are integer minor units (`amount_minor`, cents), never floats. The full schema
is in [contracts/kafka_event.schema.json](contracts/kafka_event.schema.json).

Messages are keyed by `event_id`, so one entity keeps its partition and order.
Invalid payloads are sent to the ledgerline_events_dlq topic with the
validation error and Kafka location, and can be replayed after a fix
(`make replay-dlq`).

## Run locally

Requirements: Docker, Docker Compose, and Python 3.12 (host dependencies ship
3.12 wheels).

~~~bash
python3 -m pip install -r requirements.txt
cp .env.example .env
docker compose up -d --build
~~~

Local endpoints:

| Service | Address |
| --- | --- |
| Airflow | http://localhost:8080 |
| MinIO | http://localhost:9001 |
| Trino | http://localhost:8082 |
| Kafka | localhost:9092 |
| PostgreSQL | 127.0.0.1:5432 (loopback only) |
| Grafana | http://localhost:3000 (monitoring profile) |

Airflow standalone creates an `admin` user on first start. Print the generated
password with:

~~~bash
docker compose exec airflow-standalone cat /opt/airflow/standalone_admin_password.txt
~~~

The shortest path through the whole pipeline:

~~~bash
make demo
~~~

It publishes the sample flow, runs one ingestion batch, builds dbt, and prints
the reconciliation mart. The sample publishes EUR 100.00 of orders, EUR 100.00
of captured payments, and EUR 20.00 of refunds, so net cash and recognised
revenue both equal EUR 80.00.

Step by step instead:

~~~bash
python3 infra/producer.py --scenario ledgerline
docker compose exec airflow-standalone airflow dags trigger kafka_to_iceberg
make dbt-build
docker compose exec -it trino trino   # SELECT * FROM iceberg.analytics.mart_daily_reconciliation;
~~~

To run one ingestion batch on the host rather than in Airflow:

~~~bash
python3 -m pip install -r infra/requirements.txt
python3 infra/consumer.py
~~~

## Test cases

~~~bash
python3 infra/producer.py --scenario duplicate
python3 infra/producer.py --scenario invalid
~~~

The duplicate has an existing source event ID and is removed in the dbt
staging model. The invalid payload is rejected by the consumer and written to
the DLQ.

The E2E workflow verifies:

- five valid raw records after the duplicate scenario;
- four deduplicated event facts;
- one invalid record in the DLQ;
- matching counts in the ingestion audit table;
- a passing daily reconciliation.

## Data models

| Model | Purpose |
| --- | --- |
| stg_kafka_events | Typed and deduplicated event envelope with ingestion lag |
| stg_ingestion_runs | One row per consumer batch (counts, duration) |
| fct_kafka_events | Analytics event fact |
| fct_ledgerline_events | Parsed order, payment, refund, and inventory fields |
| dim_event_payloads | Distinct event payloads keyed by content hash |
| mart_event_metrics_daily | Daily volume, cardinality, and freshness metrics |
| mart_ingestion_health_daily | Daily batch health from the audit table |
| mart_daily_reconciliation | Daily net-cash and revenue comparison |

## Quality gates

- Contract validation at the edge; invalid events never reach the warehouse.
- dbt tests on every build: uniqueness, accepted values, financial
  completeness, reconciliation threshold (EUR 0.01), and a six-hour freshness
  SLO on ingestion lag.
- Consumer failure modes (append failure, DLQ timeout) are covered by unit
  tests: offsets are committed only after a successful Iceberg append.

## Monitoring

~~~bash
make monitoring-up
~~~

Starts Prometheus, Grafana, Pushgateway, and a Kafka exporter. The Grafana
dashboard (admin credentials from `.env`) shows consumer lag, DLQ size, and
per-batch read/written/rejected counts. Durable batch history lives in
`mart_ingestion_health_daily` and does not depend on the monitoring profile.

## Operations

- `iceberg_maintenance` DAG expires raw-table snapshots older than seven days.
- `make replay-dlq` republishes DLQ events that pass the current contract.
- [Backfill runbook](docs/runbooks/backfill.md) covers offset resets and
  rebuilds.
- `ORDERS=20000 make bench` runs a load test; see
  [performance notes](docs/performance.md) for what to measure.

## Checks

~~~bash
python3 -m unittest discover -s tests -v
make validate
make lint
make dbt-test
~~~

CI runs configuration validation, unit tests with coverage, producer import
validation, Ruff, and an image build on pushes to main and on pull requests.
E2E runs on pull requests, pushes to main, or manually. `pre-commit install`
enables the same lint locally.

## Production gaps

Deliberate omissions of the local stack, each with its production control
documented:

- Kafka, Trino, and MinIO run unauthenticated inside the compose network
  (docs/security.md).
- Iceberg compaction and orphan-file cleanup need Spark procedures (ADR-005).
- Single-node everything: no HA, no backups, one consumer thread.
- Multi-currency reconciliation is out of scope (ADR-004).

## Documentation

- [Architecture](docs/architecture.md)
- [Domain and reconciliation rules](docs/business-context.md)
- [Security posture](docs/security.md)
- [Data-quality incident runbook](docs/runbooks/data-quality-incident.md)
- [Backfill runbook](docs/runbooks/backfill.md)
- [Performance notes](docs/performance.md)
- [Architecture decisions](docs/adr)

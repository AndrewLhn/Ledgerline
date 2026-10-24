# Ledgerline

Local event pipeline for retail operations:

~~~text
Kafka -> Airflow -> Iceberg / MinIO -> Trino -> dbt
~~~

The project validates versioned operational events, writes valid records to Iceberg, sends
invalid records to a Kafka DLQ, and builds a daily payment-to-revenue reconciliation.

## Components

| Component | Role |
| --- | --- |
| Kafka | Event transport and dead-letter topic |
| Airflow | Batch ingestion and retries |
| Iceberg + MinIO | Raw event storage |
| Trino | SQL query engine |
| dbt | Event models, quality checks, and reconciliation |
| GitHub Actions | Unit tests, linting, and E2E workflow |

## Event contract

The current contract supports:

~~~text
order_created
payment_captured
refund_issued
inventory_adjusted
~~~

Each event contains a source event ID, type, schema version, occurrence time, source system,
correlation ID, and event-specific data. The full schema is in
[contracts/kafka_event.schema.json](contracts/kafka_event.schema.json).

Invalid payloads are sent to the ledgerline_events_dlq topic with the validation error and
Kafka location.

## Run locally

Requirements: Docker, Docker Compose, and Python 3.12 (host dependencies are pinned to
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

Airflow standalone creates an `admin` user on first start. Print the generated password
with:

~~~bash
docker compose exec airflow-standalone cat /opt/airflow/standalone_admin_password.txt
~~~

Publish the sample flow and run ingestion:

~~~bash
python3 infra/producer.py --scenario ledgerline
docker compose exec airflow-standalone airflow dags trigger kafka_to_iceberg
make dbt-build
~~~

To run one ingestion batch on the host instead of triggering the DAG:

~~~bash
python3 -m pip install -r infra/requirements.txt
python3 infra/consumer.py
~~~

Check the reconciliation result (open a Trino shell with
`docker compose exec -it trino trino`):

~~~sql
SELECT *
FROM iceberg.analytics.mart_daily_reconciliation;
~~~

The sample publishes EUR 100.00 of orders, EUR 100.00 of captured payments, and EUR 20.00
of refunds. Net cash and recognised revenue both equal EUR 80.00.

## Test cases

~~~bash
python3 infra/producer.py --scenario duplicate
python3 infra/producer.py --scenario invalid
~~~

The duplicate has an existing source event ID and is removed in the dbt staging model.
The invalid payload is rejected by the consumer and written to the DLQ.

The E2E workflow verifies:

- five valid raw records after the duplicate scenario;
- four deduplicated event facts;
- one invalid record in the DLQ;
- a passing daily reconciliation.

## Data models

| Model | Purpose |
| --- | --- |
| stg_kafka_events | Typed and deduplicated event envelope |
| fct_kafka_events | Analytics event fact |
| fct_ledgerline_events | Parsed order, payment, refund, and inventory fields |
| dim_event_payloads | Distinct event payloads keyed by content hash |
| mart_event_metrics_daily | Daily event metrics |
| mart_daily_reconciliation | Daily net-cash and revenue comparison |

## Checks

~~~bash
python3 -m unittest discover -s tests -v
make validate
make lint
make dbt-test
~~~

CI runs configuration validation, unit tests, producer import validation, and Ruff on pushes
to main and on pull requests. E2E runs on pull requests, pushes to main, or manually.

## Notes

- Raw ingestion is at-least-once. dbt deduplicates transformed facts by source event ID.
- The supported runtime is Docker Compose.
- The repository uses only synthetic data.

## Documentation

- [Architecture](docs/architecture.md)
- [Domain and reconciliation rules](docs/business-context.md)
- [Data-quality incident runbook](docs/runbooks/data-quality-incident.md)
- [Architecture decisions](docs/adr)

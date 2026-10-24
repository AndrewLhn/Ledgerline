# Architecture

~~~mermaid
flowchart LR
    P[Producer] --> K[Kafka]
    K --> A[Airflow consumer]
    A -->|invalid| DLQ[Kafka DLQ]
    A -->|valid| I[Iceberg raw events]
    A -->|audit row| AU[ingestion_runs]
    I --> M[MinIO]
    I --> T[Trino]
    AU --> T
    T --> D[dbt]
    D --> R[Reconciliation mart]
    DLQ -.->|replay tool| K
~~~

## Ingestion

1. The producer validates the event against the JSON Schema and sends it to
   Kafka, keyed by `event_id` so one entity stays on one partition.
2. The Airflow task decodes JSON and validates the event again, server-side.
3. Invalid events go to ledgerline_events_dlq with the error and Kafka location.
4. Valid events are appended to ledgerline_raw.events, partitioned by
   `days(ingested_at)`.
5. Each non-empty batch writes one audit row to ledgerline_raw.ingestion_runs.
6. dbt builds typed, deduplicated facts and marts.

## Stored metadata

The raw event table includes:

~~~text
event_id
event_type
schema_version
occurred_at        (ISO-8601 string, as sent by the source)
ingested_at        (timestamptz, set by the consumer)
source_system
correlation_id
kafka_partition
kafka_offset
data               (validated payload as JSON text)
~~~

Event time and ingestion time are stored separately. Staging derives
`ingestion_lag_seconds` per event; the daily metrics mart aggregates the maximum
lag and counts events more than one hour late. A dbt test fails the build when
the lag exceeds the six-hour freshness SLO.

## Delivery semantics

Kafka offsets are committed after the Iceberg append. A failure between these
operations may replay raw records. This is expected at-least-once behaviour.

The staging model keeps one transformed row per source event ID. Raw data is
retained for investigation; marts use the deduplicated event set. Audit rows
follow the same at-least-once rule: a replayed batch adds a second run row,
which is acceptable for operational metrics.

## Failure handling

| Failure | Handling |
| --- | --- |
| Invalid JSON or contract | Send the message to the DLQ |
| Kafka or task failure | Airflow retries the batch |
| Duplicate source event | Deduplicate in dbt |
| DLQ backlog after a fix | Replay with scripts/replay_dlq.py |
| Metadata growth | iceberg_maintenance DAG expires old snapshots daily |
| Reconciliation mismatch | Fail the dbt test |
| E2E failure | Upload Compose logs in GitHub Actions |

## Observability

The `monitoring` compose profile runs Prometheus, Grafana, Pushgateway, and a
Kafka exporter. The consumer pushes per-batch metrics (read/written/rejected,
duration) on a best-effort basis; ingestion never fails because metrics are
unreachable. Durable batch history lives in ingestion_runs and is modelled by
mart_ingestion_health_daily. See ADR-006.

## Runtime boundary

Docker Compose is the only supported runtime. Compaction and orphan-file removal
for Iceberg need Spark procedures and are out of scope locally; the maintenance
DAG only expires snapshots. Network services run without TLS or SASL. See
docs/security.md for the full posture and production gaps.

# Architecture

~~~mermaid
flowchart LR
    P[Producer] --> K[Kafka]
    K --> A[Airflow consumer]
    A -->|invalid| DLQ[Kafka DLQ]
    A -->|valid| I[Iceberg raw events]
    I --> M[MinIO]
    I --> T[Trino]
    T --> D[dbt]
    D --> R[Reconciliation mart]
~~~

## Ingestion

1. The producer sends a version-1 Ledgerline event to Kafka.
2. The Airflow task decodes JSON and validates the event against the JSON Schema.
3. Invalid events go to ledgerline_events_dlq.
4. Valid events are appended to ledgerline_raw.events.
5. dbt builds typed, deduplicated facts and marts.

## Stored metadata

The raw event table includes:

~~~text
event_id
event_type
schema_version
occurred_at
ingested_at
source_system
correlation_id
kafka_partition
kafka_offset
~~~

Event time and ingestion time are stored separately. This makes late events measurable.

## Delivery semantics

Kafka offsets are committed after the Iceberg append. A failure between these operations may
replay raw records. This is expected at-least-once behaviour.

The staging model keeps one transformed row per source event ID. Raw data is retained for
investigation; marts use the deduplicated event set.

## Failure handling

| Failure | Handling |
| --- | --- |
| Invalid JSON or contract | Send the message to the DLQ |
| Kafka or task failure | Airflow retries the batch |
| Duplicate source event | Deduplicate in dbt |
| Reconciliation mismatch | Fail the dbt test |
| E2E failure | Upload Compose logs in GitHub Actions |

## Runtime boundary

Docker Compose is the only supported runtime.
A production deployment would need isolated environments, secret management, network
controls, monitoring, alerting, backups, and access policies.

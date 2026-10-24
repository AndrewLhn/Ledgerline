# ADR 002: At-least-once raw ingestion

- Status: Accepted
- Date: 2026-10-08, amended 2026-10-24

## Context

Kafka and Iceberg do not share one transaction. A crash after the Iceberg
append and before the offset commit can replay records.

## Options considered

- Kafka transactions + exactly-once semantics: covers Kafka-to-Kafka flows
  only; the Iceberg sink is outside the transaction.
- MERGE into Iceberg per batch (upsert by event_id): idempotent writes, but
  per-batch MERGE is heavier than append and pyiceberg 0.6 support is limited.
- Append-only at-least-once raw + deduplication in dbt: simplest writes, raw
  layer keeps full replay evidence.

## Decision

Append valid events to Iceberg, then commit Kafka offsets. Treat the raw layer
as at-least-once and deduplicate transformed facts by source event ID
(last write wins, ordered by partition and offset).

Amendment: producers key messages by `event_id`. One entity stays on one
partition, so per-entity ordering is preserved and the deduplication ordering
is unambiguous even with multiple partitions.

## Consequences

Raw duplicates remain available for investigation. dbt keeps one row per
source event ID for analytics and reconciliation. Audit rows in
ingestion_runs follow the same semantics: a replayed batch may add a second
row, which is acceptable for operational metrics.

# Ledgerline design document

Status: living document. Scope: the local reference implementation and the
reasoning behind it, including what changes at larger scale.

## 1. Purpose

Ledgerline is a reference event platform for retail operations: validated
ingestion, durable raw storage, reconciled marts, and enforceable quality
gates — reproducible on one machine with `docker compose up`. It exists to
demonstrate engineering judgment end to end, not to serve production traffic.

## 2. Requirements

Functional:

- Ingest four operational event types (orders, payments, refunds, inventory).
- Reject contract violations at the edge without losing them (DLQ + replay).
- Deduplicate replays deterministically.
- Produce a daily payment-to-revenue reconciliation with a hard threshold.

Non-functional (with the mechanism that enforces each):

| Requirement | Target | Mechanism |
| --- | --- | --- |
| Freshness | Queryable within 6h of occurrence | lag column + SLO dbt test |
| Delivery | At-least-once raw, effectively-once marts | commit-after-append + dbt dedup (ADR-002) |
| Integrity | No float money in sums | integer minor units (ADR-004) |
| Auditability | Every batch accounted for | ingestion_runs audit table (ADR-006) |
| Evolution | No silent contract breaks | immutable version files + CI gate (ADR-008) |
| Recoverability | Documented path per failure | runbooks + FMEA below |

Error budget reading: the freshness SLO is the only numeric budget; a breach
fails the dbt build, which is this project's equivalent of stopping a deploy.

## 3. Capacity model

Demo scale is ~6 events per E2E run; the load generator defaults to 5,000
order chains ≈ 10,500 events ≈ 4 MB of JSON.

Planning formulas for any scale:

~~~text
kafka_bytes/day   = events/day x ~350 B x (1 + duplication_factor)
minio_bytes/day   = events/day x ~150 B (parquet, compressed, deduped)
snapshots/week    = runs/day x 7 (each run = 1+ snapshot per touched table)
files/day         = runs/day x touched day-partitions
~~~

`ORDERS=n make bench` produces the real per-environment numbers
(publish rate, batch durations, lag); record them here per environment rather
than quoting another machine's results. Known per-run overhead: one consumer
group join and one catalog load (~1 s), so draining N events costs
O(N) I/O plus O(1) startup, bounded by the 4-minute time budget per run.

## 4. Architecture

Kafka (transport, DLQ) → Airflow (scheduled drain, retries) → Iceberg on
MinIO (raw, partitioned by ingestion day) → Trino (SQL) → dbt (staging dedup,
facts, marts, tests). Monitoring is an optional sidecar profile.

Decision index: ADR-001 runtime, ADR-002 delivery semantics and keying,
ADR-003 quality gates, ADR-004 money, ADR-005 table lifecycle,
ADR-006 observability, ADR-007 security boundary, ADR-008 contract
versioning.

## 5. Failure mode analysis

| Failure | Detection | Automatic response | Manual response |
| --- | --- | --- | --- |
| Invalid event | rejection count, DlqGrowing alert | routed to DLQ | replay tool after fix |
| Poison backlog in DLQ | DLQ size panel | none (evidence kept) | incident runbook |
| Crash after append, before commit | duplicate raw rows | staging dedup | none |
| Kafka unreachable | task failure, ConsumerLagHigh | Airflow retries x3 | backfill runbook |
| Iceberg/MinIO down | append failure, no commit | retries preserve offsets | backfill runbook |
| Catalog DB down | healthchecks, task failures | none | restore DB (prod: backup) |
| Late-data spike | max lag, SLO test fails build | build gate stops marts | investigate source |
| Producer schema drift | DLQ growth; CI gate blocks merges | contract rejection | governance doc |
| Small-file buildup | query latency | snapshot expiry only | Spark compaction (prod) |
| Scheduler down | IngestionStalled / NeverReported | none | restart; offsets durable |

## 6. Data lifecycle

Raw events: retained indefinitely locally; snapshots expire after 7 days
(ADR-005), which bounds metadata but keeps data files reachable through the
current snapshot. DLQ: retained until replayed or triaged; never auto-purged.
Audit rows: same lifecycle as raw events. Production would add tiered object
storage and explicit retention per table.

## 7. Scale narrative

- **Now (≤ ~50k events/day):** everything in this repo. Single consumer
  thread, hourly drain, JDBC catalog on Postgres.
- **10x:** raise topic partitions and run consumer tasks in parallel (group
  rebalancing does the assignment); compaction becomes mandatory, not
  optional; freshness SLO tightens from hours to minutes by shortening the
  schedule — the batch model still holds.
- **100x:** scheduled batches give way to a resident consumer or an Iceberg
  sink connector; the JDBC catalog is the first hard bottleneck (single
  writer, Postgres-backed) → REST catalog / Glue / Nessie; dbt models go
  incremental with merge; Trino gets real sizing and possibly separates
  ingestion reads from BI reads.
- **1000x / multi-team:** the contract becomes organizational — a registry
  with consumer-driven compatibility checks, deprecation windows enforced by
  policy; storage tiers, multi-region replication, and cost governance
  (request counts, compaction savings) become design inputs.

The first thing to break going up is the JDBC catalog, then small-file
pressure, then single-threaded consumption — in that order the fixes are
cheap, and each is a documented swap rather than a rewrite.

## 8. Cost considerations

Locally: zero beyond the host. Production drivers, in expected order of
magnitude: Trino compute per scanned byte (mitigated by partitioning and
compaction), object-store request counts (mitigated by larger files — the
same compaction), Kafka throughput tiers, and snapshot retention storage.
The audit table doubles as FinOps evidence: records_written per batch over
time is the volume trend line.

## 9. Security

See docs/security.md: least-privilege catalog roles and loopback exposure are
implemented; TLS/SASL, Trino auth, and per-service object-store identities
are documented production gaps rather than silently missing.

## 10. Deliberately not included

Streaming SQL enrichment, CDC sources, multi-currency FX, row-level access
control, ML features, a metrics layer (MetricFlow), Kubernetes manifests, and
multi-region anything. Each would inflate the demo without exercising the
judgment this document is about; each has a named home in the scale
narrative above.

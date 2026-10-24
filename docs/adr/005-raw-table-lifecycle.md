# ADR 005: Raw table lifecycle

- Status: Accepted
- Date: 2026-10-24

## Context

The raw events table started unpartitioned with unbounded snapshot growth.
Every hourly batch adds data files and a snapshot; incident triage needs time
travel, but keeping everything forever bloats metadata and storage.

## Options considered

- No maintenance: simplest, degrades over time.
- Spark maintenance jobs (rewrite_data_files, remove_orphan_files,
  expire_snapshots): full-featured, but adds a Spark runtime to a compose
  stack that deliberately has none.
- pyiceberg-native maintenance in Airflow: limited in 0.6 (snapshot expiry
  only), zero extra runtime.

## Decision

Partition ledgerline_raw.events by `days(ingested_at)` at creation so batches
land in day partitions and old days can be dropped or investigated cheaply.
An Airflow DAG (iceberg_maintenance, daily 03:00) expires snapshots older than
seven days on both raw tables. Compaction and orphan-file removal are
documented production steps requiring Spark, not faked locally.

## Consequences

Time travel within seven days works for incident triage. Small files from
100-message batches accumulate between compactions; at demo volume this is
noise, at production volume it requires the Spark procedure. The retention
window is an env knob (ICEBERG_SNAPSHOT_RETENTION_DAYS). Tables created before
this decision must be recreated (the local stack is disposable; E2E always
starts from empty volumes).

# Changelog

Notable changes to this project. Format loosely follows Keep a Changelog.

## Unreleased

### Changed

- Object storage: MinIO upstream was archived and its images deleted from
  Docker Hub and Quay in September 2026; the stack now runs the pinned
  `pgsty/silo` MinIO-compatible fork, and bucket bootstrap uses a plain S3
  call (infra/minio/init-bucket.py) instead of the discontinued `mc` client.
- Event contract: monetary amounts are integer minor units (`amount_minor`);
  the demo-only `id` field is gone; producers key messages by `event_id`
  (ADR-004, ADR-002).
- Contract files are versioned and immutable: v1 was renamed to
  kafka_event_v1.schema.json, validation dispatches on `schema_version`, and
  v2 (payment `method`) demonstrates the bump process (ADR-008,
  docs/contracts.md).
- The consumer drains the topic within a time budget instead of capping at
  100 messages per run, which silently bounded throughput at ~2.4k events/day.
- Raw events table is partitioned by `days(ingested_at)` and stores
  `ingested_at` as timestamptz; existing local tables must be recreated
  (ADR-005).
- Airflow image is based on 3.3.2 (2.x is EOL); Iceberg venv dependencies now
  come from infra/requirements.txt so pins cannot drift.
- dbt-core is pinned next to dbt-trino; generic tests use the `arguments:`
  syntax.

### Added

- Ingestion audit table (ledgerline_raw.ingestion_runs) with a daily health
  mart and E2E assertions against it.
- Freshness: per-event ingestion lag, daily max/late counts, and a six-hour
  SLO enforced by a dbt test.
- DLQ replay tool (scripts/replay_dlq.py) with dry-run mode and unit tests.
- iceberg_maintenance DAG expiring snapshots older than seven days.
- Monitoring compose profile: Prometheus, Grafana with a provisioned
  dashboard, Pushgateway, Kafka exporter (ADR-006).
- Postgres role separation for the Iceberg catalog database; security posture
  documented (ADR-007, docs/security.md).
- Load generator and `make bench`; performance methodology in
  docs/performance.md.
- Contract governance CI gate (immutability + self-consistency) with a
  labeled exception process; Prometheus alert rules for lag, DLQ growth, and
  stalled ingestion; design document with capacity model, failure-mode
  analysis, and the 10x/100x/1000x scale narrative.
- Backfill runbook; `make demo` one-command walkthrough; CHANGELOG.
- Consumer failure-mode unit tests (append failure, DLQ flush timeout,
  malformed payloads), coverage reporting in CI, pre-commit config, Docker
  image build in CI, dependabot coverage for Dockerfiles and infra
  requirements.

### Fixed

- E2E workflow: starts from empty volumes, waits for Airflow to parse the
  DAG, asserts DLQ counts exactly, and prints diagnostics on failure; it now
  also runs on pushes to main.
- infra/consumer.py imports and its default connection settings.

### Removed

- Broken Terraform prototype under infra/ (referenced a missing module and
  never initialized).
- dags/consumer_batch.py (vestigial entrypoint that broke DAG parsing).

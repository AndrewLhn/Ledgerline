# Changelog

Notable changes to this project. Format loosely follows Keep a Changelog.

## Unreleased

### Changed

- Event contract: monetary amounts are integer minor units (`amount_minor`);
  the demo-only `id` field is gone; producers key messages by `event_id`
  (ADR-004, ADR-002).
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

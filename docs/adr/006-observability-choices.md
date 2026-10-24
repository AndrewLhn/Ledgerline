# ADR 006: Observability choices

- Status: Accepted
- Date: 2026-10-24

## Context

Batch ingestion needs three views: is the consumer keeping up (lag), what did
each batch do (durable history), and what is broken right now (DLQ growth).
The stack is local and the consumer is a short-lived process, which rules out
scraping it directly.

## Options considered

- OpenTelemetry end-to-end: powerful, but heavy for a compose demo and most
  components would not emit anything useful.
- StatsD/DogStatsD: needs a always-on receiver, same problem as scraping.
- Prometheus + Pushgateway (batch metrics) + kafka-exporter (lag) + a durable
  audit table in Iceberg: each piece is small, standard, and optional.

## Decision

Two layers:

1. Durable: every non-empty batch appends one row to
   ledgerline_raw.ingestion_runs (counts, duration). dbt models it as
   mart_ingestion_health_daily. This is the source of truth and is queryable
   with the same tools as the data.
2. Ephemeral: the consumer best-effort pushes gauges to a Pushgateway, and
   kafka-exporter exposes consumer lag. Prometheus scrapes both; Grafana
   (provisioned with a dashboard) runs behind the `monitoring` compose
   profile.

Metrics never fail ingestion: push errors are logged and swallowed.

## Consequences

Lag, DLQ size, and batch health are visible live; batch history survives
container restarts. The audit row is at-least-once like the data (ADR-002), so
metrics derived from it tolerate occasional duplicate batches. Alerting is out
of scope locally; in production the same metrics would feed Alertmanager.

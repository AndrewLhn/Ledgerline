# ADR 007: Local security posture

- Status: Accepted
- Date: 2026-10-24

## Context

The stack ships secrets handling that must not embarrass anyone running it:
no credentials in git, no services exposed to the LAN, and database access
that is not one shared superuser for everything. Full production hardening
(TLS, SASL, vaults) cannot be meaningfully demonstrated on a single host.

## Decision

Draw an explicit boundary and document it (docs/security.md):

- Secrets live only in `.env` (git-ignored, required by compose).
- PostgreSQL is published on loopback only.
- The Iceberg catalog database keeps separate logins per service (`iceberg`,
  `trino`) for auditability, while a shared group role `catalog_owner` owns
  all catalog objects: Trino's JDBC catalog migrates its registry schema with
  DDL, which Postgres only permits to the owner. Both logins SET ROLE to
  catalog_owner inside that database; the Airflow metadata role is separate.
- Kafka, Trino, and MinIO stay unauthenticated inside the compose network;
  only Kafka (9092), Trino (8082), MinIO (9000/9001), Airflow (8080), and
  Grafana (3000) are published, for local tooling.

## Consequences

A reviewer can verify least-privilege on the catalog database without a
cloud account. Everything beyond the boundary is listed with its production
control in docs/security.md instead of being silently absent. Changing shared
credentials means updating `.env` and recreating volumes.

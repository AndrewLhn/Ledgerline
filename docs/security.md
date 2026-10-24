# Security posture (local stack)

This project runs a single-host Docker Compose stack with synthetic data. The
goal is a deliberate, documented boundary rather than production hardening.

## Implemented

- Secrets come from `.env`, which is git-ignored and required by compose
  (`:?` interpolation fails fast when a variable is missing). No credentials
  are committed.
- PostgreSQL is published on the loopback interface only (127.0.0.1:5432).
- The Iceberg catalog database has dedicated roles: `iceberg` (owner, used by
  the consumer through pyiceberg) and `trino` (used by Trino's JDBC catalog
  with grants limited to catalog tables). The Airflow metadata role does not
  own the catalog database and vice versa.
- Airflow's Python environment is isolated from Iceberg dependencies (separate
  venv in the image), so a dependency bump cannot silently break the
  scheduler.
- Configuration mounts (trino/, monitoring/) are read-only inside containers.
- Dependabot tracks pip requirements, Dockerfiles, and GitHub Actions.

## Deliberate gaps (production would need)

| Gap | Production control |
| --- | --- |
| Kafka is PLAINTEXT, no ACLs | SASL/TLS listeners, per-principal ACLs |
| Trino has no authentication | Password file / OAuth, per-user catalogs |
| MinIO root credentials shared by consumer and Trino | Per-service access keys, scoped bucket policies |
| Single PostgreSQL container; the Airflow role is superuser | Separate instances or managed services per workload |
| Grafana admin password from `.env` | SSO / org-managed identity |
| No network segmentation beyond one bridge network | Isolated subnets, egress control |
| No secrets rotation | Vault / cloud secret manager |

None of the services should be exposed beyond localhost as configured.

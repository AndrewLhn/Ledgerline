# ADR 001: Local Docker Compose stack

- Status: Accepted
- Date: 2026-10-08

## Context

The project must be reproducible for anyone reviewing it, without cloud
credentials, paid infrastructure, or a Kubernetes cluster.

## Options considered

- Managed cloud services: realistic, but not reproducible for reviewers and
  costs money to keep alive.
- Kubernetes (kind/minikube): closer to production orchestration, heavy for a
  single-pipeline demo, long setup on reviewer machines.
- Docker Compose: one command to start everything, versioned in the repo.

## Decision

Use Docker Compose as the supported runtime. Kafka, Airflow, PostgreSQL, MinIO,
Trino, and dbt run locally. Monitoring runs behind an optional compose profile
so the core stack stays light.

## Consequences

The project demonstrates local ingestion, storage, transformation, and
validation. It does not represent a managed cloud deployment or its
availability, security, and cost model. Anything that cannot run in compose
(Spark compaction, cluster HA) is documented as a production step instead of
being faked.

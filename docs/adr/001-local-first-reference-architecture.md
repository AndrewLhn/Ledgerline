# ADR 001: Local Docker Compose stack

- Status: Accepted
- Date: 2026-10-08

## Decision

Use Docker Compose as the supported runtime. Kafka, Airflow, PostgreSQL, MinIO, Trino, and
dbt run locally.

## Reason

The complete pipeline can be reproduced without cloud credentials or paid infrastructure.

## Consequences

The project demonstrates local ingestion, storage, transformation, and validation. It does
not represent a managed cloud deployment or its availability, security, and cost model.

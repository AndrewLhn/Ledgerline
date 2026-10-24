# Runbook: data-quality incident

## Triage

1. Record the failed DAG run, dbt model or test, and Kafka topic/partition/offset.
2. Stop using the affected mart until the failure is understood.
3. Keep raw records and DLQ messages; do not delete evidence to make a test pass.

## Checks

~~~bash
docker compose ps
docker compose logs --tail=200 airflow-standalone
make validate
make dbt-test
~~~

Batch-level history (what was read, written, rejected, and how long it took):

~~~sql
SELECT * FROM iceberg.analytics.mart_ingestion_health_daily ORDER BY metric_date DESC;
~~~

For GitHub Actions failures, download the Compose log artifact from the E2E run.

## DLQ replay

After fixing the producer or the contract, replay recoverable events. The dry
run lists what would happen without publishing anything:

~~~bash
python3 scripts/replay_dlq.py
python3 scripts/replay_dlq.py --apply
~~~

Only events that pass the current contract are republished, keyed by event_id.
Re-ingestion is idempotent: dbt deduplicates facts by source event ID. Events
that still fail validation are reported for manual triage; fix the source
system or extend the contract through a new schema version.

After a replay:

1. Trigger or wait for the ingestion DAG.
2. Run `make dbt-build`.
3. Confirm the reconciliation mart passes and DLQ growth has stopped
   (Grafana panel "DLQ size" with the monitoring profile).

## Common cases

| Symptom | Check |
| --- | --- |
| Invalid event | Inspect the DLQ message and JSON Schema error |
| Duplicate fact | Compare raw source event IDs with stg_kafka_events |
| Airflow failure | Inspect the failed task and service health |
| Reconciliation failure | Compare order, payment, and refund events for the business date |
| Freshness SLO failure | Compare occurred_at with ingested_at; check consumer lag and DAG schedule |
| E2E failure | Reproduce locally and inspect Compose logs |

## Recovery

1. Correct the source, contract, or transformation issue.
2. Add a test if the failure mode was not covered.
3. Re-run the Airflow task (or replay the DLQ, see above).
4. Run dbt build and dbt test.
5. Verify the affected mart with Trino.

Close the incident only after validation passes and the changed rule is
documented.

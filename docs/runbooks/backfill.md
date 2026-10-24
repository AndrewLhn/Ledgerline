# Runbook: backfilling a date range

Raw events are retained in ledgerline_raw.events, so reprocessing means
re-reading Kafka or rebuilding dbt models; both are idempotent.

## When dbt output is wrong but raw data is fine

Rebuild the transformed layer; no Kafka work needed:

~~~bash
make dbt-build
~~~

## When events were never ingested (consumer downtime)

The consumer group offset still points at the first unread record, so simply
run ingestion until the topic is drained:

~~~bash
docker compose exec airflow-standalone airflow dags trigger kafka_to_iceberg
# repeat, or run the batch script directly until it reports 0 records
make dbt-build
~~~

## When offsets were committed but records were lost

Reset the consumer group to the start of the window and re-run. Duplicates from
the overlap are removed by the staging deduplication:

~~~bash
docker compose exec -T kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group ledgerline-iceberg-consumer \
  --topic ledgerline_events \
  --reset-offsets --to-datetime 2026-10-01T00:00:00.000Z --execute

docker compose exec airflow-standalone airflow dags trigger kafka_to_iceberg
make dbt-build
~~~

Repeat the trigger until the consumer reports an empty batch (each run reads at
most 100 messages).

## Verify

~~~sql
SELECT * FROM iceberg.analytics.mart_ingestion_health_daily ORDER BY metric_date DESC;
SELECT * FROM iceberg.analytics.mart_daily_reconciliation WHERE metric_date = DATE '2026-10-01';
~~~

Counts in the audit table will show the replayed batches; facts stay unique per
source event ID.

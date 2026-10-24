# Performance notes

Numbers depend on the host, so this document fixes the method and the levers;
record results per environment instead of trusting anyone else's numbers.

## Benchmark method

~~~bash
make monitoring-up            # optional: watch lag live
ORDERS=5000 make bench
~~~

`make bench` publishes order chains with scripts/loadgen.py (one order + one
payment each, every tenth order adds a refund; ~2.1 events per order), drains
the topic in consumer batches of 100, rebuilds dbt, and prints
mart_ingestion_health_daily. The producer prints its own publish rate.

Measurements to record:

| Metric | Source |
| --- | --- |
| Producer throughput (events/s) | loadgen output |
| Batches and batch duration (avg/max ms) | mart_ingestion_health_daily |
| Rejections | mart_ingestion_health_daily.records_rejected |
| End-to-end freshness | mart_event_metrics_daily.max_ingestion_lag_seconds |
| Consumer lag while draining | Grafana "Consumer lag" panel |
| dbt build time | dbt log / `dbt build` output |

## Known overheads

- Each run is a separate process: one consumer group join and one Iceberg
  catalog load, roughly a second. A run drains the topic until polls come
  back empty or the time budget (4 minutes by default) expires, so join cost
  is paid per schedule tick, not per hundred messages. A resident consumer
  would remove it entirely; the batch model was chosen for Airflow-native
  retries (ADR-001).
- Every run writes at least one data file per touched day partition. Small
  files accumulate until snapshot expiry; production deployments should add
  Spark rewrite_data_files (see ADR-005).

## Tuning levers

| Lever | Where | Effect |
| --- | --- | --- |
| time_budget_seconds / idle_polls | run_sync() defaults | How long a run drains vs Airflow execution timeout |
| DAG schedule | kafka_to_iceberg_dag.py | Freshness vs number of runs |
| Topic partitions | Kafka | Parallelism (consumer is single-threaded today) |
| Partitioning + snapshot retention | consumer / maintenance DAG | File count, metadata size |
| dbt threads | dbt/profiles.yml | Transform parallelism |

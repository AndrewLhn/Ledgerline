with expected as (
    select
        cast(event_occurred_at as date) as metric_date,
        count(*) as event_count,
        count(distinct event_hash) as distinct_payload_count,
        count(distinct source_event_id) as distinct_source_event_count,
        max(ingestion_lag_seconds) as max_ingestion_lag_seconds,
        count_if(ingestion_lag_seconds > 3600) as late_event_count
    from {{ ref('fct_kafka_events') }}
    group by 1
)

select
    metrics.metric_date,
    metrics.event_count,
    metrics.distinct_payload_count,
    metrics.distinct_source_event_count
from {{ ref('mart_event_metrics_daily') }} as metrics
full outer join expected
    on metrics.metric_date = expected.metric_date
where metrics.event_count is distinct from expected.event_count
   or metrics.distinct_payload_count is distinct from expected.distinct_payload_count
   or metrics.distinct_source_event_count is distinct from expected.distinct_source_event_count
   or metrics.max_ingestion_lag_seconds is distinct from expected.max_ingestion_lag_seconds
   or metrics.late_event_count is distinct from expected.late_event_count

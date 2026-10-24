select
    cast(event_occurred_at as date) as metric_date,
    count(*) as event_count,
    count(distinct event_hash) as distinct_payload_count,
    count(distinct source_event_id) as distinct_source_event_count,
    max(ingestion_lag_seconds) as max_ingestion_lag_seconds,
    count_if(ingestion_lag_seconds > 3600) as late_event_count
from {{ ref('fct_kafka_events') }}
group by 1

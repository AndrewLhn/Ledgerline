select
    cast(batch_started_at as date) as metric_date,
    count(*) as batch_count,
    sum(records_read) as records_read,
    sum(records_written) as records_written,
    sum(records_rejected) as records_rejected,
    max(batch_duration_ms) as max_batch_duration_ms,
    cast(round(avg(batch_duration_ms)) as bigint) as avg_batch_duration_ms
from {{ ref('stg_ingestion_runs') }}
group by 1

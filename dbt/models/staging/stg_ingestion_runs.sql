select
    run_id,
    cast(started_at as timestamp) as batch_started_at,
    cast(finished_at as timestamp) as batch_finished_at,
    date_diff(
        'millisecond',
        cast(started_at as timestamp),
        cast(finished_at as timestamp)
    ) as batch_duration_ms,
    records_read,
    records_written,
    records_rejected
from {{ source('raw', 'ingestion_runs') }}

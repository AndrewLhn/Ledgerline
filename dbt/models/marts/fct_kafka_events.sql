select
    source_event_id,
    event_type,
    schema_version,
    event_occurred_at,
    event_ingested_at,
    ingestion_lag_seconds,
    source_system,
    correlation_id,
    event_hash,
    event_payload,
    kafka_partition,
    kafka_offset,
    current_timestamp as transformed_at
from {{ ref('stg_kafka_events') }}

with source_events as (
    select
        event_id as source_event_id,
        event_type,
        cast(schema_version as integer) as schema_version,
        cast(from_iso8601_timestamp(occurred_at) as timestamp) as event_occurred_at,
        cast(ingested_at as timestamp) as event_ingested_at,
        source_system,
        correlation_id,
        trim(data) as event_payload,
        cast(kafka_partition as integer) as kafka_partition,
        cast(kafka_offset as bigint) as kafka_offset
    from {{ source('raw', 'ledgerline_events') }}
),
deduplicated_events as (
    select
        *,
        row_number() over (
            partition by source_event_id
            order by kafka_partition desc, kafka_offset desc
        ) as row_number
    from source_events
)

select
    source_event_id,
    event_type,
    schema_version,
    event_occurred_at,
    event_ingested_at,
    date_diff('second', event_occurred_at, event_ingested_at) as ingestion_lag_seconds,
    source_system,
    correlation_id,
    event_payload,
    kafka_partition,
    kafka_offset,
    md5(concat(source_event_id, '|', event_payload)) as event_hash
from deduplicated_events
where row_number = 1

with parsed_events as (
    select
        *,
        try(json_parse(event_payload)) as event_data
    from {{ ref('fct_kafka_events') }}
),
typed_events as (
    select
        *,
        try_cast(json_extract_scalar(event_data, '$.amount_minor') as bigint) as amount_minor
    from parsed_events
)

select
    source_event_id,
    event_type,
    event_occurred_at,
    event_ingested_at,
    source_system,
    correlation_id,
    json_extract_scalar(event_data, '$.order_id') as order_id,
    json_extract_scalar(event_data, '$.payment_id') as payment_id,
    json_extract_scalar(event_data, '$.refund_id') as refund_id,
    json_extract_scalar(event_data, '$.product_id') as product_id,
    json_extract_scalar(event_data, '$.location_id') as location_id,
    json_extract_scalar(event_data, '$.currency') as currency,
    amount_minor,
    cast(cast(amount_minor as decimal(18, 2)) / 100 as decimal(18, 2)) as amount,
    try_cast(json_extract_scalar(event_data, '$.quantity_delta') as integer) as quantity_delta,
    event_hash,
    event_payload
from typed_events

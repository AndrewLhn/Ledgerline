-- The contract registers schema versions 1 and 2; anything else reaching the
-- fact layer would mean edge validation stopped working. dbt's
-- accepted_values quotes its list, which Trino cannot compare to an integer
-- column, so the check lives here instead.
select
    source_event_id,
    schema_version
from {{ ref('fct_kafka_events') }}
where schema_version not in (1, 2)

-- Freshness SLO (docs/business-context.md): an event must be queryable within
-- six hours of occurrence. Negative lag (clock skew) is a violation too.
select
    metric_date,
    max_ingestion_lag_seconds
from {{ ref('mart_event_metrics_daily') }}
where max_ingestion_lag_seconds > 21600
   or max_ingestion_lag_seconds < 0

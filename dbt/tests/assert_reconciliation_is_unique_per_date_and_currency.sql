select
    metric_date,
    currency
from {{ ref('mart_daily_reconciliation') }}
group by metric_date, currency
having count(*) > 1

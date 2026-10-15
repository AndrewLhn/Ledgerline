with daily_financial_events as (
    select
        cast(event_occurred_at as date) as metric_date,
        currency,
        coalesce(
            sum(case when event_type = 'order_created' then amount end),
            cast(0 as decimal(18, 2))
        ) as ordered_amount,
        coalesce(
            sum(case when event_type = 'payment_captured' then amount end),
            cast(0 as decimal(18, 2))
        ) as captured_payment_amount,
        coalesce(
            sum(case when event_type = 'refund_issued' then amount end),
            cast(0 as decimal(18, 2))
        ) as refunded_amount
    from {{ ref('fct_ledgerline_events') }}
    where event_type in ('order_created', 'payment_captured', 'refund_issued')
    group by 1, 2
),
reconciled as (
    select
        metric_date,
        currency,
        ordered_amount,
        captured_payment_amount,
        refunded_amount,
        captured_payment_amount - refunded_amount as net_captured_cash,
        ordered_amount - refunded_amount as recognised_revenue
    from daily_financial_events
)

select
    *,
    net_captured_cash - recognised_revenue as variance_amount,
    case
        when abs(net_captured_cash - recognised_revenue) <= cast(0.01 as decimal(18, 2))
            then 'passed'
        else 'failed'
    end as reconciliation_status
from reconciled

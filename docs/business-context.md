# Domain and reconciliation rules

## Events

| Event | Required business fields |
| --- | --- |
| order_created | order_id, customer_id, channel, currency, amount_minor |
| payment_captured | payment_id, order_id, currency, amount_minor |
| refund_issued | refund_id, payment_id, order_id, currency, amount_minor |
| inventory_adjusted | adjustment_id, product_id, location_id, quantity_delta |

The schema accepts version 1 only. Breaking changes require a new version.

Monetary amounts travel as integer minor units (cents) and are stored that way
in the fact table (`amount_minor`). Marts expose `amount` as decimal(18,2) for
reporting. Floats never participate in financial sums. See ADR-004.

## Reconciliation

The current mart operates by business date:

~~~text
net_captured_cash = captured_payment_amount - refunded_amount
recognised_revenue = ordered_amount - refunded_amount
variance_amount = net_captured_cash - recognised_revenue
~~~

A variance above EUR 0.01 sets reconciliation_status to failed and fails the
dbt test.

The sample scenario is intentionally small:

~~~text
orders:   EUR 100.00
payments: EUR 100.00
refunds:  EUR  20.00
result:   EUR  80.00 net cash and EUR  80.00 revenue
~~~

## Freshness

An event must be queryable within six hours of occurrence. Staging records
`ingestion_lag_seconds` per event, the daily metrics mart reports the maximum
lag and counts events more than one hour late, and a dbt test fails the build
when the SLO is breached.

## Data rules

- source event IDs are immutable and used as Kafka message keys;
- occurrence time and ingestion time are separate fields;
- raw data may contain duplicates after replay;
- transformed facts are deduplicated by source event ID;
- invalid events are retained in the DLQ and can be replayed after a fix;
- all data in the repository is synthetic.

## Not modelled

This project does not model tax, partial fulfilment, chargebacks, inventory
valuation, multi-currency conversion, or accounting-grade revenue recognition.

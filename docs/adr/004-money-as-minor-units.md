# ADR 004: Money as integer minor units

- Status: Accepted
- Date: 2026-10-24

## Context

The original contract typed monetary amounts as JSON `number`. JSON numbers are
IEEE-754 doubles in virtually every parser, so sums accumulate binary rounding
error. A financial reconciliation gate with a 0.01 threshold should not depend
on float behaviour.

## Options considered

- Keep float, round in dbt: works for small demos, hides the problem, and
  every consumer must remember to round.
- Decimal string in JSON: exact, but awkward to aggregate and easy to
  mis-parse.
- Integer minor units (cents): exact, trivially validated (`type: integer`),
  standard practice in payment APIs.

## Decision

Contract v1 requires `amount_minor` as a positive integer in the currency's
minor units. The fact table stores `amount_minor` and exposes a derived
`amount decimal(18,2)` for reporting. All sums happen on decimals derived from
integers.

The change landed before any external consumer existed, so it did not require
a schema version bump; the first released version of the contract carries it.

## Consequences

Producers must send integers; a float `amount_minor` fails contract validation
and lands in the DLQ. Multi-currency stays out of scope: `currency` is carried
per event, but reconciliation assumes a single currency per business date.
Exchange rates would be a separate domain decision.

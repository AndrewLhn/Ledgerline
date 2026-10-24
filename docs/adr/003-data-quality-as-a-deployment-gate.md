# ADR 003: Quality checks block reconciliation output

- Status: Accepted
- Date: 2026-10-08, amended 2026-10-24

## Context

A completed task does not prove that data is valid or reconciled. Consumers of
the reconciliation mart need a gate that stops bad numbers from looking
official.

## Options considered

- Alerting only (checks run, failures notify): fast, but bad data still lands
  in marts between alert and fix.
- External data-quality platform (Great Expectations, Soda): more features,
  another runtime dependency for a local stack.
- dbt tests as build gates plus contract validation at the edge: one tool,
  runs everywhere dbt runs, failures block the model build.

## Decision

Run contract tests at ingestion, dbt tests on every build, unit tests and lint
in CI, and an E2E workflow that asserts end-state counts. A reconciliation
variance above the threshold, a contract violation, a duplicate fact, or a
freshness SLO breach fails the build.

Amendment: freshness is enforced as a test too — events must be queryable
within six hours of occurrence (assert_event_freshness_within_slo).

## Consequences

Invalid events go to the DLQ and can be replayed after a fix
(scripts/replay_dlq.py). Quality failures are visible in CI and in `dbt build`
output rather than discovered by downstream consumers. The gate is only as
good as the tests, so new failure modes must come with a new test (runbook
rule).

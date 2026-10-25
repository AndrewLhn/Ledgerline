#!/usr/bin/env bash
# End-to-end pipeline check: producer -> Kafka -> Airflow consumer -> Iceberg -> dbt marts.
# Asserts absolute row counts, so it always starts from empty volumes.
set -euo pipefail

compose=(docker compose)

log() { printf '\n=== %s ===\n' "$1"; }

fail() {
  echo "E2E FAILED: $1" >&2
  "${compose[@]}" ps >&2 || true
  "${compose[@]}" logs --tail=100 >&2 || true
  exit 1
}

check_eq() {
  local label="$1" actual="$2" expected="$3"
  if [[ "$actual" != "$expected" ]]; then
    fail "$label: expected '$expected', got '$actual'"
  fi
  echo "OK: $label = $actual"
}

query_scalar() {
  # Trino CLI quotes CSV fields; strip the surrounding quotes so values
  # compare as plain strings in check_eq.
  "${compose[@]}" exec -T trino trino --output-format CSV_HEADER --execute "$1" \
    | tail -n 1 | tr -d "\r" | sed -e 's/^"//' -e 's/"$//'
}

log "Resetting previous stack state (volumes included)"
"${compose[@]}" down -v --remove-orphans

log "Starting the stack"
# --wait blocks until every healthchecked service (including airflow-standalone) is ready.
# --wait-timeout makes a stuck service fail fast, with its name in the error,
# instead of letting the job idle until the workflow timeout cancels it.
"${compose[@]}" up -d --build --wait --wait-timeout 420

log "Waiting for Airflow to parse the DAG"
dag_ready=""
for _ in $(seq 1 60); do
  if "${compose[@]}" exec -T airflow-standalone airflow dags list 2>/dev/null | grep -q "kafka_to_iceberg"; then
    dag_ready="1"
    break
  fi
  sleep 5
done
[[ -n "$dag_ready" ]] || fail "DAG kafka_to_iceberg never showed up in 'airflow dags list'"

log "Publishing synthetic events"
python3 infra/producer.py --scenario ledgerline
python3 infra/producer.py --scenario duplicate
python3 infra/producer.py --scenario invalid

log "Running the ingestion DAG once"
"${compose[@]}" exec -T airflow-standalone airflow dags test kafka_to_iceberg

log "Building dbt models and tests"
"${compose[@]}" --profile tools run --rm dbt build

log "Asserting pipeline results"
check_eq "raw_event_rows" \
  "$(query_scalar "SELECT count(*) FROM iceberg.ledgerline_raw.events")" "5"
check_eq "deduplicated_fact_rows" \
  "$(query_scalar "SELECT count(*) FROM iceberg.analytics.fct_kafka_events")" "4"
check_eq "reconciliation_status" \
  "$(query_scalar "SELECT reconciliation_status FROM iceberg.analytics.mart_daily_reconciliation")" "passed"
check_eq "audit_records_written" \
  "$(query_scalar "SELECT sum(records_written) FROM iceberg.analytics.stg_ingestion_runs")" "5"
check_eq "audit_records_rejected" \
  "$(query_scalar "SELECT sum(records_rejected) FROM iceberg.analytics.stg_ingestion_runs")" "1"

# kafka-console-consumer exits 1 on --timeout-ms expiry, which is the normal path here.
dlq_output="$("${compose[@]}" exec -T kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ledgerline_events_dlq \
  --from-beginning \
  --timeout-ms 15000 2>/dev/null || true)"
dlq_invalid_count="$(printf '%s\n' "$dlq_output" | grep -c '"error"' || true)"
check_eq "dlq_invalid_events" "$dlq_invalid_count" "1"

log "E2E passed"

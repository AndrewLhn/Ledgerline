COMPOSE := docker compose

.PHONY: up down logs build validate lint test dbt-build dbt-compile dbt-test e2e demo bench replay-dlq monitoring-up

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

build:
	$(COMPOSE) build

validate:
	$(COMPOSE) config --quiet
	python3 -m compileall -q dags infra scripts tests

lint:
	ruff check dags infra scripts tests

test:
	python3 -m unittest discover -s tests -v

dbt-build:
	$(COMPOSE) --profile tools run --rm dbt build

dbt-compile:
	$(COMPOSE) --profile tools run --rm dbt compile

dbt-test:
	$(COMPOSE) --profile tools run --rm dbt test

e2e:
	bash scripts/e2e.sh

demo:
	python3 infra/producer.py --scenario ledgerline
	$(COMPOSE) exec -T airflow-standalone airflow dags test kafka_to_iceberg
	$(COMPOSE) --profile tools run --rm dbt build
	$(COMPOSE) exec -T trino trino --output-format=ALIGNED --execute "SELECT * FROM iceberg.analytics.mart_daily_reconciliation"

# Load test: publish order chains, drain them, rebuild, show health.
# Each consumer run drains the topic within its time budget; a few runs
# absorb whatever the first one left behind.
# Override volume with ORDERS=20000 make bench
bench:
	python3 scripts/loadgen.py --orders $${ORDERS:-5000}
	for i in 1 2 3; do \
		$(COMPOSE) exec -T airflow-standalone env -u PYTHONPATH -u LD_PRELOAD PYTHONNOUSERSITE=1 \
			/home/airflow/iceberg_env/bin/python /opt/airflow/dags/kafka_to_iceberg_run.py; \
	done
	$(COMPOSE) --profile tools run --rm dbt build
	$(COMPOSE) exec -T trino trino --output-format=ALIGNED --execute "SELECT * FROM iceberg.analytics.mart_ingestion_health_daily"

replay-dlq:
	python3 scripts/replay_dlq.py --apply

monitoring-up:
	$(COMPOSE) --profile monitoring up -d

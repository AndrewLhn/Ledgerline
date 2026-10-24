#!/bin/sh
# Idempotent bootstrap of the Iceberg catalog database and its roles.
# Runs on every `docker compose up` via the iceberg-db-init service.
#
#   iceberg_login  owns the catalog DB; used by pyiceberg in the consumer
#   trino_login    used by Trino's JDBC catalog; may read/write catalog tables
#
# The airflow superuser role stays confined to the Airflow metadata DB; it is
# still a PostgreSQL superuser inside this container, which is an accepted
# limitation of the single-node local stack (see docs/security.md).
set -eu

until pg_isready -h "${PGHOST:-airflow-db}" -d postgres >/dev/null 2>&1; do
  sleep 2
done

psql -v ON_ERROR_STOP=1 -d postgres <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${ICEBERG_DB_USER}') THEN
    CREATE ROLE "${ICEBERG_DB_USER}" LOGIN PASSWORD '${ICEBERG_DB_PASSWORD}';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${TRINO_DB_USER}') THEN
    CREATE ROLE "${TRINO_DB_USER}" LOGIN PASSWORD '${TRINO_DB_PASSWORD}';
  END IF;
END
\$\$;
SQL

if ! psql -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '${ICEBERG_CATALOG_DB}'" | grep -q 1; then
  psql -v ON_ERROR_STOP=1 -d postgres -c \
    "CREATE DATABASE \"${ICEBERG_CATALOG_DB}\" OWNER \"${ICEBERG_DB_USER}\""
fi

psql -v ON_ERROR_STOP=1 -d "${ICEBERG_CATALOG_DB}" <<SQL
GRANT CONNECT, CREATE ON DATABASE "${ICEBERG_CATALOG_DB}" TO "${TRINO_DB_USER}";
GRANT USAGE, CREATE ON SCHEMA public TO "${TRINO_DB_USER}";
GRANT ALL ON ALL TABLES IN SCHEMA public TO "${TRINO_DB_USER}";
ALTER DEFAULT PRIVILEGES FOR ROLE "${ICEBERG_DB_USER}" IN SCHEMA public
  GRANT ALL ON TABLES TO "${TRINO_DB_USER}";
ALTER DEFAULT PRIVILEGES FOR ROLE "${ICEBERG_DB_USER}" IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO "${TRINO_DB_USER}";
SQL

#!/bin/sh
# Idempotent bootstrap of the Iceberg catalog database and its roles.
# Runs on every `docker compose up` via the iceberg-db-init service.
#
#   catalog_owner  group role owning every catalog object. Trino's JDBC
#                  catalog runs schema migrations (DDL) on the registry
#                  tables, and pyiceberg creates them, so ownership must be
#                  shared; logins stay separate for auditability:
#                    iceberg  - pyiceberg in the consumer
#                    trino    - Trino JDBC catalog
#                  Both logins SET ROLE to catalog_owner inside this
#                  database (see docs/security.md, ADR-007).
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
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'catalog_owner') THEN
    CREATE ROLE catalog_owner;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${ICEBERG_DB_USER}') THEN
    CREATE ROLE "${ICEBERG_DB_USER}" LOGIN PASSWORD '${ICEBERG_DB_PASSWORD}';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${TRINO_DB_USER}') THEN
    CREATE ROLE "${TRINO_DB_USER}" LOGIN PASSWORD '${TRINO_DB_PASSWORD}';
  END IF;
END
\$\$;
GRANT catalog_owner TO "${ICEBERG_DB_USER}";
GRANT catalog_owner TO "${TRINO_DB_USER}";
SQL

if ! psql -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '${ICEBERG_CATALOG_DB}'" | grep -q 1; then
  psql -v ON_ERROR_STOP=1 -d postgres -c \
    "CREATE DATABASE \"${ICEBERG_CATALOG_DB}\" OWNER catalog_owner"
fi

psql -v ON_ERROR_STOP=1 -d "${ICEBERG_CATALOG_DB}" <<SQL
ALTER DATABASE "${ICEBERG_CATALOG_DB}" OWNER TO catalog_owner;
REASSIGN OWNED BY "${ICEBERG_DB_USER}" TO catalog_owner;
REASSIGN OWNED BY "${TRINO_DB_USER}" TO catalog_owner;
GRANT CONNECT ON DATABASE "${ICEBERG_CATALOG_DB}" TO "${ICEBERG_DB_USER}", "${TRINO_DB_USER}";
ALTER ROLE "${ICEBERG_DB_USER}" IN DATABASE "${ICEBERG_CATALOG_DB}" SET role = 'catalog_owner';
ALTER ROLE "${TRINO_DB_USER}" IN DATABASE "${ICEBERG_CATALOG_DB}" SET role = 'catalog_owner';
SQL

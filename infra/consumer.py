"""Run the Kafka-to-Iceberg batch consumer on the host against the Compose stack.

Prerequisites:
    docker compose up -d
    python3 -m pip install -r infra/requirements.txt

Connection defaults match .env.example (PostgreSQL is exposed on 127.0.0.1:5432,
MinIO on 127.0.0.1:9000, Kafka on localhost:9092). If you changed credentials in
.env, export the same values (ICEBERG_DB_USER, ICEBERG_DB_PASSWORD,
ICEBERG_CATALOG_DB, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD) before running this
script.
"""

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
# dags/ must be importable as a plain directory: kafka_to_iceberg_run imports
# ledgerline_contract as a top-level module, exactly as it does inside Airflow.
for _path in (PROJECT_ROOT, PROJECT_ROOT / "dags"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

_postgres_user = os.getenv("ICEBERG_DB_USER", "iceberg")
_postgres_password = os.getenv("ICEBERG_DB_PASSWORD", "change-this-iceberg-password")
_iceberg_catalog_db = os.getenv("ICEBERG_CATALOG_DB", "iceberg_metadata")

# kafka_to_iceberg_run reads its connection settings at import time,
# so every default must be in place before the import below.
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault(
    "ICEBERG_CATALOG_URI",
    f"postgresql+psycopg2://{_postgres_user}:{_postgres_password}"
    f"@127.0.0.1:5432/{_iceberg_catalog_db}",
)
os.environ.setdefault("ICEBERG_WAREHOUSE", "s3://warehouse/")
os.environ.setdefault("MINIO_ENDPOINT", "http://127.0.0.1:9000")
os.environ.setdefault("MINIO_ROOT_USER", os.getenv("MINIO_ROOT_USER", "minioadmin"))
os.environ.setdefault(
    "MINIO_ROOT_PASSWORD", os.getenv("MINIO_ROOT_PASSWORD", "change-this-minio-password")
)

from dags.kafka_to_iceberg_run import run_sync  # noqa: E402

if __name__ == "__main__":
    run_sync()

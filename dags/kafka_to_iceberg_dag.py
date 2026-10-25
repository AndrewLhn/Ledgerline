from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator

from airflow import DAG

with DAG(
    dag_id="kafka_to_iceberg",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=5),
    },
    tags=["kafka", "iceberg"],
) as dag:
    BashOperator(
        task_id="consume_kafka_batch",
        bash_command=(
            "env -u PYTHONPATH -u LD_PRELOAD PYTHONNOUSERSITE=1 "
            "/home/airflow/iceberg_env/bin/python "
            "/opt/airflow/dags/kafka_to_iceberg_run.py"
        ),
    )

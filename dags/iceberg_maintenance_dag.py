from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator

from airflow import DAG

with DAG(
    dag_id="iceberg_maintenance",
    start_date=datetime(2025, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["iceberg", "maintenance"],
) as dag:
    BashOperator(
        task_id="expire_snapshots",
        bash_command=(
            "env -u PYTHONPATH -u LD_PRELOAD PYTHONNOUSERSITE=1 "
            "/home/airflow/iceberg_env/bin/python "
            "/opt/airflow/dags/iceberg_maintenance_run.py"
        ),
    )

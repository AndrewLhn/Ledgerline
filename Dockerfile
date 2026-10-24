FROM apache/airflow:3.3.2-python3.12

USER airflow

# Keep Iceberg dependencies separate from Airflow's own Python environment.
# infra/requirements.txt is the single source of truth for these pins; dependabot
# tracks it (pip) and the base image tag above (docker).
COPY infra/requirements.txt /tmp/iceberg-requirements.txt
RUN python -m venv /home/airflow/iceberg_env && \
    /home/airflow/iceberg_env/bin/pip install --no-cache-dir -r /tmp/iceberg-requirements.txt && \
    rm /tmp/iceberg-requirements.txt

COPY contracts /opt/airflow/contracts

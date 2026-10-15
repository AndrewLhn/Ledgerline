FROM apache/airflow:2.10.0

USER airflow

# Keep Iceberg dependencies separate from Airflow's own Python environment.
RUN python -m venv /home/airflow/iceberg_env && \
    /home/airflow/iceberg_env/bin/pip install --no-cache-dir \
      confluent-kafka==2.12.2 \
      jsonschema==4.25.1 \
      rfc3339-validator==0.1.4 \
      numpy==1.26.4 \
      pyarrow==14.0.1 \
      "pyiceberg[sql-postgres,pyarrow]==0.6.0" \
      psycopg2-binary==2.9.9

COPY contracts /opt/airflow/contracts

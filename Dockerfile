FROM apache/airflow:2.10.5-python3.12

USER airflow

RUN pip install --no-cache-dir airflow-clickhouse-plugin==1.4.0
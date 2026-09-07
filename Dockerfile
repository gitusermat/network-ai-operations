FROM apache/airflow:3.3.1

USER airflow

COPY dags /opt/airflow/dags
FROM apache/airflow:2.7.3-python3.11

ARG AIRFLOW_HOME=/opt/airflow

ENV AIRFLOW_HOME=${AIRFLOW_HOME}

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN chown -R airflow: ${AIRFLOW_HOME}

USER airflow

COPY bucket/requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
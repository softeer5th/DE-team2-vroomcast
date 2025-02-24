# Airflow 이미지 (MWAA와 동일한 버전 사용)
FROM apache/airflow:2.10.3-python3.11

# Airflow Home (빌드 시 주입)
ARG AIRFLOW_HOME=/opt/airflow

# 주입된 Airflow Home을 환경 변수로 설정
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

# 루트 권한으로 진행
USER root

# 기본 패키지 설치
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Airflow Home 소유자 변경
RUN chown -R airflow: ${AIRFLOW_HOME}

# airflow 사용자로 전환
USER airflow

# requirements.txt 복사
COPY bucket/requirements.txt /requirements.txt

# 파이썬 패키지 설치
RUN pip install --no-cache-dir -r /requirements.txt
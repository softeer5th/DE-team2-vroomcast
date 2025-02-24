import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from modules.constants import S3_CONFIG_BUCKET


def _read_config_from_s3(**context) -> dict:
    """
    S3에서 config 파일을 읽어옵니다.
    Args:
        **context: Airflow context
    Returns:
        dict: config 파일
    """
    s3_hook = S3Hook()
    car_config = s3_hook.read_key(key="airflow/car.json", bucket_name=S3_CONFIG_BUCKET)
    community_config = s3_hook.read_key(
        key="airflow/community.json", bucket_name=S3_CONFIG_BUCKET
    )

    config = {"car": json.loads(car_config), "community": json.loads(community_config)}

    return config


def create_read_config_task(dag: DAG) -> PythonOperator:
    """
    S3에서 config 파일을 읽는 Task를 생성합니다.
    Args:
        dag (DAG): Airflow DAG
    Returns:
        PythonOperator: Task
    """
    return PythonOperator(
        task_id="read_config", python_callable=_read_config_from_s3, dag=dag
    )

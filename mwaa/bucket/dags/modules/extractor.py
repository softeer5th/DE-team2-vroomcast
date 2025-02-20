import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from botocore.config import Config
from modules.operators import LambdaInvokeFunctionOperator
from modules.constants import S3_BUCKET


def create_extract_task(
    dag: DAG,
    community: str,
    car_id: str,
    keywords: list[str],
    date: str,
    batch: int,
    start_datetime: str,
    end_datetime: str,
) -> LambdaInvokeFunctionOperator:
    """Extract 태스크를 생성하는 함수"""
    return LambdaInvokeFunctionOperator(
        task_id=f"extract_{car_id}_{community}",
        function_name=f"vroomcast-lambda-extract-{community}",
        payload=json.dumps(
            {
                "bucket": S3_BUCKET,
                "car_id": car_id,
                "keywords": keywords,
                "date": date,
                "batch": batch,
                "start_datetime": start_datetime,
                "end_datetime": end_datetime,
            }
        ),
        dag=dag,
    )
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.lambda_function import \
    LambdaInvokeFunctionOperator
from modules.constants import S3_BUCKET


def create_extract_task(
    dag: DAG,
    community: str,
    car_id: str,
    keywords: list[str],
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
                "date": "{{ ds }}",
                "batch": batch,
                "start_datetime": start_datetime,
                "end_datetime": end_datetime,
            }
        ),
        dag=dag,
    )


def create_combine_task(dag: DAG, car_id: str) -> LambdaInvokeFunctionOperator:
    """Combine 태스크를 생성하는 함수"""
    return LambdaInvokeFunctionOperator(
        task_id=f"combine_{car_id}",
        function_name=f"vroomcast-lambda-combine",
        payload=json.dumps({"bucket": S3_BUCKET, "car_id": car_id, "date": "{{ ds }}"}),
        dag=dag,
    )

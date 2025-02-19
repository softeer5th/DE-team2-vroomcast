from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

from modules.constants import S3_BUCKET

import json

def create_combine_task(dag: DAG, car_id: str, batch_datetime: str) -> LambdaInvokeFunctionOperator:
    """Combine 태스크를 생성하는 함수"""
    return LambdaInvokeFunctionOperator(
        task_id=f"combine_{car_id}",
        function_name=f"vroomcast-lambda-combine",
        payload=json.dumps({"bucket": S3_BUCKET, "car_id": car_id, "date": "{{ ds }}", "batch_datetime": batch_datetime}),
        dag=dag,
    )

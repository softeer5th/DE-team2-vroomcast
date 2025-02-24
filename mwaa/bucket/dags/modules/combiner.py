import json

from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import \
    LambdaInvokeFunctionOperator

from modules.constants import S3_BUCKET


def create_combine_task(
    dag: DAG, car_id: str, date: str, batch: int, batch_datetime: str
) -> LambdaInvokeFunctionOperator:
    """
    추출된 JSON을 Parquet로 병합하는 Lambda를 호출하는 Task를 생성합니다.
    Args:
        dag (DAG): Airflow DAG
        car_id (str): 차량 ID
        date (str): 날짜
        batch (int): 배치
        batch_datetime (str): 배치 시각
    Returns:
        LambdaInvokeFunctionOperator: Task
    """
    return LambdaInvokeFunctionOperator(
        task_id=f"combine_{car_id}",
        function_name=f"vroomcast-lambda-combine",
        payload=json.dumps(
            {
                "bucket": S3_BUCKET,
                "car_id": car_id,
                "date": date,
                "batch": batch,
                "batch_datetime": batch_datetime,
            }
        ),
        dag=dag,
    )

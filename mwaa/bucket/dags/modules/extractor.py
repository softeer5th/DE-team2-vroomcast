import json
from datetime import timedelta

from airflow import DAG

from modules.constants import S3_BUCKET
from modules.operators import LambdaInvokeFunctionOperator


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
    """
    커뮤니티 반응을 추출하는 Lambda를 호출하는 Task를 생성합니다.
    Args:
        dag (DAG): Airflow DAG
        community (str): 커뮤니티
        car_id (str): 차량 ID
        keywords (list[str]): 키워드
        date (str): 날짜
        batch (int): 배치
        start_datetime (str): 시작 시각
        end_datetime (str): 종료 시각
    Returns:
        LambdaInvokeFunctionOperator: Task
    """
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
        execution_timeout=timedelta(minutes=14),
        dag=dag,
    )

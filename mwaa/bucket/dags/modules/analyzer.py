import json

from airflow import DAG

from modules.constants import S3_BUCKET
from modules.operators import LambdaInvokeFunctionOperator


def create_analyze_sentiment_task(
    dag: DAG,
    date: str,
    batch: int,
) -> LambdaInvokeFunctionOperator:
    """
    감성 분석을 수행하는 Lambda를 호출하는 Task를 생성합니다.
    Args:
        dag (DAG): Airflow DAG
        date (str): 날짜
        batch (int): 배치
    Returns:
        LambdaInvokeFunctionOperator: Task
    """
    return LambdaInvokeFunctionOperator(
        task_id=f"analyze_sentiment",
        function_name=f"vroomcast-lambda-sentiment",
        payload=json.dumps(
            {
                "bucket_name": S3_BUCKET,
                "input_dir": f"transformed/{date}/{batch}/sentence/",
                "output_dir": f"transformed/{date}/{batch}/sentence_sentiment/",
            }
        ),
        dag=dag,
    )

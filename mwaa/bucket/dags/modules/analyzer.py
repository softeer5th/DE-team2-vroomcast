import json

from airflow import DAG
from modules.operators import LambdaInvokeFunctionOperator

from modules.constants import S3_BUCKET

def create_analyze_sentiment_task(
    dag: DAG,
    date: str,
    batch: int,
) -> LambdaInvokeFunctionOperator:
    """Extract 태스크를 생성하는 함수"""
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
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from datetime import datetime, timedelta

import os
import json

extract_bucket = os.environ.get('MWAA_S3_EXTRACT_BUCKET')
transform_bucket = os.environ.get('MWAA_S3_TRANSFORM_BUCKET')

def get_keywords():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(current_dir, 'keywords.json')
    with open(json_path, 'r') as f:
        return json.load(f)
    
def get_community():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(current_dir, 'community.json')
    with open(json_path, 'r') as f:
        return json.load(f)

keywords = get_keywords()
community = get_community()

default_args = {
    'owner': 'vroomcast', # 작성자
    'depends_on_past': False, # 이전 실행이 성공했는지 확인
    'start_date': datetime(2024, 2, 10), # workflow 시작 날짜
    'email_on_failure': True, # 실패 시 이메일 발송
    'email_on_retry': True, # 재시도 시 이메일 발송
    'retries': 10, # 실패 시 재시도 횟수
    'retry_delay': timedelta(minutes=10) # 재시도 간격
}

# DAG 정의
dag = DAG(
    'vroomcast_workflow', # DAG 이름
    default_args=default_args, # 기본 인수
    description='ETL workflow for Vroomcast', # DAG 설명
    schedule_interval=timedelta(days=1), # DAG 실행 주기
    catchup=False # 과거 DAG 실행 여부 (backfill)
)

def create_extract_task(community: str, car_id: str, keyword: str, date: str) -> LambdaInvokeFunctionOperator:
    """Extract 태스크를 생성하는 함수"""
    return LambdaInvokeFunctionOperator(
        task_id=f'extract_{community}_{car_id}_{keyword}',
        function_name='vroomcast-lambda-extract-{community}',
        payload={
            "bucket": extract_bucket,
            "keywords": [keyword],
            "car_id": car_id,
            "date": date
        },
        dag=dag
    )

def create_transform_task(community: str, car_id: str, date: str) -> LambdaInvokeFunctionOperator:
    """Transform 태스크를 생성하는 함수"""
    return LambdaInvokeFunctionOperator(
        task_id=f'transform_{community}_{car_id}',
        function_name='vroomcast-lambda-transform-{community}',
        payload={
            "bucket": transform_bucket,
            "car_id": car_id,
            "date": date
        },
        dag=dag
    )
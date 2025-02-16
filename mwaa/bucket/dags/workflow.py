import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.lambda_function import \
    LambdaInvokeFunctionOperator


import os


# AIRFLOW_VAR_로 시작하는 환경 변수는 자동으로 Airflow Variable이 됨
S3_BUCKET = Variable.get("S3_BUCKET")

# 설정 파일 경로
CONFIG_PATH = os.path.join(os.getenv("AIRFLOW_HOME"), "configs")


def load_config(filename: str) -> dict:
    """설정 파일을 로드하는 함수"""
    try:
        with open(f"{CONFIG_PATH}/{filename}", "r") as f:
            return json.load(f)
    except Exception as e:
        raise Exception(f"Failed to load config file {filename}: {str(e)}")


# 설정 파일 로드
CARS = {item["car_id"]: item["keywords"] for item in load_config("car.json")}
COMMUNITIES = load_config("community.json")

default_args = {
    "owner": "vroomcast",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 10, 2, 0, 0),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 10,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "vroomcast_workflow",
    default_args=default_args,
    description="Data pipeline workflow for Vroomcast",
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    def create_extract_task(
        community: str, car_id: str, keywords: list[str]
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
                }
            ),
            dag=dag,
        )

    def create_combine_task(car_id: str) -> LambdaInvokeFunctionOperator:
        """Combine 태스크를 생성하는 함수"""
        return LambdaInvokeFunctionOperator(
            task_id=f"combine_{car_id}",
            function_name=f"vroomcast-lambda-combine",
            payload=json.dumps(
                {"bucket": S3_BUCKET, "car_id": car_id, "date": "{{ ds }}"}
            ),
            dag=dag,
        )

    # DAG 내부에서 테스크 생성
    extract_tasks = []
    combine_tasks = []

    # 각 차종에 대해 Extract 태스크와 Combine 태스크 생성
    for car_info in CARS.items():
        car_id = car_info[0]
        keywords = car_info[1]

        # 특정 차종에 대해 커뮤니티마다 수행되는 Extract 태스크 생성
        car_extract_tasks = [
            create_extract_task(community, car_id, keywords)
            for community in COMMUNITIES
        ]

        extract_tasks.extend(car_extract_tasks)

        # Combine 태스크 생성
        combine_task = create_combine_task(car_id)
        combine_tasks.append(combine_task)

        # 하나의 자동차에 대한 Extract 태스크가 모두 완료되면 Combine 태스크 실행
        car_extract_tasks >> combine_task

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def print_params(**context):
    print(f"bucket: {context['params']['bucket']}")
    print(f"input_path: {context['params']['input_path']}")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 19),
    "params": {  # DAG 레벨의 기본 params 설정
        "bucket": "default-bucket",
        "input_path": "default/path",
    },
}

with DAG(
    "params_example",
    default_args=default_args,
    schedule_interval=None,  # manually triggered only
) as dag:

    task = PythonOperator(
        task_id="print_params",
        python_callable=print_params,
        # Task 레벨에서 params 추가 또는 덮어쓰기 가능
        params={"input_path": "task/specific/path"},
    )

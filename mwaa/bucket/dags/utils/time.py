from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.xcom import create_push_to_xcom_task, pull_from_xcom


def create_push_time_info_task(
    dag: DAG, date: str, time: str, batch: int
) -> PythonOperator:
    return create_push_to_xcom_task(
        dag,
        "time_info",
        {
            "date": date,
            "time": time,
            "batch": batch,
        },
    )


def create_push_start_time_task(dag: DAG) -> PythonOperator:
    current_time = datetime.now()
    return create_push_to_xcom_task(
        dag,
        "start_time",
        {
            "date": current_time.strftime("%Y-%m-%d"),
            "time": current_time.strftime("%H:%M:%S"),
        },
    )


def pull_time_info(**context) -> dict[str, str | int]:
    return {
        "date": pull_from_xcom("time_info", "date", **context),
        "time": pull_from_xcom("time_info", "time", **context),
        "batch": pull_from_xcom("time_info", "batch", **context),
    }


def get_time_diff(
    start_date: str, start_time: str, current_date: str, current_time: str
) -> str:
    current_datetime = datetime.strptime(
        f"{current_date} {current_time}", "%Y-%m-%d %H:%M:%S"
    )
    start_datetime = datetime.strptime(
        f"{start_date} {start_time}", "%Y-%m-%d %H:%M:%S"
    )

    # 시간 차이 계산
    time_diff = current_datetime - start_datetime

    # 전체 초를 시/분/초로 변환
    total_seconds = int(time_diff.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    # HH:MM:SS 형식으로 포맷팅
    time_diff_formatted = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    return time_diff_formatted

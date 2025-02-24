from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import timezone

from utils.xcom import create_push_to_xcom_task, pull_from_xcom


def create_push_time_info_task(
    dag: DAG, date: str, time: str, batch: int
) -> PythonOperator:
    """
    Logical Datetime 정보를 XCom에 저장하는 Task를 생성합니다.
    Args:
        dag (DAG): Airflow DAG
        date (str): 날짜
        time (str): 시간
        batch (int): 배치
    Returns:
        PythonOperator: Task
    """
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
    """
    작업 시작 시간을 XCom에 저장하는 Task를 생성합니다.
    Args:
        dag (DAG): Airflow DAG
    Returns:
        PythonOperator: Task
    """
    KST = timezone("Asia/Seoul")
    current_datetime = datetime.now(KST)

    return create_push_to_xcom_task(
        dag,
        "start_time",
        {
            "date": current_datetime.strftime("%Y-%m-%d"),
            "time": current_datetime.strftime("%H:%M:%S"),
        },
    )


def pull_time_info(**context) -> dict[str, str | int]:
    """
    Logical Datetime 정보를 XCom에서 가져옵니다.
    Args:
        **context: Airflow Context
    Returns:
        dict[str, str | int]: 날짜, 시간, 배치 정보
    """
    return {
        "date": pull_from_xcom("time_info", "date", **context),
        "time": pull_from_xcom("time_info", "time", **context),
        "batch": pull_from_xcom("time_info", "batch", **context),
    }


def get_time_diff(
    start_date: str, start_time: str, current_date: str, current_time: str
) -> str:
    """
    두 날짜와 시간 사이의 시간 차이를 계산합니다.
    Args:
        start_date (str): 시작 날짜
        start_time (str): 시작 시간
        current_date (str): 현재 날짜
        current_time (str): 현재 시간
    Returns:
        str: 시간 차이 (HH:MM:SS)
    """
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

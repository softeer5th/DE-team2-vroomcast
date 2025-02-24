from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.xcom import create_push_to_xcom_task, pull_from_xcom


def create_push_time_task(dag: DAG, date: str, time: str, batch: int) -> PythonOperator:
    return create_push_to_xcom_task(
        dag,
        "time_info",
        {
            "date": date,
            "time": time,
            "batch": batch,
        },
    )


def pull_time_info(**context) -> dict[str, str | int]:
    return {
        "date": pull_from_xcom("time_info", "date", **context),
        "time": pull_from_xcom("time_info", "time", **context),
        "batch": pull_from_xcom("time_info", "batch", **context),
    }

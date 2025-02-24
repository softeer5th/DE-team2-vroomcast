from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator


def _push_to_xcom(**context) -> None:
    for key, value in context["templates_dict"].items():
        context["task_instance"].xcom_push(key=key, value=value)


def pull_from_xcom(task_id: str, key: str, **context) -> Any:
    return context["task_instance"].xcom_pull(task_ids=task_id, key=key)


def create_push_to_xcom_task(dag: DAG, task_id: str, values: dict[str | Any]):
    return PythonOperator(
        task_id=task_id, python_callable=_push_to_xcom, templates_dict=values, dag=dag
    )

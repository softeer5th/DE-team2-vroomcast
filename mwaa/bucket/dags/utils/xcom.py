from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator


def _push_to_xcom(**context) -> None:
    """
    XCom에 값을 저장합니다.
    Args:
        **context: Airflow Context
    """
    for key, value in context["templates_dict"].items():
        context["task_instance"].xcom_push(key=key, value=value)


def pull_from_xcom(task_id: str, key: str, **context) -> Any:
    """
    XCom에서 값을 가져옵니다.
    Args:
        task_id (str): Task ID
        key (str): Key
        **context: Airflow Context
    Returns:
        Any
    """
    return context["task_instance"].xcom_pull(task_ids=task_id, key=key)


def create_push_to_xcom_task(dag: DAG, task_id: str, values: dict[str | Any]):
    """
    XCom에 값을 저장하는 Task를 생성합니다.
    Args:
        dag (DAG): Airflow DAG
        task_id (str): Task ID
        values (dict[str, Any]): 저장할 값
    Returns:
        PythonOperator: Task
    """
    return PythonOperator(
        task_id=task_id, python_callable=_push_to_xcom, templates_dict=values, dag=dag
    )

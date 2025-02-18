import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def print_hello():
    logger.info("Hello from MWAA!")
    logger.info("This is a test for MWAA logs")
    logger.error("This is an error from MWAA!")
    return "Hello from MWAA!"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "hello_mwaa",
    default_args=default_args,
    description="A simple MWAA DAG",
    schedule_interval=timedelta(days=1),
)

task = PythonOperator(
    task_id="hello_task",
    python_callable=print_hello,
    dag=dag,
)

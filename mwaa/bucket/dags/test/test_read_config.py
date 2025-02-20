from datetime import datetime

from airflow import DAG
from modules.config import create_read_config_task

with DAG(
    dag_id="test_read_config",
    start_date=datetime(2024, 2, 19),
    default_args={
        "owner": "airflow",
    },
    schedule_interval=None,
) as dag:
    read_config_task = create_read_config_task(dag)
    read_config_task

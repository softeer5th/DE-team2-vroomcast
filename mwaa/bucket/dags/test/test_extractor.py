from datetime import datetime

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from modules.aggregator import create_aggregate_task
from modules.constants import CARS
from modules.extractor import create_extract_task
from modules.notificator import create_notificate_extract_task

with DAG(
    dag_id="test_extractor",
    start_date=datetime(2024, 2, 19),
    default_args={
        "owner": "airflow",
    },
    params={  # DAG 레벨의 기본 params 설정
        "community": "bobaedream",
        "date": "2020-01-05",
        "batch": 0,
        "start_datetime": "2020-01-01",
        "end_datetime": "2020-01-05",
    },
    schedule_interval=None,
) as dag:
    extract_tasks = []

    for car_id, keywords in CARS.items():
        # Extract 태스크 생성

        extract_task = create_extract_task(
            dag,
            dag.params["community"],
            car_id,
            keywords,
            dag.params["date"],
            dag.params["batch"],
            dag.params["start_datetime"],
            dag.params["end_datetime"],
        )

        extract_tasks.append(extract_task)

        # Aggregate 태스크 생성
    aggregate_task = create_aggregate_task(dag)
    aggregate_task.trigger_rule = TriggerRule.ALL_DONE

    # Notificate 태스크 생성
    notificate_extract_task = create_notificate_extract_task(dag)

    extract_tasks >> aggregate_task >> notificate_extract_task

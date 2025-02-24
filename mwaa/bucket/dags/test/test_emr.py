import logging
from datetime import datetime

from airflow import DAG

from modules.transformer import (create_check_emr_termination_task,
                                 create_execute_emr_task,
                                 create_terminate_emr_cluster_task)
from utils.xcom import create_push_to_xcom_task, pull_from_xcom

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

"""
EMR 클러스터 생성 및 Spark Job 제출 테스트
"""

with DAG(
    dag_id="test_emr",
    start_date=datetime(2024, 2, 19),
    default_args={
        "owner": "airflow",
    },
    schedule_interval=None,
) as dag:

    info = [
        {"date": "2025-02-24", "time": "02:30:00", "batch": "150"},
        {"date": "2025-02-24", "time": "03:15:09", "batch": "195"},
    ]

    push_task = create_push_to_xcom_task(
        dag, "synchronize", {"prev_batch_info": info[0], "current_batch_info": info[1]}
    )

    execute_emr_task = create_execute_emr_task(dag)
    check_emr_termination_task = create_check_emr_termination_task(dag)
    terminate_emr_cluster_task = create_terminate_emr_cluster_task(dag)

    (
        push_task
        >> execute_emr_task
        >> check_emr_termination_task
        >> terminate_emr_cluster_task
    )

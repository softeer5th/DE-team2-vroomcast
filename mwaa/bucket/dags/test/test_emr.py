import logging
from datetime import datetime

from airflow import DAG
from modules.transformer import create_execute_emr_task, create_check_emr_termination_task, create_terminate_emr_cluster_task

from utils.xcom import pull_from_xcom, create_push_to_xcom_task

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

with DAG(
    dag_id="test_synchronizer",
    start_date=datetime(2024, 2, 19),
    default_args={
        "owner": "airflow",
    },
    schedule_interval=None,
) as dag:

    info = [
        {
            "date": "2025-02-24",
            "time": "02:30:00",
            "batch": "150"
        },
        {
            "date": "2025-02-24",
            "time": "03:15:09",
            "batch": "195"
        }
    ]


    # execute_emr_task = create_execute_emr_task(dag)
    # check_emr_termination_task = create_check_emr_termination_task(dag)
    # terminate_emr_cluster_task = create_terminate_emr_cluster_task(dag)

    # execute_emr_task >> check_emr_termination_task >> terminate_emr_cluster_task
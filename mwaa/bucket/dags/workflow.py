import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.lambda_function import \
    LambdaInvokeFunctionOperator
from modules.constants import (BATCH_DURATION_HOURS, BATCH_INTERVAL_MINUTES,
                               CARS, COMMUNITIES, CONFIG_PATH, S3_BUCKET)
from modules.extractor import create_extract_task
from modules.monitor import create_monitor_extract_task
from modules.combiner import create_combine_task

logger = logging.getLogger(__name__)

default_args = {
    "owner": "vroomcast",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 10, 2, 0, 0),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "vroomcast_workflow",
    default_args=default_args,
    description="Data pipeline workflow for Vroomcast",
    schedule_interval=timedelta(minutes=BATCH_INTERVAL_MINUTES),
    catchup=False,
) as dag:
    logger.info("=== Starting DAG Task Creation ===")

    cur_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    ref_dt = cur_time
    ref_date = datetime.fromisoformat(ref_dt).strftime('%Y-%m-%d')  # '2024-02-19'
    ref_time = datetime.fromisoformat(ref_dt).strftime('%H:%M:%S')  # '04:00:00'

    logger.info(f"Reference date: {ref_date}")
    logger.info(f"Reference time: {ref_time}")

    # 현재 시간을 기준으로 추출 기간 설정
    end_datetime = ref_dt
    start_datetime = (
        datetime.fromisoformat(ref_dt) - timedelta(hours=BATCH_DURATION_HOURS)
    ).strftime('%Y-%m-%dT%H:%M:%S')

    # 하루의 시작(00:00)부터 몇 분이 지났는지
    batch = (datetime.fromisoformat(cur_time).hour * 60) + datetime.fromisoformat(cur_time).minute

    extract_tasks = []
    monitor_extract_tasks = []
    combine_tasks = []

    # 각 차종에 대해 Extract 태스크와 Combine 태스크 생성
    for car_info in CARS.items():
        car_id = car_info[0]
        keywords = car_info[1]
        logger.info(f"\nProcessing car: {car_id}")

        car_extract_tasks = []

        for community in COMMUNITIES:
            logger.info(f"Creating tasks for {car_id} in {community}")
            extract_task = create_extract_task(
                dag, community, car_id, keywords, batch, start_datetime, end_datetime
            )

            car_extract_tasks.append(extract_task)

        # Combine 태스크 생성
        combine_task = create_combine_task(dag, car_id, ref_dt)
        logger.info(f"Setting dependencies for combine task: {combine_task.task_id}")

        # Validate 태스크들과 Combine 태스크 간의 dependency 설정
        car_extract_tasks >> combine_task

        extract_tasks.extend(car_extract_tasks)
        combine_tasks.append(combine_task)

        logger.info(f"Completed task creation for {car_id}")

    # Monitor Extract 태스크 생성
    monitor_extract_task = create_monitor_extract_task(dag)
    extract_tasks >> monitor_extract_task

    logger.info("\n=== Task Creation Summary ===")
    logger.info(f"Total extract tasks: {len(extract_tasks)}")
    logger.info(f"Total combine tasks: {len(combine_tasks)}")

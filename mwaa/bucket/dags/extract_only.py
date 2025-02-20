import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from modules.aggregator import create_aggregate_task
from modules.combiner import create_combine_task
from modules.constants import (
    BATCH_DURATION_HOURS,
    BATCH_INTERVAL_MINUTES,
    CARS,
    COMMUNITIES,
)
from modules.extractor import create_extract_task
from pendulum import timezone

logger = logging.getLogger(__name__)

KST = timezone("Asia/Seoul")

default_args = {
    "owner": "vroomcast",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 10, 0, 0, 0, tzinfo=KST),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "extract_only",
    default_args=default_args,
    description="Data pipeline workflow for Vroomcast",
    schedule_interval=timedelta(minutes=BATCH_INTERVAL_MINUTES),
    catchup=False,
    # max_active_runs=1,
    user_defined_macros={"BATCH_DURATION_HOURS": BATCH_DURATION_HOURS},
) as dag:
    logger.info("=== Starting DAG Task Creation ===")

    ref_dt = (
        "{{ execution_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%dT%H:%M:%S') }}"
    )
    ref_date = "{{ execution_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
    ref_time = "{{ execution_date.in_timezone('Asia/Seoul').strftime('%H:%M:%S') }}"

    logger.info(f"Reference date: {ref_date}")
    logger.info(f"Reference time: {ref_time}")

    # 현재 시간을 기준으로 추출 기간 시작 시간과 종료 시간 설정
    end_datetime = (
        "{{ execution_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%dT%H:%M:%S') }}"
    )
    start_datetime = "{{ (execution_date.in_timezone('Asia/Seoul') - macros.timedelta(hours=BATCH_DURATION_HOURS)).strftime('%Y-%m-%dT%H:%M:%S') }}"

    # 하루의 시작(00:00)부터 몇 분이 지났는지
    batch = "{{ (execution_date.in_timezone('Asia/Seoul').hour * 60) + execution_date.in_timezone('Asia/Seoul').minute }}"

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
                dag,
                community,
                car_id,
                keywords,
                ref_date,
                batch,
                start_datetime,
                end_datetime,
            )

            car_extract_tasks.append(extract_task)

        # Combine 태스크 생성
        combine_task = create_combine_task(dag, car_id, ref_date, batch, ref_dt)
        combine_task.trigger_rule = TriggerRule.ALL_DONE
        logger.info(f"Setting dependencies for combine task: {combine_task.task_id}")

        # Validate 태스크들과 Combine 태스크 간의 dependency 설정
        car_extract_tasks >> combine_task

        extract_tasks.extend(car_extract_tasks)
        combine_tasks.append(combine_task)

        logger.info(f"Completed task creation for {car_id}")

    # Aggregate 태스크 생성
    aggregate_task = create_aggregate_task(dag)
    aggregate_task.trigger_rule = TriggerRule.ALL_DONE

    extract_tasks >> aggregate_task

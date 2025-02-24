import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.baseoperator import cross_downstream
from airflow.utils.trigger_rule import TriggerRule
from modules.aggregator import create_aggregate_task
from modules.analyzer import create_analyze_sentiment_task
from modules.combiner import create_combine_task
from modules.constants import (
    BATCH_DURATION_HOURS,
    BATCH_INTERVAL_MINUTES,
    CARS,
    COMMUNITIES,
)
from modules.extractor import create_extract_task
from modules.loader import (
    create_load_dynamic_to_redshift_tasks,
    create_load_post_car_to_redshift_tasks,
    create_load_static_to_redshift_tasks,
)
from modules.notificator import (
    create_notificate_all_done_task,
    create_notificate_extract_task,
    create_social_alert_task
)
from modules.synchronizer import create_synchronize_task
from modules.transformer import (
    create_check_emr_termination_task,
    create_execute_emr_task,
    create_terminate_emr_cluster_task,
)
from pendulum import timezone
from utils.time import create_push_time_task

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

KST = timezone("Asia/Seoul")

with DAG(
    "vroomcast_workflow",
    default_args=default_args,
    description="Data pipeline workflow for Vroomcast",
    schedule_interval=timedelta(minutes=BATCH_INTERVAL_MINUTES),
    catchup=False,
    user_defined_macros={"BATCH_DURATION_HOURS": BATCH_DURATION_HOURS},
    max_active_runs=1,  # 한번에 하나의 DAG 인스턴스만 실행
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

    push_time_task = create_push_time_task(dag, ref_date, ref_time, batch)

    synchronize_task = create_synchronize_task(dag, "batch.json")

    extract_tasks = []
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

        # Extract >> Combine
        car_extract_tasks >> combine_task

        extract_tasks.extend(car_extract_tasks)
        combine_tasks.append(combine_task)

        logger.info(f"Completed task creation for {car_id}")

    push_time_task >> synchronize_task >> extract_tasks

    # Aggregate 태스크 생성
    aggregate_task = create_aggregate_task(dag)
    aggregate_task.trigger_rule = TriggerRule.ALL_DONE

    # Notificate 태스크 생성
    notificate_extract_task = create_notificate_extract_task(dag)

    # Extract >> Aggregate >> Notificate
    extract_tasks >> aggregate_task >> notificate_extract_task

    execute_emr_task = create_execute_emr_task(dag)
    check_emr_termination_task = create_check_emr_termination_task(dag)
    terminate_emr_cluster_task = create_terminate_emr_cluster_task(dag)

    # Combine >> Execute EMR >> Check EMR Termination >> Terminate EMR
    (
        combine_tasks
        >> execute_emr_task
        >> check_emr_termination_task
        >> terminate_emr_cluster_task
    )

    analyze_sentiment_task = create_analyze_sentiment_task(dag, ref_date, batch)

    load_dynamic_toredshift_tasks = create_load_dynamic_to_redshift_tasks(
        dag, ref_date, batch
    )
    load_post_car_to_redshift_tasks = create_load_post_car_to_redshift_tasks(
        dag, ref_date, batch
    )
    load_transformed_to_redshift_tasks = create_load_static_to_redshift_tasks(
        dag, ref_date, batch
    )

    social_alert_task = create_social_alert_task(dag)

    notificate_all_done_task = create_notificate_all_done_task(dag)

    (
        terminate_emr_cluster_task
        >> analyze_sentiment_task
        >> load_transformed_to_redshift_tasks
    )

    cross_downstream(load_transformed_to_redshift_tasks, load_dynamic_toredshift_tasks)

    cross_downstream(load_dynamic_toredshift_tasks, load_post_car_to_redshift_tasks)

    load_post_car_to_redshift_tasks >> social_alert_task >> notificate_all_done_task

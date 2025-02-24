import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.synchronizer import create_synchronize_task
from utils.time import create_push_time_info_task
from utils.xcom import pull_from_xcom

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

"""
지난 배치 정보 조회 및 현재 배치 정보 갱신 테스트
"""

with DAG(
    dag_id="test_synchronizer",
    start_date=datetime(2024, 2, 19),
    default_args={
        "owner": "airflow",
    },
    schedule_interval=None,
) as dag:

    # 날짜 및 시간
    ref_dt = (
        "{{ execution_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%dT%H:%M:%S') }}"
    )
    ref_date = "{{ execution_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
    ref_time = "{{ execution_date.in_timezone('Asia/Seoul').strftime('%H:%M:%S') }}"

    # 하루의 시작(00:00)부터 몇 분이 지났는지
    batch = "{{ (execution_date.in_timezone('Asia/Seoul').hour * 60) + execution_date.in_timezone('Asia/Seoul').minute }}"

    # 현재 시간 정보를 XCom에 저장
    push_time_task = create_push_time_info_task(dag, ref_date, ref_time, batch)

    # S3에 저장된 배치 정보를 읽고 현재 배치 정보를 갱신
    synchronize_task = create_synchronize_task(dag, "test_batch.json")

    # 이전 배치 정보 조회
    def _get_prev_batch_info(**context):
        prev_batch = pull_from_xcom("synchronize", "prev_batch_info", **context)
        logger.info(f"Prev Batch: {prev_batch}")
        return prev_batch

    # 이전 배치 정보 조회 태스크
    get_prev_batch_info_task = PythonOperator(
        task_id="get_prev_batch_info",
        python_callable=_get_prev_batch_info,
        provide_context=True,
        dag=dag,
    )

    push_time_task >> synchronize_task >> get_prev_batch_info_task

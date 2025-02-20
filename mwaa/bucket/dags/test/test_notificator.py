from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from modules.notificator import create_notificate_extract_task
from utils.time import create_push_time_task

with DAG(
    dag_id="test_notification",
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

    push_time_task = create_push_time_task(dag, ref_date, ref_time, batch)

    def extract(**context):
        return {
            "ev9": {
                "bobaedream": {
                    "success": True,
                    "error": None,
                    "attempted_posts_count": 1,
                    "extracted_posts_count": 1,
                },
                "clien": {
                    "success": True,
                    "error": None,
                    "attempted_posts_count": 0,
                    "extracted_posts_count": 0,
                },
                "dcinside": {
                    "success": True,
                    "error": None,
                    "attempted_posts_count": 1,
                    "extracted_posts_count": 1,
                },
            },
            "ioniq9": {
                "bobaedream": {
                    "success": True,
                    "error": None,
                    "attempted_posts_count": 1,
                    "extracted_posts_count": 1,
                },
                "clien": {
                    "success": True,
                    "error": None,
                    "attempted_posts_count": 0,
                    "extracted_posts_count": 0,
                },
                "dcinside": {
                    "success": True,
                    "error": None,
                    "attempted_posts_count": 2,
                    "extracted_posts_count": 2,
                },
            },
            "palisade": {
                "bobaedream": {
                    "success": True,
                    "error": None,
                    "attempted_posts_count": 1,
                    "extracted_posts_count": 1,
                },
                "clien": {
                    "success": True,
                    "error": None,
                    "attempted_posts_count": 0,
                    "extracted_posts_count": 0,
                },
                "dcinside": {
                    "success": True,
                    "error": None,
                    "attempted_posts_count": 17,
                    "extracted_posts_count": 17,
                },
            },
        }

    aggregate_task = PythonOperator(
        task_id="aggregate_task",
        python_callable=extract,
        dag=dag,
    )

    notificate_task = create_notificate_extract_task(dag)

    push_time_task >> aggregate_task >> notificate_task

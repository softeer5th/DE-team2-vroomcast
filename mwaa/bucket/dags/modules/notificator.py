import json
from datetime import datetime, timedelta
import logging

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from modules.constants import SLACK_WEBHOOK_URL

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def _generate_community_stats_message(data):
    # 커뮤니티별 통계를 저장할 딕셔너리 초기화
    stats = {
        'bobaedream': {'attempted': 0, 'extracted': 0},
        'clien': {'attempted': 0, 'extracted': 0},
        'dcinside': {'attempted': 0, 'extracted': 0}
    }
    
    # 각 차종별 데이터를 순회하며 통계 집계
    for car_data in data.values():
        for community, results in car_data.items():
            if results['attempted_posts_count'] is not None:
                stats[community]['attempted'] += results['attempted_posts_count']
            if results['extracted_posts_count'] is not None:
                stats[community]['extracted'] += results['extracted_posts_count']
    
    # 메시지 생성
    message = "📊 커뮤니티별 수집 현황\n"
    for community, counts in stats.items():
        message += f"• {community}: 시도 {counts['attempted']}건, 추출 {counts['extracted']}건\n"
    
    return message

def create_notificate_extract_task(dag: DAG) -> PythonOperator:
    def _notificate(**context) -> None:
        task_instance = context["task_instance"]
        stats = task_instance.xcom_pull(task_ids="aggregate_task")
        logger.info("Sending notification to Slack")
        message = _generate_community_stats_message(stats)
        requests.post(SLACK_WEBHOOK_URL, json={"text": message})
        logger.info("Notification sent to Slack")
    notificate_extract_task = PythonOperator(
        task_id="notificate_extract_task", python_callable=_notificate, dag=dag
    )
    return notificate_extract_task

def create_notificate_all_done_task(dag: DAG, date: str, batch: int) -> PythonOperator:
    def _notificate(**context) -> None:
        logger.info("Sending notification to Slack")
        requests.post(SLACK_WEBHOOK_URL, json={"text": f"데이터 처리가 완료되었습니다."})
        logger.info("Notification sent to Slack")
    notificate_all_done_task = PythonOperator(
        task_id="notificate_all_done_task", python_callable=_notificate, dag=dag
    )
    return notificate_all_done_task
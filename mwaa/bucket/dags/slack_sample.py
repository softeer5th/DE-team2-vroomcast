import json
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from modules.constants import SLACK_WEBHOOK_URL

# DAG의 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# 모니터링 데이터를 생성하는 예제 함수
def generate_monitoring_data(**context):
    # 예제 모니터링 데이터
    monitoring_result = {
        "status": "success",
        "processed_records": 1000,
        "execution_time": "120s",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # XCom에 결과 저장
    context["task_instance"].xcom_push(key="monitoring_data", value=monitoring_result)
    return "모니터링 데이터 생성 완료"


# Slack으로 메시지를 보내는 함수
def send_slack_message(**context):
    # XCom에서 이전 태스크의 결과 가져오기
    monitoring_data = context["task_instance"].xcom_pull(
        task_ids="generate_monitoring_data", key="monitoring_data"
    )

    # Slack 메시지 포맷팅
    message = {
        "text": "Airflow 모니터링 알림",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Airflow 모니터링 결과*\n"
                    f"• 상태: {monitoring_data['status']}\n"
                    f"• 처리된 레코드: {monitoring_data['processed_records']}\n"
                    f"• 실행 시간: {monitoring_data['execution_time']}\n"
                    f"• 타임스탬프: {monitoring_data['timestamp']}",
                },
            }
        ],
    }

    # Slack webhook으로 메시지 전송
    response = requests.post(
        SLACK_WEBHOOK_URL,
        data=json.dumps(message),
        headers={"Content-Type": "application/json"},
    )

    if response.status_code != 200:
        raise ValueError(
            f"Slack 메시지 전송 실패: {response.status_code}, {response.text}"
        )

    return "Slack 메시지 전송 완료"


# DAG 정의
with DAG(
    "slack_monitoring_dag",
    default_args=default_args,
    description="모니터링 결과를 Slack으로 전송하는 DAG",
    schedule_interval=None,
    catchup=False,
) as dag:

    # 모니터링 데이터 생성 태스크
    monitoring_task = PythonOperator(
        task_id="generate_monitoring_data",
        python_callable=generate_monitoring_data,
    )

    # Slack 메시지 전송 태스크
    slack_task = PythonOperator(
        task_id="send_slack_message",
        python_callable=send_slack_message,
    )

    # 태스크 순서 설정
    monitoring_task >> slack_task

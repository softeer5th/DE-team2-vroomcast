# Slack Operator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# Slack Webhook URL 설정
SLACK_WEBHOOK_URL = "your_slack_webhook_url"  # Slack에서 발급받은 Webhook URL 입력

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'slack_notification_test',
    default_args=default_args,
    schedule_interval=None,  # 수동 실행을 위해 None으로 설정
    catchup=False
)

def dummy_function():
    return "Task 실행 완료!"

# 테스트용 태스크
test_task = PythonOperator(
    task_id='test_task',
    python_callable=dummy_function,
    dag=dag
)

# Slack 알림 태스크
slack_notification = SlackWebhookOperator(
    task_id='slack_notification',
    webhook_token=SLACK_WEBHOOK_URL,
    message="DAG 실행이 완료되었습니다! :white_check_mark:",
    channel="#your-channel",  # 알림을 보낼 채널명
    username='Airflow Bot',
    dag=dag
)

test_task >> slack_notification
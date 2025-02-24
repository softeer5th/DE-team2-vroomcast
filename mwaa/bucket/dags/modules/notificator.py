import json
import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from modules.constants import SLACK_WEBHOOK_URL
from modules.operators import LambdaInvokeFunctionOperator
from utils.time import pull_time_info

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _generate_community_stats_message(
    stats: dict[str, dict], time_info: dict[str, str | int]
) -> str:
    """
    커뮤니티별 통계를 Slack 메시지로 변환합니다.
    Args:
        stats (dict[str, dict]): 커뮤니티별 통계
        time_info (dict[str, str | int]): 시간 정보
    Returns:
        str: 메시지
    """
    # 커뮤니티별 통계를 저장할 딕셔너리 초기화
    sums = {
        "bobaedream": {"attempted": 0, "extracted": 0},
        "clien": {"attempted": 0, "extracted": 0},
        "dcinside": {"attempted": 0, "extracted": 0},
    }

    # 통계 집계
    for stat in stats.values():
        for community, results in stat.items():
            if results["attempted_posts_count"] is not None:
                sums[community]["attempted"] += results["attempted_posts_count"]
            if results["extracted_posts_count"] is not None:
                sums[community]["extracted"] += results["extracted_posts_count"]

    # 메시지 생성
    message = ">>>*데이터 수집 리포트*\n"
    message += f"━━━━━━━━━━━━━━━━━━━━━━\n\n"

    message += f">>*수집 정보*\n"
    message += f"📅 일자: {time_info['date']}\n"
    message += f"⏰ 시각: {time_info['time']}\n"
    message += f"🔄 배치: #{time_info['batch']}\n\n"

    message += f">>*커뮤니티별 현황*\n"
    for community, counts in sums.items():
        success_rate = (
            (counts["extracted"] / counts["attempted"] * 100)
            if counts["attempted"] > 0
            else 0
        )
        message += f"🌐 *{community}*\n"
        message += f"└ 시도: `{counts['attempted']:,}건` | 성공: `{counts['extracted']:,}건` | 성공률: `{success_rate:.1f}%`\n"

    message += "\n💡 _상세 내역은 대시보드를 참고해주세요_"

    return message


def create_notificate_extract_task(dag: DAG) -> PythonOperator:
    """
    추출 Task 완료 시 Slack으로 알림을 보내는 Task를 생성합니다.
    Args:
        dag (DAG): Airflow DAG
    Returns:
        PythonOperator: Task
    """
    def _notificate(**context) -> None:
        task_instance = context["task_instance"]
        stats = task_instance.xcom_pull(task_ids="aggregate_task")

        time_info = pull_time_info(**context)

        logger.info("Sending notification to Slack")
        message = _generate_community_stats_message(stats, time_info)
        # Slack으로 메시지 전송
        requests.post(SLACK_WEBHOOK_URL, json={"text": message})
        logger.info("Notification sent to Slack")

    notificate_extract_task = PythonOperator(
        task_id="notificate_extract_task", python_callable=_notificate, dag=dag
    )
    return notificate_extract_task


def create_notificate_all_done_task(dag: DAG) -> PythonOperator:
    """
    모든 Task 완료 시 Slack으로 알림을 보내는 Task를 생성합니다.
    Args:
        dag (DAG): Airflow DAG
    Returns:
        PythonOperator: Task
    """
    def _notificate(**context) -> None:
        logger.info("Sending notification to Slack")
        # Slack으로 메시지 전송
        requests.post(
            SLACK_WEBHOOK_URL, json={"text": f"데이터 처리가 완료되었습니다."}
        )
        logger.info("Notification sent to Slack")

    notificate_all_done_task = PythonOperator(
        task_id="notificate_all_done_task", python_callable=_notificate, dag=dag
    )
    return notificate_all_done_task


def create_social_alert_task(dag: DAG) -> LambdaInvokeFunctionOperator:
    """
    임계값을 넘은 데이터 식별시 경고선 알림을 발송하는 Lambda를 호출하는 Task를 생성합니다.
    Args:
        dag (DAG): Airflow DAG
    Returns:
        LambdaInvokeFunctionOperator: Task
    """
    return LambdaInvokeFunctionOperator(
        task_id=f"social_alert",
        function_name=f"vroomcast-social-alert",
        dag=dag,
    )

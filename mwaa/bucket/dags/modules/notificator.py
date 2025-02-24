import json
import logging
from datetime import datetime, timedelta
from time import sleep

from pendulum import timezone
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.constants import SLACK_WEBHOOK_URL
from modules.operators import LambdaInvokeFunctionOperator
from utils.time import get_time_diff, pull_time_info
from utils.xcom import pull_from_xcom

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _generate_community_stats_message(stats: dict[str, dict], **context) -> dict:
    """
    커뮤니티별 통계를 Slack 메시지로 변환합니다.
    Args:
        stats (dict[str, dict]): 커뮤니티별 통계
    Returns:
        dict: Slack Blocks 형식의 메시지
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

    time_info = pull_time_info(**context)

    KST = timezone("Asia/Seoul")
    current_datetime = datetime.now(KST)

    current_date = current_datetime.strftime("%Y-%m-%d")
    current_time = current_datetime.strftime("%H:%M:%S")
    start_date = pull_from_xcom("start_time", "date", **context)
    start_time = pull_from_xcom("start_time", "time", **context)
    elapsed_time = get_time_diff(start_date, start_time, current_date, current_time)

    # Slack Blocks 메시지 생성
    message = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "📊 데이터 수집 리포트",
                    "emoji": True,
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*📅 논리적 스케줄링 정보*"},
                "fields": [
                    {"type": "mrkdwn", "text": f"*Logical Date:*\n{time_info['date']}"},
                    {"type": "mrkdwn", "text": f"*Logical Time:*\n{time_info['time']}"},
                    {"type": "mrkdwn", "text": f"*Batch:*\n#{time_info['batch']}"},
                ],
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*⏱️ 실행 시간 정보*"},
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*시작 시간:*\n{start_date} {start_time}",
                    },
                    {"type": "mrkdwn", "text": f"*소요 시간:*\n{elapsed_time}"},
                ],
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*🌐 커뮤니티별 수집 현황*"},
            },
        ]
    }

    # 커뮤니티별 통계 블록 추가
    for community, counts in sums.items():
        success_rate = (
            (counts["extracted"] / counts["attempted"] * 100)
            if counts["attempted"] > 0
            else 0
        )

        # 성공률에 따른 색상 설정
        rate_color = "🔴" if success_rate < 70 else "🟡" if success_rate < 90 else "🟢"

        message["blocks"].append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*{community.upper()}*\n"
                        f"시도: `{counts['attempted']:,}건` | "
                        f"성공: `{counts['extracted']:,}건` | "
                        f"{rate_color} 성공률: `{success_rate:.1f}%`"
                    ),
                },
            }
        )

    # 푸터 추가
    message["blocks"].extend(
        [
            {"type": "divider"},
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": "💡 상세 내역은 대시보드를 참고해주세요"}
                ],
            },
        ]
    )

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

        logger.info("Sending notification to Slack")
        message = _generate_community_stats_message(stats, **context)
        # Slack으로 메시지 전송
        requests.post(SLACK_WEBHOOK_URL, json=message)
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
        start_date = pull_from_xcom("start_time", "date", **context)
        start_time = pull_from_xcom("start_time", "time", **context)

        KST = timezone("Asia/Seoul")
        current_datetime = datetime.now(KST)

        current_date = current_datetime.strftime("%Y-%m-%d")
        current_time = current_datetime.strftime("%H:%M:%S")
        elapsed_time = get_time_diff(start_date, start_time, current_date, current_time)

        dag_id = context["dag"].dag_id

        # Slack 메시지 포맷팅
        message = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🎉 데이터 처리 완료 알림",
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*DAG:*\n{dag_id}"},
                        {"type": "mrkdwn", "text": f"*소요 시간:*\n{elapsed_time}"},
                    ],
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*시작 시간:*\n{start_date} {start_time}",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*완료 시간:*\n{current_date} {current_time}",
                        },
                    ],
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": "✨ Airflow Pipeline 실행이 성공적으로 완료되었습니다.",
                        }
                    ],
                },
            ]
        }

        # Slack으로 메시지 전송
        requests.post(SLACK_WEBHOOK_URL, json=message)
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

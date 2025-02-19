import logging

from airflow.models import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def create_monitor_extract_task(dag: DAG) -> PythonOperator:
    def _monitor(**context):
        logger.info("Starting monitoring for all extract tasks")
        task_instance = context["task_instance"]

        # DAG의 모든 task를 순회하면서 extract로 시작하는 task 찾기
        monitoring_results = []
        for task in context["dag"].tasks:
            if task.task_id.startswith("extract_"):
                try:
                    # task_id에서 car_id와 community 추출
                    _, car_id, community = task.task_id.split("_")
                    extract_result = task_instance.xcom_pull(task_ids=task.task_id)

                    monitoring_results.append(
                        {
                            "task_id": task.task_id,
                            "car_id": car_id,
                            "community": community,
                            "result": extract_result,
                            "status": "success",
                        }
                    )
                    logger.info(f"Monitored {task.task_id}: {extract_result}")

                except Exception as e:
                    logger.error(
                        f"Error monitoring {task.task_id}: {str(e)}", exc_info=True
                    )
                    monitoring_results.append(
                        {
                            "task_id": task.task_id,
                            "car_id": car_id,
                            "community": community,
                            "error": str(e),
                            "status": "failed",
                        }
                    )

        # 전체 모니터링 결과를 바탕으로 알림 발송 등 처리
        # 예: Slack으로 종합 결과 전송

        return monitoring_results

    monitor_task = PythonOperator(
        task_id="monitor_extract_tasks", python_callable=_monitor, dag=dag
    )

    return monitor_task

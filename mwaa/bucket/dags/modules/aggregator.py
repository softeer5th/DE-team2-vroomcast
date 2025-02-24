import logging

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from modules.constants import CARS, COMMUNITIES

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_aggregate_task(dag: DAG) -> PythonOperator:
    """
    추출 결과 집계 Task을 생성합니다.
    Args:
        dag (DAG): Airflow DAG
    Returns:
        PythonOperator: Task
    """

    def _aggregate(**context) -> dict:
        """
        모든 추출 Task에 대한 집계를 수행합니다.
        Args:
            **context: Airflow context
        Returns:
            dict: 집계된 통계
        """
        logger.info("Starting aggregation for all extract tasks")
        task_instance = context["task_instance"]
        statistics = {}

        for car_id in CARS:
            statistics[car_id] = {}
            for community in COMMUNITIES:
                statistics[car_id][community] = {
                    "success": True,
                    "error": None,
                    "attempted_posts_count": None,
                    "extracted_posts_count": None,
                }

                try:
                    task_id = f"extract_{car_id}_{community}"
                    extract_result = task_instance.xcom_pull(task_ids=task_id)

                    # None인 경우 처리
                    if extract_result is None:
                        statistics[car_id][community]["success"] = False
                        statistics[car_id][community][
                            "error"
                        ] = "No data returned from task"
                        continue

                    # JSON 문자열을 파싱
                    if isinstance(extract_result, str):
                        import json

                        extract_result = json.loads(extract_result)

                    info = extract_result["body"]
                    logger.info(f"Received info for {task_id}: {info}")

                    if info["success"] == False:
                        statistics[car_id][community]["success"] = False
                        statistics[car_id][community]["error"] = info["error"]
                    else:
                        statistics[car_id][community]["attempted_posts_count"] = (
                            info.get("attempted_posts_count")
                        )
                        statistics[car_id][community]["extracted_posts_count"] = (
                            info.get("extracted_posts_count")
                        )

                    logger.info(
                        f"Aggregated {task_id}: {statistics[car_id][community]}"
                    )

                except Exception as e:
                    logger.error(
                        f"Error aggregating {task_id}: {str(e)}", exc_info=True
                    )

        return statistics

    aggregate_task = PythonOperator(
        task_id="aggregate_task", python_callable=_aggregate, dag=dag
    )

    return aggregate_task

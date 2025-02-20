import json
import logging
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError
from modules.constants import S3_BUCKET
from utils.time import pull_time_info

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_synchronize_task(dag, batch_json_path: str):
    """S3의 json 파일을 읽고 현재 배치 정보를 추가하는 태스크 생성"""

    def _synchronize_batch(**context):
        s3_hook = S3Hook()

        # 현재 배치 정보
        current_batch_info = pull_time_info(**context)

        # 현재 배치 정보를 출력
        logger.info(f"Current batch: {current_batch_info}")

        try:
            # S3에서 파일 읽기
            file_content = s3_hook.read_key(key=batch_json_path, bucket_name=S3_BUCKET)

            if file_content:
                batch_list = json.loads(file_content)

                prev_batch_info = batch_list[-1]

                context["task_instance"].xcom_push(
                    key="prev_batch_info", value=prev_batch_info
                )

            else:
                batch_list = []

            # 기존 배치 리스트에 현재 배치 추가
            batch_list.append(current_batch_info)

            # 업데이트된 내용을 S3에 저장
            s3_hook.load_string(
                string_data=json.dumps(batch_list, indent=2),
                key=batch_json_path,
                bucket_name=S3_BUCKET,
                replace=True,
            )

            return current_batch_info

        except Exception as e:
            print(f"Error occurred: {str(e)}")
            raise

    return PythonOperator(
        task_id="synchronize_task",
        python_callable=_synchronize_batch,
        provide_context=True,
        dag=dag,
    )

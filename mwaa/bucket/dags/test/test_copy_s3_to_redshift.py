from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import cross_downstream

from modules.loader import (TableMapping, create_load_dynamic_to_redshift_task,
                            create_load_dynamic_to_redshift_tasks,
                            create_load_post_car_to_redshift_tasks,
                            create_load_static_to_redshift_tasks)

from modules.constants import DYNAMIC_MAPPINGS

"""
Redshift로의 적재를 종합적으로 테스트
"""

with DAG(
    dag_id="test_copy_s3_to_redshift",
    start_date=datetime(2024, 2, 19),
    default_args={
        "owner": "airflow",
    },
    schedule_interval=None,
) as dag:

    folders = [
        ("2025-02-22", 0),
        ("2025-02-23", 0),
    ]
    previous_tasks = None

    for count, folder in enumerate(folders):
        date, batch = folder
        identifier = f"{count}"

        current_transformed_tasks = create_load_static_to_redshift_tasks(
            dag, date, batch, identifier
        )
        current_combined_tasks = create_load_dynamic_to_redshift_tasks(
            dag, date, batch, identifier
        )
        current_post_car_tasks = create_load_post_car_to_redshift_tasks(
            dag, date, batch, identifier
        )

        current_tasks = (
            current_transformed_tasks + current_combined_tasks + current_post_car_tasks
        )

        if previous_tasks is not None:
            cross_downstream(from_tasks=previous_tasks, to_tasks=current_tasks)

        previous_tasks = current_tasks

"""
벡터 데이터 적재를 테스트
"""
with DAG(
    dag_id="test_copy_v_dynamic_data_to_redshift",
    start_date=datetime(2024, 2, 19),
    default_args={
        "owner": "airflow",
    },
    schedule_interval=None,
) as dag:
    car_id = "ioniq9"
    date = "2025-02-24"
    batch = 570

    table_mappings = DYNAMIC_MAPPINGS

    for count, table_mapping in enumerate(table_mappings):

        identifier = f"{count}"
        task = create_load_dynamic_to_redshift_task(
            dag, car_id, date, batch, table_mapping, identifier
        )

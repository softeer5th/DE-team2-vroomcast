from datetime import datetime

from airflow import DAG
from modules.constants import TRANSFORMED_TABLES
from modules.loader import create_load_transformed_to_readshift_tasks, create_load_combined_to_redshift_tasks, create_load_post_car_to_redshift_tasks

with DAG(
    dag_id="load_transformed_to_redshift_test",
    start_date=datetime(2024, 2, 19),
    default_args={
        "owner": "airflow",
    },
    params={  # DAG 레벨의 기본 params 설정
        "date": "2025-02-20",
        "batch": 0,
    },
    schedule_interval=None,
) as dag:

    load_transformed_to_redshift_tasks = create_load_transformed_to_readshift_tasks(dag, dag.params["date"], dag.params["batch"])

with DAG(
    dag_id="load_combined_to_redshift_test",
    start_date=datetime(2024, 2, 19),
    default_args={
        "owner": "airflow",
    },
    params={  # DAG 레벨의 기본 params 설정
        "date": "2025-02-20",
        "batch": 0,
    },
    schedule_interval=None,
) as dag:

    load_combined_to_redshift_tasks = create_load_combined_to_redshift_tasks(dag, dag.params["date"], dag.params["batch"])

with DAG(
    dag_id="load_post_car_to_redshift_test",
    start_date=datetime(2024, 2, 19),
    default_args={
        "owner": "airflow",
    },
    params={  # DAG 레벨의 기본 params 설정
        "date": "2025-02-20",
        "batch": 0,
    },
    schedule_interval=None,
) as dag:

    load_post_car_to_redshift_tasks = create_load_post_car_to_redshift_tasks(dag, dag.params["date"], dag.params["batch"])
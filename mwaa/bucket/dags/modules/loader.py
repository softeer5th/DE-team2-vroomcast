from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from modules.constants import (
    CARS,
    DYNAMIC_MAPPINGS,
    DYNAMIC_PATH,
    POST_CAR_MAPPING,
    POST_CAR_PATH,
    S3_BUCKET,
    STATIC_MAPPINGS,
    STATIC_PATH,
    TableMapping,
)


def create_load_static_to_redshift_task(
    dag: DAG,
    date: str,
    batch: int,
    table_mapping: TableMapping,
    identifier: str | None = None,
) -> S3ToRedshiftOperator:
    """
    정적 데이터를 Redshift로 로드하는 Task를 생성합니다.
    """

    return S3ToRedshiftOperator(
        task_id=f"load_transformed_{table_mapping.table}_to_redshift"
        + (f"_{identifier}" if identifier else ""),
        schema="{{ var.value.redshift_schema }}",
        table=table_mapping.table,
        s3_bucket=S3_BUCKET,
        s3_key=STATIC_PATH.format(
            date=date, batch=batch, parquet=table_mapping.parquet
        ),
        copy_options=[
            "FORMAT PARQUET",
        ],
        aws_conn_id="aws_default",
        redshift_conn_id="redshift_default",
        autocommit=True,
        dag=dag,
    )


def create_load_static_to_redshift_tasks(
    dag: DAG, date: str, batch: int, identifier: str | None = None
) -> list[S3ToRedshiftOperator]:
    """
    정적 데이터를 Redshift로 로드하는 Task들을 생성합니다.
    """
    return [
        create_load_static_to_redshift_task(dag, date, batch, table_mapping, identifier)
        for table_mapping in STATIC_MAPPINGS
    ]


def create_load_dynamic_to_redshift_task(
    dag: DAG,
    car_id: str,
    date: str,
    batch: int,
    table_mapping: TableMapping,
    identifier: str | None = None,
) -> S3ToRedshiftOperator:
    """
    동적 데이터를 Redshift로 로드하는 Task를 생성합니다.
    """

    return S3ToRedshiftOperator(
        task_id=f"load_{car_id}_{table_mapping.table}_to_redshift"
        + (f"_{identifier}" if identifier else ""),
        schema="{{ var.value.redshift_schema }}",
        table=table_mapping.table,
        s3_bucket=S3_BUCKET,
        s3_key=DYNAMIC_PATH.format(
            car_id=car_id, date=date, batch=batch, parquet=table_mapping.parquet
        ),
        copy_options=[
            "FORMAT PARQUET",
        ],
        aws_conn_id="aws_default",
        redshift_conn_id="redshift_default",
        autocommit=True,
        method="UPSERT",
        upsert_keys=table_mapping.keys,
        dag=dag,
    )


def create_load_dynamic_to_redshift_tasks(
    dag: DAG, date: str, batch: int, identifier: str | None = None
) -> list[S3ToRedshiftOperator]:
    """
    동적 데이터를 Redshift로 로드하는 Task들을 생성합니다.
    """
    return [
        create_load_dynamic_to_redshift_task(
            dag, car_id, date, batch, table_mapping, identifier
        )
        for table_mapping in DYNAMIC_MAPPINGS
        for car_id in CARS
    ]


def create_load_post_car_to_redshift_task(
    dag: DAG,
    car_id: str,
    date: str,
    batch: int,
    table_mapping: TableMapping,
    identifier: str | None = None,
) -> S3ToRedshiftOperator:
    """
    Post를 검색한 키워드를 매핑한 데이터를 Redshift로 로드하는 Task를 생성합니다.
    """
    return S3ToRedshiftOperator(
        task_id=f"load_{car_id}_{table_mapping.table}_to_redshift"
        + (f"_{identifier}" if identifier else ""),
        schema="{{ var.value.redshift_schema }}",
        table=table_mapping.table,
        s3_bucket=S3_BUCKET,
        s3_key=POST_CAR_PATH.format(car_id=car_id, date=date, batch=batch),
        copy_options=[
            "FORMAT PARQUET",
        ],
        aws_conn_id="aws_default",
        redshift_conn_id="redshift_default",
        autocommit=True,
        method="UPSERT",
        upsert_keys=table_mapping.keys,
        dag=dag,
    )


def create_load_post_car_to_redshift_tasks(
    dag: DAG, date: str, batch: int, identifier: str | None = None
) -> list[S3ToRedshiftOperator]:
    """
    Post를 검색한 키워드를 매핑한 데이터를 Redshift로 로드하는 Task들을 생성합니다.
    """
    return [
        create_load_post_car_to_redshift_task(
            dag, car_id, date, batch, POST_CAR_MAPPING, identifier
        )
        for car_id in CARS
    ]

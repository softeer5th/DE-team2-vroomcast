from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from modules.constants import CARS, S3_BUCKET, TRANSFORMED_TABLES, COMBINED_DYNAMICS


# RedshiftDataOperator를 사용한 COPY 작업
def create_load_transformed_to_redshift_task(
    dag: DAG, date: str, batch: int, table_name: str
) -> S3ToRedshiftOperator:
    
    if table_name == "sentence":
        s3_path = "sentence_sentiment"
    else:
        s3_path = table_name

    return S3ToRedshiftOperator(
        task_id=f'load_transformed_{table_name}_to_redshift',
        schema="{{ var.value.redshift_schema }}",
        table=table_name,
        s3_bucket=S3_BUCKET,
        s3_key=f"past-transformed-data/ev3/s3_path", # f"transformed/{date}/{batch}/{table_name}/",
        copy_options=[
            "FORMAT PARQUET",
        ],
        aws_conn_id='aws_default',
        redshift_conn_id='redshift_default',
        autocommit=True,
        dag=dag
    )

def create_load_transformed_to_readshift_tasks(
    dag: DAG, date: str, batch: int
) -> list[S3ToRedshiftOperator]:
    return [
        create_load_transformed_to_redshift_task(dag, date, batch, table_name)
        for table_name in TRANSFORMED_TABLES
    ]

def create_load_combined_to_redshift_task(
    dag: DAG, car_id: str, date: str, batch: int, combined_dynamic: str
) -> S3ToRedshiftOperator:
    
    return S3ToRedshiftOperator(
        task_id=f'load_combined_{car_id}_{combined_dynamic}_dynamic_to_redshift',
        schema="{{ var.value.redshift_schema }}",
        table=f"{combined_dynamic}_dynamic",
        s3_bucket=S3_BUCKET,
        s3_key=f"combined/{car_id}/{date}/{batch}/dynamic/{combined_dynamic}",
        copy_options=[
            "FORMAT PARQUET",
        ],
        aws_conn_id='aws_default',
        redshift_conn_id='redshift_default',
        autocommit=True,
        dag=dag
    )

def create_load_combined_to_redshift_tasks(
    dag: DAG, date: str, batch: int
) -> list[S3ToRedshiftOperator]:
    return [
        create_load_combined_to_redshift_task(dag, car_id, date, batch, combined_dynamic)
        for combined_dynamic in COMBINED_DYNAMICS for car_id in CARS
    ]


def create_load_post_car_to_redshift_task(
    dag: DAG, car_id: str, date: str, batch: int
) -> S3ToRedshiftOperator:
    
    return S3ToRedshiftOperator(
        task_id=f'load_{car_id}_post_car_to_redshift',
        schema="{{ var.value.redshift_schema }}",
        table=f"post_car",
        s3_bucket=S3_BUCKET,
        s3_key=f"combined/{car_id}/{date}/{batch}/post_car.parquet",
        copy_options=[
            "FORMAT PARQUET",
        ],
        aws_conn_id='aws_default',
        redshift_conn_id='redshift_default',
        autocommit=True,
        dag=dag
    )

def create_load_post_car_to_redshift_tasks(
    dag: DAG, date: str, batch: int
) -> list[S3ToRedshiftOperator]:
    return [
        create_load_post_car_to_redshift_task(dag, car_id, date, batch)
        for car_id in CARS
    ]
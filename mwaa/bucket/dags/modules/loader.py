from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator, RedshiftDataHook


from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.models import Variable

def build_copy_statement(table_name: str):
    return """
        COPY {{ var.value.redshift_database }}.{{ var.value.redshift_schema }}.{{ var.value.redshift_table }}
        FROM '{{ var.value.s3_source_path }}'
        IAM_ROLE '{{ var.value.redshift_iam_role }}'
        FORMAT AS PARQUET;
    """

# RedshiftDataOperator를 사용한 COPY 작업
def create_load_to_redshift_task(dag: DAG, date: str, batch: int, table_name: str) -> RedshiftDataOperator:
    return RedshiftDataOperator(
        task_id='load_to_redshift_task',
        sql=build_copy_statement(table_name),  # Jinja 템플릿으로 처리됨
        workgroup_name="{{ var.value.redshift_workgroup }}",  # Serverless 용
        database="{{ var.value.redshift_database }}",
        secret_arn="{{ var.value.redshift_secret_arn }}",
        wait_for_completion=True,
        dag=dag
    )

# def create_load_task(dag: DAG, date: str, batch: int) -> Redshift
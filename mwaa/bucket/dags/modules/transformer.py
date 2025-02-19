from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from modules.constants import S3_BUCKET, S3_CONFIG_BUCKET

# EMR 클러스터 설정을 위한 상수
EMR_CONFIG = {
    "RELEASE_LABEL": "emr-7.7.0",
    "INSTANCE_TYPE_MASTER": "m5.xlarge",
    "INSTANCE_TYPE_CORE": "m5.xlarge",
    "CORE_INSTANCE_COUNT": 2,
    "APPLICATIONS": [
        {"Name": "Hadoop"},
        {"Name": "Hive"},
        {"Name": "JupyterEnterpriseGateway"},
        {"Name": "Livy"},
        {"Name": "Spark"},
    ],
    "AUTO_TERMINATION_IDLE_TIMEOUT": 3600,  # 1시간S
}


# Jinja 템플릿을 사용한 EMR 클러스터 설정
def get_emr_job_flow_overrides(date: str, batch: int):
    return {
        "Name": "mainTransformCluster",
        "LogUri": "{{ var.value.emr_base_log_uri }}/{{ ts_nodash }}/",
        "ReleaseLabel": EMR_CONFIG["RELEASE_LABEL"],
        "ServiceRole": "{{ var.value.emr_service_role }}",
        "JobFlowRole": "{{ var.value.emr_ec2_role }}",
        "Instances": {
            "Ec2SubnetId": "{{ var.value.emr_subnet_id }}",
            "Ec2KeyName": "{{ var.value.emr_key_pair }}",
            "EmrManagedMasterSecurityGroup": "{{ var.value.emr_master_sg }}",
            "EmrManagedSlaveSecurityGroup": "{{ var.value.emr_slave_sg }}",
            "InstanceGroups": [
                {
                    "InstanceCount": 1,
                    "InstanceRole": "MASTER",
                    "Name": "Primary",
                    "InstanceType": EMR_CONFIG["INSTANCE_TYPE_MASTER"],
                },
                {
                    "InstanceCount": EMR_CONFIG["CORE_INSTANCE_COUNT"],
                    "InstanceRole": "CORE",
                    "Name": "Core",
                    "InstanceType": EMR_CONFIG["INSTANCE_TYPE_CORE"],
                },
            ],
        },
        "Applications": EMR_CONFIG["APPLICATIONS"],
        "BootstrapActions": [
            {
                "Name": "kss-bootstrap",
                "ScriptBootstrapAction": {
                    "Path": f"s3://{S3_CONFIG_BUCKET}/"
                    + "{{ var.value.emr_bootstrap_script_path }}"
                },
            }
        ],
        "AutoTerminationPolicy": {
            "IdleTimeout": EMR_CONFIG["AUTO_TERMINATION_IDLE_TIMEOUT"]
        },
        "Steps": [
            {
                "Name": "Run Spark Job",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode",
                        "cluster",
                        "--conf",
                        "spark.executorEnv.OPENAI_API_KEY={{ var.value.emr_openai_api_key }}",
                        "--conf",
                        "spark.driver.extraJavaOptions=-DOPENAI_API_KEY={{ var.value.emr_openai_api_key }}",
                        f"s3://{S3_CONFIG_BUCKET}/" + "{{ var.value.emr_script_path }}",
                        "--bucket",
                        f"{S3_BUCKET}",
                        "--input_post_paths",
                        f"combined/*/{date}/{batch}/static/post*.parquet",
                        "--input_comment_paths",
                        f"combined/*/{date}/{batch}/static/comment*.parquet",
                        "--output_dir",
                        f"transformed/{date}/{batch}/",
                    ],
                },
            }
        ],
    }


def create_execute_emr_task(dag, date: str, batch: int):
    return EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=get_emr_job_flow_overrides(date, batch),
        dag=dag,
    )


def create_check_emr_termination_task(dag):
    return EmrJobFlowSensor(
        task_id="check_emr_termination",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster') }}",
        target_states=["TERMINATED", "TERMINATED_WITH_ERRORS"],
        dag=dag,
    )


def create_terminate_emr_cluster_task(dag):
    return EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster') }}",
        dag=dag,
    )

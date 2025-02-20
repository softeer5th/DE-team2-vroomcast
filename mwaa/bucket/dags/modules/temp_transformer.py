# from airflow.providers.amazon.aws.operators.emr import (
#     EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator, Context)
# from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrHook
# from modules.constants import S3_BUCKET, S3_CONFIG_BUCKET

# from utils.xcom import pull_from_xcom

# # EMR 클러스터 설정을 위한 상수
# EMR_CONFIG = {
#     "RELEASE_LABEL": "emr-7.7.0",
#     "INSTANCE_TYPE_MASTER": "m5.xlarge",
#     "INSTANCE_TYPE_CORE": "m5.xlarge",
#     "CORE_INSTANCE_COUNT": 2,
#     "APPLICATIONS": [
#         {"Name": "Hadoop"},
#         {"Name": "Hive"},
#         {"Name": "JupyterEnterpriseGateway"},
#         {"Name": "Livy"},
#         {"Name": "Spark"},
#     ],
#     "AUTO_TERMINATION_IDLE_TIMEOUT": 3600,  # 1시간
# }


# def _get_base_emr_config() -> dict:
#     """
#     EMR 클러스터 구성에 필요한 기본 세팅을 반환하는 함수.
#     실제 job_flow_overrides에 공통적으로 들어갈 부분만 정의한다.
#     """
#     return {
#         "Name": "mainTransformCluster",
#         "LogUri": "{{ var.value.emr_base_log_uri }}/{{ ts_nodash }}/",
#         "ReleaseLabel": EMR_CONFIG["RELEASE_LABEL"],
#         "ServiceRole": "{{ var.value.emr_service_role }}",
#         "JobFlowRole": "{{ var.value.emr_ec2_role }}",
#         "Instances": {
#             "Ec2SubnetId": "{{ var.value.emr_subnet_id }}",
#             "Ec2KeyName": "{{ var.value.emr_key_pair }}",
#             "EmrManagedMasterSecurityGroup": "{{ var.value.emr_master_sg }}",
#             "EmrManagedSlaveSecurityGroup": "{{ var.value.emr_slave_sg }}",
#             "InstanceGroups": [
#                 {
#                     "InstanceCount": 1,
#                     "InstanceRole": "MASTER",
#                     "Name": "Primary",
#                     "InstanceType": EMR_CONFIG["INSTANCE_TYPE_MASTER"],
#                 },
#                 {
#                     "InstanceCount": EMR_CONFIG["CORE_INSTANCE_COUNT"],
#                     "InstanceRole": "CORE",
#                     "Name": "Core",
#                     "InstanceType": EMR_CONFIG["INSTANCE_TYPE_CORE"],
#                 },
#             ],
#         },
#         "Applications": EMR_CONFIG["APPLICATIONS"],
#         "BootstrapActions": [
#             {
#                 "Name": "kss-bootstrap",
#                 "ScriptBootstrapAction": {
#                     "Path": f"s3://{S3_CONFIG_BUCKET}/"
#                     + "{{ var.value.emr_bootstrap_script_path }}"
#                 },
#             }
#         ],
#         "AutoTerminationPolicy": {
#             "IdleTimeout": EMR_CONFIG["AUTO_TERMINATION_IDLE_TIMEOUT"]
#         },
#         # Steps는 여기서 지정하지 않고 추후 동적으로 삽입할 예정.
#         # "Steps": [...]
#     }


# def _create_spark_step(
#     step_name: str, script_s3_path: str, extra_args: list = None
# ) -> dict:
#     if extra_args is None:
#         extra_args = []

#     base_args = [
#         "spark-submit",
#         "--deploy-mode",
#         "cluster",
#         script_s3_path,  # 예: s3://my_config_bucket/my_script.py
#         # "--bucket",
#         # f"{S3_BUCKET}",
#         # "--input_post_paths",
#         # f"combined/*/{date}/{batch}/static/post*.parquet",
#         # "--input_comment_paths",
#         # f"combined/*/{date}/{batch}/static/comment*.parquet",
#         # "--output_dir",
#         # f"transformed/{date}/{batch}/",
#     ]
#     # extra_args가 있으면 뒤에 이어붙임
#     all_args = base_args + extra_args

#     return {
#         "Name": step_name,
#         "ActionOnFailure": "CONTINUE",
#         "HadoopJarStep": {
#             "Jar": "command-runner.jar",
#             "Args": all_args,
#         },
#     }


# def _assemble_job_flow_overrides(base_config: dict, steps: list) -> dict:
#     """
#     base_config(클러스터 설정)과 steps 리스트를 합쳐 최종 job_flow_overrides를 생성한다.
#     """
#     # 사본을 만들어서 steps를 주입
#     config_with_steps = base_config.copy()
#     config_with_steps["Steps"] = steps
#     return config_with_steps


# def _define_transform_static_step(date: str, batch: int) -> dict:
#     transform_static_step = _create_spark_step(
#         step_name="transform_static",
#         script_s3_path=f"s3://{S3_CONFIG_BUCKET}/" + "{{ var.value.emr_static_script_path }}",
#         extra_args=[
#             "--bucket",
#             f"{S3_BUCKET}",
#             "--input_post_paths",
#             f"combined/*/{date}/{batch}/static/post*.parquet",
#             "--input_comment_paths",
#             f"combined/*/{date}/{batch}/static/comment*.parquet",
#             "--output_dir",
#             f"transformed/{date}/{batch}/",
#         ],
#     )

#     return transform_static_step

# def _define_transform_dynamic_step(prev_date: str, prev_batch: int, date: str, batch: int) -> dict:
#     transform_dynamic_step = _create_spark_step(
#         step_name="transform_dynamic",
#         script_s3_path=f"s3://{S3_CONFIG_BUCKET}/" + "{{ var.value.emr_dynamic_script_path }}",
#         extra_args=[
#             "--bucket",
#             f"{S3_BUCKET}",
#             "--before_dynamic_posts",
#             f"combined",
#             "--after_dynamic_posts",
#             f"combined"
#             "--before_dynamic_comments",
#             f"combined/",
#             "--after_dynamic_comments",
#             f"combined",
#         ]
#     )


# class CustomEmrCreateJobFlowOperator(EmrCreateJobFlowOperator):
#     def execute(self, context: Context) -> str:
#         prev_batch = pull_from_xcom("synchronize_task", "prev_batch", **context)

#         # base config 설정
#         base_config = _get_base_emr_config()

#         # steps 동적 생성
#         steps = [{
#             "Name": "transform_static",
#             "ActionOnFailure": "CONTINUE",
#             "HadoopJarStep": {
#                 "Jar": "command-runner.jar",
#                 "Args": [
#                     "spark-submit",
#                     "--deploy-mode", "cluster",
#                     f"s3://{S3_CONFIG_BUCKET}/scripts/transform_static.py",
#                     "--bucket", f"{S3_BUCKET}",
#                     "--input_post_paths", f"combined/*/{date}/{batch}/static/post*.parquet",
#                     "--input_comment_paths", f"combined/*/{date}/{batch}/static/comment*.parquet",
#                     "--output_dir", f"transformed/{date}/{batch}/",
#                 ]
#             }
#         }]

#         # config와 steps 조립
#         self.job_flow_overrides = _assemble_job_flow_overrides(base_config, steps)

#         # 부모 클래스의 execute 호출
#         return super().execute(context)

# def create_execute_emr_task(dag, date: str, batch: int):
#     base_config = _get_base_emr_config()
#     steps = _define_emr_steps(date, batch)
#     config_with_steps = _assemble_job_flow_overrides(base_config, steps)

#     return EmrCreateJobFlowOperator(
#         task_id="create_emr_cluster",
#         job_flow_overrides=config_with_steps,
#         dag=dag,
#     )


# def create_check_emr_termination_task(dag):
#     return EmrJobFlowSensor(
#         task_id="check_emr_termination",
#         job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster') }}",
#         target_states=["TERMINATED", "TERMINATED_WITH_ERRORS"],
#         dag=dag,
#     )


# def create_terminate_emr_cluster_task(dag):
#     return EmrTerminateJobFlowOperator(
#         task_id="terminate_emr_cluster",
#         job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster') }}",
#         dag=dag,
#     )

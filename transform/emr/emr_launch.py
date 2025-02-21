import boto3
import os
from datetime import datetime
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 현재 날짜 기반으로 로그 저장 디렉토리 생성
current_date = datetime.now().strftime("%Y-%m-%d-%H%M%S")
base_log_uri = os.getenv("BASE_LOG_URI")
log_uri = f"{base_log_uri}/{current_date}/"

# EMR 클라이언트 생성
emr_client = boto3.client("emr", region_name=os.getenv("AWS_REGION"))

# 클러스터 생성 요청
response = emr_client.run_job_flow(
    Name="mainTransformCluster",
    LogUri=log_uri,
    ReleaseLabel="emr-7.7.0",
    ServiceRole=os.getenv("EMR_SERVICE_ROLE"),
    JobFlowRole=os.getenv("EMR_EC2_ROLE"),
    Instances={
        "Ec2SubnetId": os.getenv("EMR_SUBNET_ID"),
        "Ec2KeyName": os.getenv("EMR_KEY_PAIR"),
        "EmrManagedMasterSecurityGroup": os.getenv("EMR_MASTER_SG"),
        "EmrManagedSlaveSecurityGroup": os.getenv("EMR_SLAVE_SG"),
        "InstanceGroups": [
            {
                "InstanceCount": 1,
                "InstanceRole": "MASTER",
                "Name": "Primary",
                "InstanceType": "m5.xlarge",
            },
            {
                "InstanceCount": 2,
                "InstanceRole": "CORE",
                "Name": "Core",
                "InstanceType": "m5.xlarge",
            },
        ],
    },
    Applications=[{"Name": "Hadoop"}, {"Name": "Hive"}, {"Name": "JupyterEnterpriseGateway"}, {"Name": "Livy"}, {"Name": "Spark"}],
    BootstrapActions=[
        {
            "Name": "kss-bootstrap",
            "ScriptBootstrapAction": {"Path": os.getenv("BOOTSTRAP_SCRIPT_PATH")},
        }
    ],
    AutoTerminationPolicy={"IdleTimeout": 3600},  # 1시간 후 자동 종료
    Steps=[
        {
            "Name": "Run Spark Job",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--conf", f"spark.executorEnv.OPENAI_API_KEY={os.getenv('OPENAI_API_KEY')}",
                    "--conf", f"spark.driver.extraJavaOptions=-DOPENAI_API_KEY={os.getenv('OPENAI_API_KEY')}",
                    s3_script_path,
                    "--bucket", bucket,
                    "--input_post_paths", input_post_path,
                    "--input_comment_paths", input_comment_path,
                    "--output_dir", output_dir
                ],
            },
        }
    ]
)

print(f"EMR Cluster created with ID: {response['JobFlowId']}")
print(f"Logs will be stored in: {log_uri}")
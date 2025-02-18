import json
import os

from airflow.models import Variable

# AIRFLOW_VAR_로 시작하는 환경 변수는 자동으로 Airflow Variable이 됨
S3_BUCKET = Variable.get("S3_BUCKET")

BATCH_INTERVAL_MINUTES = int(Variable.get("BATCH_INTERVAL_MINUTES"))
BATCH_DURATION_HOURS = int(Variable.get("BATCH_DURATION_HOURS"))

# 설정 파일 경로
CONFIG_PATH = os.path.join(os.getenv("AIRFLOW_HOME"), "configs")


def _load_config(filename: str) -> dict | list:
    """설정 파일을 로드하는 함수"""
    try:
        with open(f"{CONFIG_PATH}/{filename}", "r") as f:
            return json.load(f)
    except Exception as e:
        if filename == "car.json":
            return [
                {"car_id": "santafe", "keywords": ["싼타페", "산타페"]},
                {"car_id": "avante", "keywords": ["아반떼", "애반떼"]},
            ]
        if filename == "community.json":
            return ["bobaedream", "clien"]
        raise Exception(f"Failed to load config file {filename}: {str(e)}")


# 설정 파일 로드
CARS = {item["car_id"]: item["keywords"] for item in _load_config("car.json")}
COMMUNITIES = _load_config("community.json")

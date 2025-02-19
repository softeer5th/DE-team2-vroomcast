import json
import logging
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from post_extractor import extract_post
from post_info_list_extractor import get_post_infos

logger = logging.getLogger()
logger.setLevel(logging.INFO)

COMMUNITY = "bobaedream"
SAVE_PATH = "extracted/{car_id}/{date}/{batch}/raw/{community}/{post_id}.json"


def _save_to_s3(post: dict, bucket: str, key: str):
    s3_client = boto3.client("s3")
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(post, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
            ContentEncoding="utf-8",
        )
    except ClientError as e:
        logger.error(f"Error saving to S3: {e}")
        raise


def _extract(bucket: str, car_id: str, keyword: str, date: str, batch: int, start_datetime: str, end_datetime: str) -> None:
    post_infos = get_post_infos(keyword, start_datetime, end_datetime)

    for post_info in post_infos:
        post = extract_post(post_info["url"], str(post_info["id"]), start_datetime, end_datetime)
        if not post:
            continue
        s3_key = SAVE_PATH.format(
            car_id=car_id, date=date, batch=batch, community=COMMUNITY, post_id=post_info["id"]
        )
        _save_to_s3(post, bucket, s3_key)
        logger.info(f"Saved to S3: {s3_key}")


def lambda_handler(event, context):
    start_time = datetime.now()

    try:
        bucket = event.get("bucket")
        car_id = event.get("car_id")
        keywords = event.get("keywords")
        date = event.get("date")
        batch = event.get("batch")
        start_datetime = event.get("start_datetime")
        end_datetime = event.get("end_datetime")

        if not all([bucket, car_id, keywords, date, batch, start_datetime, end_datetime]):
            raise ValueError("Missing required fields")

        for keyword in keywords:
            _extract(bucket, car_id, keyword, date, batch, start_datetime, end_datetime)

        end_time = datetime.now()
        duration = end_time - start_time

        return {
            "statusCode": 200,
            "body": {
                "success": True,
                "end_time": end_time.isoformat(),
                "duration": str(duration),
                "car_id": car_id,
                "date": date,
                "batch": batch,
                "start_datetime": start_datetime,
                "end_datetime": end_datetime,
            },
        }
    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        end_time = datetime.now()
        duration = end_time - start_time

        return {
            "statusCode": 500,
            "body": {
                "success": False,
                "end_time": end_time.isoformat(),
                "duration": str(duration),
                "car_id": car_id if car_id else None,
                "date": date,
                "batch": batch,
                "start_datetime": start_datetime,
                "end_datetime": end_datetime,
            },
        }

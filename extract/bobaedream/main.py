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


def _extract(bucket: str, car_id: str, keyword: str, date: str, batch: int, start_datetime: str, end_datetime: str) -> tuple[int, int]:
    post_infos = get_post_infos(keyword, start_datetime, end_datetime)

    attempted_posts_count = 0
    extracted_posts_count = 0

    for post_info in post_infos:
        post, is_success = extract_post(post_info["url"], str(post_info["id"]), start_datetime, end_datetime)
        if not is_success:
            attempted_posts_count += 1
            continue

        if not post:
            continue
        
        s3_key = SAVE_PATH.format(
            car_id=car_id, date=date, batch=batch, community=COMMUNITY, post_id=post_info["id"]
        )
        try:
            _save_to_s3(post, bucket, s3_key)
        except Exception as e:
            attempted_posts_count += 1
            continue

        attempted_posts_count += 1
        extracted_posts_count += 1

        logger.info(f"Saved to S3: {s3_key}")
        logger.info(f"Post ID and Date {post['post_id']}, {post['created_at']}")

    return attempted_posts_count, extracted_posts_count


def lambda_handler(event, context):
    start_time = datetime.now()

    bucket = event.get("bucket")
    car_id = event.get("car_id")
    keywords = event.get("keywords")
    date = event.get("date")
    batch = event.get("batch")
    start_datetime = event.get("start_datetime")
    end_datetime = event.get("end_datetime")

    attempted_posts_count = 0
    extracted_posts_count = 0

    try:
        if any(param is None for param in [bucket, car_id, keywords, date, batch, start_datetime, end_datetime]):
            raise ValueError("Missing required fields")
        
        for keyword in keywords:
            attempted, extracted = _extract(bucket, car_id, keyword, date, batch, start_datetime, end_datetime)
            attempted_posts_count += attempted
            extracted_posts_count += extracted

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
                "attempted_posts_count": attempted_posts_count,
                "extracted_posts_count": extracted_posts_count,
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
                "attempted_posts_count": attempted_posts_count,
                "extracted_posts_count": extracted_posts_count,
                "error": str(e),
            },
        }

import json
import logging
import os
from datetime import datetime
from itertools import islice
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

EXTRACTED_PATH = "extracted/{car_id}/{date}/raw/"

POST_PATH = "combined/{car_id}/{date}/post.parquet"
COMMENT_PATH = "combined/{car_id}/{date}/comment.parquet"

POST_SCHEMA = pa.schema(
    [
        pa.field("post_id", pa.int64(), nullable=False),
        pa.field("post_url", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
        pa.field("content", pa.string(), nullable=False),
        pa.field("created_at", pa.timestamp('s'), nullable=False),
        pa.field("view_count", pa.int64(), nullable=True),
        pa.field("upvote_count", pa.int64(), nullable=True),
        pa.field("downvote_count", pa.int64(), nullable=True),
        pa.field("comment_count", pa.int64(), nullable=True),
    ]
)

COMMENT_SCHEMA = pa.schema(
    [
        pa.field("comment_id", pa.int64(), nullable=False),
        pa.field("post_id", pa.int64(), nullable=False),
        pa.field("content", pa.string(), nullable=False),
        pa.field("is_reply", pa.bool_(), nullable=True),
        pa.field("created_at", pa.timestamp('s'), nullable=False),
        pa.field("upvote_count", pa.int64(), nullable=True),
        pa.field("downvote_count", pa.int64(), nullable=True),
    ]
)

def _parse_datetime(date_str: str) -> datetime:
    # ISO 형식의 문자열을 datetime으로 변환
    return datetime.fromisoformat(date_str.replace('Z', '+00:00'))

def _get_extracted_data_paths(s3: Any, bucket: str, car_id: str, date: str):
    response = s3.list_objects_v2(
        Bucket=bucket, Prefix=EXTRACTED_PATH.format(car_id=car_id, date=date)
    )

    if "Contents" not in response:
        raise ValueError("No data found in the extracted directory")

    matched_files = []
    for obj in response.get("Contents", []):
        key: str = obj["Key"]
        if key.endswith(".json"):  # 패턴의 마지막 부분 체크
            matched_files.append(key)

    return matched_files


def _read_extracted_data(s3: Any, bucket: str, car_id: str, date: str):
    paths = _get_extracted_data_paths(s3, bucket, car_id, date)
    if not paths:
        logging.info("No data found in the extracted directory")
        return

    for path in paths:
        try:
            response = s3.get_object(Bucket=bucket, Key=path)
            content = response["Body"].read().decode("utf-8")
            data = json.loads(content)
            yield data
        except Exception as e:
            logging.error(f"Failed to process file {path}: {str(e)}")
            raise


def _split_data(data: dict) -> tuple[dict, list[dict]]:
    post = {
        "post_id": data["post_id"],
        "post_url": data["post_url"],
        "title": data["title"],
        "content": data["content"],
        "created_at": _parse_datetime(data["created_at"]),
        "view_count": data.get("view_count", None),
        "upvote_count": data.get("upvote_count", None),
        "downvote_count": data.get("downvote_count", None),
        "comment_count": data.get("comment_count", None),
    }

    post_comments = []
    for comment in data["comments"]:
        comment_data = {
            "comment_id": comment["comment_id"],
            "post_id": data["post_id"],
            "content": comment["content"],
            "is_reply": comment["is_reply"],
            "created_at": _parse_datetime(comment["created_at"]),
            "upvote_count": comment.get("upvote_count", None),
            "downvote_count": comment.get("downvote_count", None),
        }
        post_comments.append(comment_data)

    return post, post_comments


def _store_combined_data(
    car_id: str, date: str, posts: list[dict], comments: list[dict]
):
    posts_table = pa.Table.from_pylist(posts, schema=POST_SCHEMA)
    comments_table = pa.Table.from_pylist(comments, schema=COMMENT_SCHEMA)

    post_tmp_path = os.path.join("/tmp", POST_PATH.format(car_id=car_id, date=date))
    comment_tmp_path = os.path.join(
        "/tmp", COMMENT_PATH.format(car_id=car_id, date=date)
    )

    os.makedirs(os.path.dirname(post_tmp_path), exist_ok=True)
    os.makedirs(os.path.dirname(comment_tmp_path), exist_ok=True)

    try:
        existing_posts = pq.read_table(post_tmp_path)
        existing_comments = pq.read_table(comment_tmp_path)

        posts_table = pa.concat_tables([existing_posts, posts_table])
        comments_table = pa.concat_tables([existing_comments, comments_table])
    except (FileNotFoundError, OSError):
        pass

    pq.write_table(posts_table, post_tmp_path)
    pq.write_table(comments_table, comment_tmp_path)


def _write_combined_data(s3: Any, bucket: str, car_id: str, date: str):
    post_s3_path = POST_PATH.format(car_id=car_id, date=date)
    comment_s3_path = COMMENT_PATH.format(car_id=car_id, date=date)

    post_tmp_path = os.path.join("/tmp", post_s3_path)
    comment_tmp_path = os.path.join("/tmp", comment_s3_path)

    try:
        with open(post_tmp_path, "rb") as f:
            s3.upload_fileobj(f, bucket, post_s3_path)
        with open(comment_tmp_path, "rb") as f:
            s3.upload_fileobj(f, bucket, comment_s3_path)

        os.remove(post_tmp_path)
        os.remove(comment_tmp_path)

        logging.info(
            f"Successfully uploaded combined data for car_id: {car_id}, date: {date}"
        )
    except Exception as e:
        logging.error(f"Error uploading combined data: {str(e)}")
        raise


def combine(bucket: str, car_id: str, date: str):
    s3 = boto3.client("s3")

    extracted_data = _read_extracted_data(s3, bucket, car_id, date)

    batch_size = 30

    while True:
        batch = list(islice(extracted_data, batch_size))
        posts = []
        comments = []

        for data in batch:
            post, post_comments = _split_data(data)
            posts.append(post)
            comments.extend(post_comments)

        if not posts:
            break

        _store_combined_data(car_id, date, posts, comments)

    _write_combined_data(s3, bucket, car_id, date)


def lambda_handler(event, context):
    start_time = datetime.now()
    try:
        logging.basicConfig(level=logging.INFO)

        bucket = event.get("bucket")
        car_id = event.get("car_id")
        date = event.get("date")

        if not all([bucket, car_id, date]):
            raise ValueError("Missing required input")

        end_time = datetime.now()
        duration = end_time - start_time

        combine(bucket, car_id, date)

        return {
            "statusCode": 200,
            "body": {
                "success": True,
                "end_time": end_time.isoformat(),
                "duration": duration.total_seconds(),
                "car_id": car_id,
                "date": date,
            },
        }

    except Exception as e:
        logging.error(f"Error in lambda_handler: {e}")

        end_time = datetime.now()
        duration = end_time - start_time

        return {
            "statusCode": 500,
            "body": {
                "success": False,
                "end_time": end_time.isoformat(),
                "duration": duration.total_seconds(),
                "car_id": car_id,
                "date": date,
                "error": str(e),
            },
        }
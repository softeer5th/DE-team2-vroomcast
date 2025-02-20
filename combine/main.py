import json
import logging
import os
from datetime import datetime
from itertools import islice
import re
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger()
logger.setLevel(logging.INFO)

EXTRACTED_PATH = "extracted/{car_id}/{date}/{batch}/raw/"
ID_PATH = "id_set.txt"

POST_PATH = "combined/{car_id}/{date}/{batch}/{type}/post_{chunk}.parquet"
COMMENT_PATH = "combined/{car_id}/{date}/{batch}/{type}/comment_{chunk}.parquet"

POST_STATIC_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("url", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
        pa.field("content", pa.string(), nullable=False),
        pa.field("created_at", pa.timestamp('s'), nullable=False),
    ]
)

POST_DYNAMIC_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("extracted_at", pa.timestamp('s'), nullable=False),
        pa.field("view_count", pa.int64(), nullable=True),
        pa.field("upvote_count", pa.int64(), nullable=True),
        pa.field("downvote_count", pa.int64(), nullable=True),
        pa.field("comment_count", pa.int64(), nullable=True),
    ]
)

COMMENT_STATIC_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("post_id", pa.string(), nullable=False),
        pa.field("content", pa.string(), nullable=False),
        pa.field("is_reply", pa.bool_(), nullable=False),
        pa.field("created_at", pa.timestamp('s'), nullable=False),
    ]
)

COMMENT_DYNAMIC_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("extracted_at", pa.timestamp('s'), nullable=False),
        pa.field("upvote_count", pa.int64(), nullable=True),
        pa.field("downvote_count", pa.int64(), nullable=True),
    ]
)

POST_CAR_SCHEMA = pa.schema(
    [
        pa.field("car_id", pa.int64(), nullable=False),
        pa.field("post_id", pa.int64(), nullable=False),
    ]
)

def _parse_datetime(date_str: str) -> datetime:
    return datetime.fromisoformat(date_str.replace('Z', '+00:00'))

def _extract_community_from_path(path: str) -> str:
    # extracted/{car_id}/{date}/raw/{community}/{post_id}.json 형식에서 community 추출
    match = re.search(r'raw/([^/]+)/[^/]+\.json$', path)
    if not match:
        raise ValueError(f"Invalid path format: {path}")
    return match.group(1)

def _get_extracted_data_paths(s3: Any, bucket: str, car_id: str, date: str, batch: int):
    response = s3.list_objects_v2(
        Bucket=bucket, Prefix=EXTRACTED_PATH.format(car_id=car_id, date=date, batch=batch)
    )

    if "Contents" not in response:
        raise ValueError("No data found in the extracted directory")

    matched_files = []
    for obj in response.get("Contents", []):
        key: str = obj["Key"]
        if key.endswith(".json"):
            matched_files.append(key)

    return matched_files

def read_id_set(s3: Any, bucket: str) -> set[str]:
    try:
        response = s3.get_object(Bucket=bucket, Key=ID_PATH)
        text: str = response["Body"].read().decode("utf-8")
        id_set = set(line.strip() for line in text.splitlines() if line.strip())
        return id_set
    except s3.exceptions.NoSuchKey:
        return set()

def _read_extracted_data(s3: Any, bucket: str, car_id: str, date: str, batch: int):
    paths = _get_extracted_data_paths(s3, bucket, car_id, date, batch)
    if not paths:
        logger.info("No data found in the extracted directory")
        return

    for path in paths:
        try:
            community = _extract_community_from_path(path)
            response = s3.get_object(Bucket=bucket, Key=path)
            content = response["Body"].read().decode("utf-8")
            data = json.loads(content)
            data['community'] = community
            yield data
        except Exception as e:
            logger.error(f"Failed to process file {path}: {str(e)}")
            raise

def _split_data(data: dict, batch_datetime: str) -> tuple[dict, list[dict]]:
    community = data['community']
    post_id = f"{community}_post_{data['post_id']}"
    
    post_static = {
        "id": post_id,
        "url": data["post_url"],
        "title": data["title"],
        "content": data["content"],
        "created_at": _parse_datetime(data["created_at"]),
    }

    post_dynamic = {
        "id": post_id,
        "extracted_at": _parse_datetime(batch_datetime),
        "view_count": data.get("view_count", None),
        "upvote_count": data.get("upvote_count", None),
        "downvote_count": data.get("downvote_count", None),
        "comment_count": data.get("comment_count", None),
    }

    post = {
        "post_static": post_static,
        "post_dynamic": post_dynamic,
    }

    comments = []
    for comment in data["comments"]:
        comment_id = f"{community}_comment_{comment['comment_id']}"

        comment_static = {
            "id": comment_id,
            "post_id": post_id,
            "content": comment["content"],
            "is_reply": comment["is_reply"],
            "created_at": _parse_datetime(comment["created_at"]),
        }

        comment_dynamic = {
            "id": comment_id,
            "extracted_at": _parse_datetime(batch_datetime),
            "upvote_count": comment.get("upvote_count", None),
            "downvote_count": comment.get("downvote_count", None),
        }

        comments.append({
            "comment_static": comment_static,
            "comment_dynamic": comment_dynamic,
        })

    return post, comments

def _upload_id_set(s3: Any, bucket: str, id_set: set[str]):
    try:
        text = "\n".join(id_set)
        s3.put_object(Bucket=bucket, Key=ID_PATH, Body=text)
    except Exception as e:
        logger.error(f"Error uploading id set: {str(e)}")

def combine(bucket: str, car_id: str, date: str, batch: int, batch_datetime: str):
    s3 = boto3.client("s3")
    extracted_data = _read_extracted_data(s3, bucket, car_id, date, batch)
    id_set = read_id_set(s3, bucket)

    def _upload_data(data: list[dict], schema: Any, path: str):
        try:
            table = pa.Table.from_pylist(data, schema=schema)
            
            buffer = pa.BufferOutputStream()
            pq.write_table(table, buffer)
            
            s3.put_object(
                Bucket=bucket,
                Key=path,
                Body=buffer.getvalue().to_pybytes()
            )
        except Exception as e:
            logger.error(f"Error uploading data: {str(e)}")

    chunk_idx = 0
    chunk_size = 200

    while True:
        chunk = list(islice(extracted_data, chunk_size))
        if not chunk:
            break

        post_statics = []
        post_dynamics = []
        comment_statics = []
        comment_dynamics = []

        for data in chunk:
            post, comments = _split_data(data, batch_datetime)

            if post["post_static"]["id"] not in id_set:
                post_statics.append(post["post_static"])
                id_set.add(post["post_static"]["id"])
            post_dynamics.append(post["post_dynamic"])

            for comment in comments:
                if comment["comment_static"]["id"] not in id_set:
                    comment_statics.append(comment["comment_static"])
                    id_set.add(comment["comment_static"]["id"])
                comment_dynamics.append(comment["comment_dynamic"])

        logger.info(f"Uploading chunk {chunk_idx} for car_id: {car_id}, date: {date}")
        logger.info(f"Post statics: {len(post_statics)}, Post dynamics: {len(post_dynamics)}")
        logger.info(f"Comment statics: {len(comment_statics)}, Comment dynamics: {len(comment_dynamics)}")
                        
        if post_statics:
            _upload_data(post_statics, POST_STATIC_SCHEMA, POST_PATH.format(car_id=car_id, date=date, batch=batch, type="static", chunk=chunk_idx))

        if comment_statics:
            _upload_data(comment_statics, COMMENT_STATIC_SCHEMA, COMMENT_PATH.format(car_id=car_id, date=date, batch=batch, type="static", chunk=chunk_idx))

        if post_dynamics:
            _upload_data(post_dynamics, POST_DYNAMIC_SCHEMA, POST_PATH.format(car_id=car_id, date=date, batch=batch, type="dynamic", chunk=chunk_idx))

        if comment_dynamics:
            _upload_data(comment_dynamics, COMMENT_DYNAMIC_SCHEMA, COMMENT_PATH.format(car_id=car_id, date=date, batch=batch, type="dynamic", chunk=chunk_idx))

        chunk_idx += 1

    _upload_id_set(s3, bucket, id_set)

def lambda_handler(event, context):
    start_time = datetime.now()
    try:

        bucket = event.get("bucket")
        car_id = event.get("car_id")
        date = event.get("date")
        batch = event.get("batch")
        batch_datetime = event.get("batch_datetime")

        if any([arg is None for arg in [bucket, car_id, date, batch, batch_datetime]]):
            raise ValueError("Missing required input")

        combine(bucket, car_id, date, batch, batch_datetime)

        end_time = datetime.now()
        duration = end_time - start_time

        return {
            "statusCode": 200,
            "body": {
                "success": True,
                "end_time": end_time.isoformat(),
                "duration": duration.total_seconds(),
                "car_id": car_id,
                "date": date,
                "batch": batch,
                "batch_datetime": batch_datetime,
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
                "duration": duration.total_seconds(),
                "car_id": car_id,
                "date": date,
                "batch": batch,
                "batch_datetime": batch_datetime,
                "error": str(e),
            },
        }
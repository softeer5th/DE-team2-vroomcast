from datetime import datetime
from itertools import islice
import json
from typing import Any
import boto3
import os

import pyarrow as pa

import logging

EXTRACTED_PATH = "extracted/{car_id}/{date}/raw/"

POST_PATH = "combined/{car_id}/{date}/post.parquet"
COMMENT_PATH = "combined/{car_id}/{date}/comment.parquet"

POST_SCHEMA = pa.schema([
    ('post_id', pa.string()),
    ('post_url', pa.string()),
    ('title', pa.string()),
    ('content', pa.string()),
    ('created_at', pa.timestamp('us')),  # microsecond precision
    ('view_count', pa.int64()),
    ('upvote_count', pa.int64()),
    ('downvote_count', pa.int64()),
    ('comment_count', pa.int64())
])

COMMENT_SCHEMA = pa.schema([
    ('comment_id', pa.string()),
    ('post_id', pa.string()),
    ('content', pa.string()),
    ('is_reply', pa.bool_()),
    ('created_at', pa.timestamp('us')),
    ('upvote_count', pa.int64()),
    ('downvote_count', pa.int64())
])

def _get_extracted_data_paths(s3: Any, bucket: str, car_id: str, date: str):
    response = s3.list_objects_v2(
        Bucket='your-bucket-name',
        Prefix=EXTRACTED_PATH.format(car_id=car_id, date=date)
    )

    if "Contents" not in response:
        raise ValueError("No data found in the extracted directory")
    
    matched_files = []
    for obj in response.get('Contents', []):
        key: str = obj['Key']
        if key.endswith('.json'):  # 패턴의 마지막 부분 체크
            matched_files.append(key)
                
    return matched_files

def _read_extracted_data(s3: Any, bucket: str, car_id: str, date: str):
    paths = _get_extracted_data_paths(s3, bucket, car_id, date)
    if not paths:
        logging.info("No data found in the extracted directory")
        return
    for path in paths:
        obj = s3.get_object(Bucket=bucket, Key=path)
        data = obj["Body"].read().decode("utf-8")
        yield json.loads(data)

def _split_data(data: dict) -> tuple[dict, list[dict]]:
    post = {
        "post_id": data["post_id"],
        "post_url": data["post_url"],
        "title": data["title"],
        "content": data["content"],
        "created_at": data["created_at"],
        "view_count": data["view_count"],
        "upvote_count": data["upvote_count"],
        "downvote_count": data["downvote_count"],
        "comment_count": data["comment_count"],
    }

    post_comments = []
    for comment in data["comments"]:
        comment = {
            "comment_id": comment["comment_id"],
            "post_id": data["post_id"],
            "content": comment["content"],
            "is_reply": comment["is_reply"],
            "created_at": comment["created_at"],
            "upvote_count": comment["upvote_count"],
            "downvote_count": comment["downvote_count"],
        }
        post_comments.append(comment)

    return post, post_comments

def _store_combined_data(car_id: str, date: str, posts: list[dict], comments: list[dict]):
    posts_table = pa.Table.from_pylist(posts, schema=POST_SCHEMA)
    comments_table = pa.Table.from_pylist(comments, schema=COMMENT_SCHEMA)
    
    post_tmp_path = os.path.join("/tmp", POST_PATH.format(car_id=car_id, date=date))
    comment_tmp_path = os.path.join("/tmp", COMMENT_PATH.format(car_id=car_id, date=date))
    
    os.makedirs(os.path.dirname(post_tmp_path), exist_ok=True)
    os.makedirs(os.path.dirname(comment_tmp_path), exist_ok=True)
    
    try:
        existing_posts = pa.parquet.read_table(post_tmp_path)
        existing_comments = pa.parquet.read_table(comment_tmp_path)
        
        posts_table = pa.concat_tables([existing_posts, posts_table])
        comments_table = pa.concat_tables([existing_comments, comments_table])
    except (FileNotFoundError, OSError):
        pass
    
    pa.parquet.write_table(posts_table, post_tmp_path)
    pa.parquet.write_table(comments_table, comment_tmp_path)

def _write_combined_data(s3: Any, bucket: str, car_id: str, date: str):
    post_tmp_path = os.path.join("/tmp", POST_PATH.format(car_id=car_id, date=date))
    comment_tmp_path = os.path.join("/tmp", COMMENT_PATH.format(car_id=car_id, date=date))
    
    post_s3_path = POST_PATH.format(car_id=car_id, date=date)
    comment_s3_path = COMMENT_PATH.format(car_id=car_id, date=date)
    
    try:
        # Upload to S3
        with open(post_tmp_path, 'rb') as f:
            s3.upload_fileobj(f, bucket, post_s3_path)
        with open(comment_tmp_path, 'rb') as f:
            s3.upload_fileobj(f, bucket, comment_s3_path)
            
        # Clean up temporary files
        os.remove(post_tmp_path)
        os.remove(comment_tmp_path)
        
        # Remove empty directories
        os.removedirs(os.path.dirname(post_tmp_path))
        os.removedirs(os.path.dirname(comment_tmp_path))
        
        logging.info(f"Successfully uploaded combined data for car_id: {car_id}, date: {date}")
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
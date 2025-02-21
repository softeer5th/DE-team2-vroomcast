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
    """
    Uploads a post dictionary as a JSON object to the specified S3 bucket.
    
    Args:
        post (dict): Post data to be saved. Must be JSON serializable.
        bucket (str): Name of the S3 bucket.
        key (str): S3 object key where the post will be stored.
    
    Raises:
        ClientError: If an error occurs during the S3 put_object operation.
    """
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
    """
    Extracts post content and saves it to S3, returning counts of attempted and saved posts.
    
    Retrieves post metadata using the provided keyword and datetime range, then iterates over
    each post's URL to extract content. If post extraction succeeds and valid content is returned,
    formats the S3 key with the car ID, date, batch, and post ID, and attempts to save the post
    to the specified S3 bucket. Posts successfully saved are logged along with their post ID and
    creation date. The function tracks both total extraction attempts and successful saves, returning
    them as a tuple.
    
    Parameters:
        bucket (str): Name of the S3 bucket to upload posts.
        car_id (str): Identifier associated with the car for which posts are extracted.
        keyword (str): Search keyword to filter post metadata retrieval.
        date (str): Date string used in constructing the S3 key.
        batch (int): Batch number representing the current extraction iteration.
        start_datetime (str): Start datetime (inclusive) for filtering posts.
        end_datetime (str): End datetime (inclusive) for filtering posts.
    
    Returns:
        tuple[int, int]: A tuple where the first element is the total count of attempted extractions
        and the second element is the count of posts successfully extracted and saved.
    """
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
    """
    AWS Lambda handler for post extraction and S3 upload.
    
    Extracts required parameters from the event—namely, 'bucket', 'car_id', 'keywords', 
    'date', 'batch', 'start_datetime', and 'end_datetime'—and iterates over each keyword 
    to extract posts from the specified source. Aggregates counts of attempted and successfully 
    extracted posts, calculates the operation's duration, and returns a structured response 
    with execution details. In case of errors (e.g., missing parameters or extraction issues), 
    logs the error and returns a response with a 500 status code and error information.
    
    Args:
        event (dict): Dictionary containing input parameters for extraction. Required keys:
            - "bucket" (str): The S3 bucket name.
            - "car_id" (str): Identifier for the car.
            - "keywords" (list): List of keywords to search posts.
            - "date" (str): Date for which to extract posts.
            - "batch" (int): Batch number for the extraction process.
            - "start_datetime" (str): ISO formatted start date and time.
            - "end_datetime" (str): ISO formatted end date and time.
        context: AWS Lambda context object providing runtime information.
    
    Returns:
        dict: Response dictionary with a "statusCode" and a "body" that includes:
            - On success (HTTP 200): "success" flag set to True, ISO formatted "end_time", 
              execution "duration", provided input parameters, and counts for "attempted_posts_count" 
              and "extracted_posts_count".
            - On error (HTTP 500): "success" flag set to False, ISO formatted "end_time", execution 
              "duration", provided input parameters (if available), counts for attempted and extracted 
              posts, and an "error" message detailing the exception.
    """
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

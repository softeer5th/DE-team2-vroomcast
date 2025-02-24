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


def _save_to_s3(post: dict, bucket: str, key: str) -> None:
    """
    S3에 데이터 저장
    Args:
        post (dict): 게시글 정보
        bucket (str): S3 버킷 이름
        key (str): S3 키
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


def _extract(
    bucket: str,
    car_id: str,
    keyword: str,
    date: str,
    batch: int,
    start_datetime: str,
    end_datetime: str,
) -> tuple[int, int]:
    """
    게시글 추출
    Args:
        bucket (str): S3 버킷 이름
        car_id (str): 차량 ID
        keyword (str): 검색 키워드
        date (str): 날짜
        batch (int): 배치
        start_datetime (str): 시작 날짜
        end_datetime (str): 종료 날짜
    Returns:
        tuple: 시도한 게시글 수, 추출한 게시글 수
    """

    # 게시글 정보 목록
    post_infos = get_post_infos(keyword, start_datetime, end_datetime)

    # 시도한 게시글 수, 추출한 게시글 수
    attempted_posts_count = 0
    extracted_posts_count = 0

    # 게시글 정보 목록 순회
    for post_info in post_infos:
        # 게시글 추출 시도 & 성공 여부 확인
        post, is_success = extract_post(
            post_info["url"], str(post_info["id"]), start_datetime, end_datetime
        )

        # Request 실패 시 시도 횟수 증가
        if not is_success:
            attempted_posts_count += 1
            continue

        # Request는 성공했으나 추출할 필요가 없었던 경우는 아무 값도 증가하지 않음
        if not post:
            continue

        # S3 경로 생성
        s3_key = SAVE_PATH.format(
            car_id=car_id,
            date=date,
            batch=batch,
            community=COMMUNITY,
            post_id=post_info["id"],
        )

        # S3에 저장 시도
        try:
            _save_to_s3(post, bucket, s3_key)
        except Exception as e:
            # 실패 시 시도 횟수 증가
            attempted_posts_count += 1
            continue

        # 저장 성공 시 시도 횟수와 추출 횟수 증가
        attempted_posts_count += 1
        extracted_posts_count += 1

        logger.info(f"Saved to S3: {s3_key}")
        logger.info(f"Post ID and Date {post['post_id']}, {post['created_at']}")

    return attempted_posts_count, extracted_posts_count


def lambda_handler(event, context) -> dict:
    """
    Lambda 핸들러
    Args:
        event (dict): Lambda 이벤트
        context (object): Lambda 컨텍스트
    Returns:
        dict: Lambda 응답
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
        if any(
            param is None
            for param in [
                bucket,
                car_id,
                keywords,
                date,
                batch,
                start_datetime,
                end_datetime,
            ]
        ):
            raise ValueError("Missing required fields")

        # 키워드 별로 추출
        for keyword in keywords:
            attempted, extracted = _extract(
                bucket, car_id, keyword, date, batch, start_datetime, end_datetime
            )
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

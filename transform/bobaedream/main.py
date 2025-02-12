import json
import logging
from datetime import datetime

import boto3
from bs4 import BeautifulSoup

EXTRACTED_PATH = "extracted/{car_id}/{date}/raw/bobaedream/{post_id}.json"
PARSED_PATH = "parsed/{car_id}/{date}/post/bobaedream/{post_id}.json"


def _get_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def _open_from_s3(bucket: str, path: str) -> dict:
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=path)
    content = response["Body"].read().decode("utf-8")
    return json.loads(content)


def _save_to_s3(bucket: str, path: str, data: dict):
    s3 = boto3.client("s3")
    content = json.dumps(data, ensure_ascii=False)
    s3.put_object(Bucket=bucket, Key=path, Body=content)


def _parse_title(soup: BeautifulSoup) -> str:
    """게시글 제목을 파싱"""
    title_elem = soup.select_one(".writerProfile dt strong")
    if title_elem:
        # Remove comment count in brackets if present
        title = title_elem.get_text().split("[")[0].strip()
        return title
    return ""


def _parse_content(soup: BeautifulSoup) -> str:
    """게시글 본문 내용을 파싱"""
    content_elem = soup.select_one(".bodyCont")
    if content_elem:
        return content_elem.get_text(strip=True)
    return ""


def _parse_author(soup: BeautifulSoup) -> str:
    """작성자 정보를 파싱"""
    author_elem = soup.select_one(".proflieInfo .nickName")
    if author_elem:
        return author_elem.get_text(strip=True)
    return ""


def _parse_created_at(soup: BeautifulSoup) -> str:
    """작성일시를 파싱"""
    date_elem = soup.select_one(".writerProfile .countGroup")
    if date_elem:
        # Extract date from text like "조회 433 | 추천 0 | 2025.02.10 (월) 08:25"
        date_text = date_elem.get_text().split("|")[-1].strip()
        return date_text
    return ""


def _parse_view_count(soup: BeautifulSoup) -> int:
    """조회수를 파싱"""
    view_elem = soup.select_one(".writerProfile .countGroup .txtType")
    if view_elem:
        try:
            return int(view_elem.get_text().strip())
        except ValueError:
            return 0
    return 0


def _parse_upvote_count(soup: BeautifulSoup) -> int:
    """추천수를 파싱"""
    upvote_elem = soup.select(".writerProfile .countGroup .txtType")[1]
    if upvote_elem:
        try:
            return int(upvote_elem.get_text().strip())
        except (ValueError, IndexError):
            return 0
    return 0


def _parse_comment_count(soup: BeautifulSoup) -> int:
    """댓글 수를 파싱"""
    comment_elem = soup.select_one(".writerProfile dt strong em")
    if comment_elem:
        try:
            # Remove brackets from text like "[5]"
            count = comment_elem.get_text().strip("[]")
            return int(count)
        except ValueError:
            return 0
    return 0


def _extract(bucket: str, car_id: str, date: str, post_id: str) -> None:
    extracted_path = EXTRACTED_PATH.format(car_id=car_id, date=date, post_id=post_id)

    post = _open_from_s3(bucket, extracted_path)

    post_url = post["url"]

    html = post["html"]

    soup = _get_soup(html)

    title = _parse_title(soup)
    content = _parse_content(soup)
    author = _parse_author(soup)
    created_at = _parse_created_at(soup)
    view_count = _parse_view_count(soup)
    upvote_count = _parse_upvote_count(soup)
    downvote_count = 0
    comment_count = _parse_comment_count(soup)

    parsed_path = PARSED_PATH.format(car_id=car_id, date=date, post_id=post_id)

    parsed_data = {
        "title": title,
        "content": content,
        "author": author,
        "created_at": created_at,
        "view_count": view_count,
        "upvote_count": upvote_count,
        "downvote_count": downvote_count,
        "comment_count": comment_count,
    }

    _save_to_s3(bucket, parsed_path, parsed_data)


def lambda_handler(event, context):
    start_time = datetime.now()

    try:
        logging.basicConfig(level=logging.INFO)

        bucket = event.get("bucket")
        car_id = event.get("car_id")
        date = event.get("date")
        post_id = event.get("post_id")

        if not all([bucket, car_id, date, post_id]):
            raise ValueError("Missing required input")

        _extract(bucket, car_id, date, post_id)

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
                "post_id": post_id,
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
                "post_id": post_id,
                "error": str(e),
            },
        }


def test_local():
    with open("test.html", "r", encoding="utf-8") as f:
        html = f.read()

    soup = _get_soup(html)

    print("=== 보배드림 게시글 파싱 테스트 ===")
    print(f"제목: {_parse_title(soup)}")
    print(f"작성자: {_parse_author(soup)}")
    print(f"작성일시: {_parse_created_at(soup)}")
    print(f"조회수: {_parse_view_count(soup)}")
    print(f"추천수: {_parse_upvote_count(soup)}")
    print(f"댓글수: {_parse_comment_count(soup)}")
    print("\n=== 본문 내용 ===")
    print(_parse_content(soup))


if __name__ == "__main__":
    test_local()

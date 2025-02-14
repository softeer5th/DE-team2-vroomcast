import json
import random
import requests
import urllib.parse
from bs4 import BeautifulSoup
from datetime import datetime
import time
import logging
import boto3
from parse_html import get_post_dict

# 상수 정의
BASE_URL = "https://www.clien.net"
SEARCH_URL = "/service/board/cm_car?&od=T31&category=0&po={}"
SLEEP_SECONDS = (1, 3)
TRIAL_LIMIT = 10

def fetch_html(url: str) -> str:
    """URL로부터 HTML 콘텐츠를 가져옵니다."""
    i = 0
    while i < TRIAL_LIMIT:
        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True)
            logging.info(f"{response.status_code} from {url}")
            if response.status_code != 200:
                i += 1
                continue
            return response.text
        except Exception as e:
            i += 1
            logging.warning(f"{e} from {url}")
            print(f"{e} from {url}")
            time.sleep(random.randint(*SLEEP_SECONDS))
            print("Retrying...")
    return ""


def parse_rows(rows) -> dict:
    """HTML row 데이터를 파싱하고 필터링하여 URL 딕셔너리를 반환합니다."""
    urls = {}
    for row in rows:
        content_time = row.select_one('span.timestamp')
        content_url = row.select_one('a.list_subject')
        content_title = row.select_one('span.subject_fixed')['title']
        content_url_postfix = content_url['href'].split('?')[0]
        content_id = content_url_postfix.split('/')[-1]
        content_date = datetime.strptime(content_time.text, "%Y-%m-%d %H:%M:%S").date()
        full_url = BASE_URL + content_url_postfix
        urls[content_id] = (full_url, content_date, content_title)
    return urls

def get_first_last_date_of_page(page_number: int):
    search_url = BASE_URL + SEARCH_URL.format(page_number)
    logging.info(f"Fetching URL: {search_url}")
    html_content = fetch_html(search_url)
    if not html_content:
        logging.error(f"failed to fetch {search_url}")
        return None, None
    soup = BeautifulSoup(html_content, "html.parser")
    rows = soup.select("div.list_item.symph_row")
    if not rows:
        return None, None
    latest_date = datetime.strptime(rows[0].select_one('span.timestamp').text, "%Y-%m-%d %H:%M:%S").date()
    old_date = datetime.strptime(rows[-1].select_one('span.timestamp').text, "%Y-%m-%d %H:%M:%S").date()
    logging.info(f"latest_date: {latest_date}, old_date: {old_date}")
    return latest_date, old_date # last가 더 오래된 날짜.

def get_target_date_page_num(date: str, span: int = 100):
    """
    주어진 date가 포함된 첫 페이지 번호를 반환합니다.
    방향을 잘못 선택했다면 `span`을 줄이면서 반대로 이동합니다.
    :param date: 검색할 목표 날짜 (YYYY-MM-DD)
    :param span: 탐색 시 페이지를 이동하는 단위 (초기값 100)
    :return: 목표 날짜가 포함된 페이지 번호 또는 None
    """
    target_date = datetime.strptime(date, "%Y-%m-%d").date()  # 목표 날짜를 파싱
    page_number = 700  # 탐색 시작 페이지 번호

    while True:
        # 현재 페이지 날짜 범위를 가져옵니다.
        latest_date, older_date = get_first_last_date_of_page(page_number)

        # 유효하지 않은 페이지는 검색 종료
        if latest_date is None or older_date is None:
            return None

        # 1. 목표 날짜가 현재 페이지 범위에 포함된 경우 반환
        if older_date <= target_date <= latest_date:
            return page_number

        # 2. 탐색 방향이 잘못된 경우 `span` 줄이기
        # (과거로 이동했어야 하는데 미래로 갔거나, 반대인 경우)
        if target_date < older_date and span < 0:  # 잘못된 방향 (더 미래로 이동했음)
            logging.info(f"span{target_date}, {latest_date}, {older_date}, {page_number}, {span}")
            span = max(1, abs(span) // 2)  # 방향 전환 후 span 감소
        elif target_date > latest_date and span > 0:  # 잘못된 방향 (더 과거로 이동했음)
            logging.info(f"{target_date}, {latest_date}, {older_date}, {page_number}, {span}")
            span = -max(1, abs(span) // 2)  # 방향 전환 후 span 감소

        # 3. 올바른 방향으로 이동
        if target_date > latest_date:
            page_number -= abs(span)  # 미래(페이지 번호 감소)
        elif target_date < older_date:
            page_number += abs(span)  # 과거(페이지 번호 증가)
        time.sleep(random.randint(*SLEEP_SECONDS))

def save_json_to_s3(car_id, urls, s3, bucket):
    for content_id, (post_url, post_date) in urls.items():
        html_content = fetch_html(post_url)  # 게시글 HTML 가져오기
        if not html_content:
            logging.error(f"Failed to fetch content from {post_url}. Skipping.")
            continue

        # JSON 덤프 데이터 생성
        dump_data = get_post_dict(html_content, content_id, post_url)
        json_body = json.dumps(dump_data, ensure_ascii=False, indent=4)

        # S3 객체 경로 정의
        object_key = f"extracted/{car_id}/{str(post_date)}/raw/clien/{content_id}.json"
        logging.info(f"Uploading to S3: {object_key}")

        # S3에 업로드
        s3.Object(bucket, object_key).put(Body=json_body)
        logging.info(f"Uploaded successfully: {object_key}")

        # 요청 대기 시간 추가 (Rate-Limiting)
        time.sleep(random.randint(1, 3))

def main_crawler(keywords: list, date: str, s3, car_id, bucket, months: int = 1,) -> dict:
    """
    지정된 키워드와 타겟 날짜부터 N개월간의 게시글 URL을 크롤링합니다.
    :param date: 시작 기준 타겟 날짜 (형식: "%Y-%m-%d")
    :param months: 크롤링 기간 (단위: 월)
    :return: 크롤링된 게시글 URL 딕셔너리 (key: URL, value: 날짜)
    """

    # 기준 날짜(target_date)와 종료 날짜(end_date) 계산
    target_date = datetime.strptime(date, "%Y-%m-%d").date()  # 기준 날짜
    year = target_date.year
    month = target_date.month + months  # N개월 전 설정
    if month > 12:  # 월이 양수이면 연도 증가 감소
        year += 1
        month -= 12
    end_date = target_date.replace(year=year, month=month)  # N개월 이전 날짜

    # 타겟 페이지 찾기
    page_number = get_target_date_page_num(date,200)  # 기준 날짜가 포함된 페이지 번호 찾기
    if page_number is None:
        logging.error(f"Could not find the target page for date {date}")
        return {}

    urls = {}

    logging.info(f"Starting to crawl for target_date: {target_date}, end_date: {end_date}")

    while True:
        # URL 생성
        search_url = BASE_URL + SEARCH_URL.format(page_number)
        logging.info(f"Fetching URL: {search_url}")

        # HTML 콘텐츠 요청
        html_content = fetch_html(search_url)
        if not html_content:
            logging.error(f"Failed to fetch content from {search_url}. Skipping to next page.")
            page_number -= 1
            continue

        # 페이지 데이터 파싱
        soup = BeautifulSoup(html_content, "html.parser")
        rows = soup.select("div.list_item.symph_row")
        if not rows:
            logging.info(f"No rows found on page {page_number}. Ending crawl.")
            break

        # 행 데이터에서 URL과 날짜 파싱
        page_urls = parse_rows(rows)

        # 필터링 및 URL 저장
        for content_id, value in page_urls.items():
            post_url, post_date, title = value
            if post_date > end_date:
                logging.info(f"Post dated {post_date} is before end_date {end_date}. Stopping crawl.")
                return urls  # 조건 범위를 벗어나면 크롤링 종료
            for keyword in keywords:
                if keyword in title:
                    urls[content_id] = (post_url, post_date)
                    break

        # 가장 오래된 게시글이 종료 조건에 도달했는지 확인
        oldest_post_date = min(value[1] for _, value in page_urls.items())
        if oldest_post_date > end_date:
            logging.info(f"Oldest post date {oldest_post_date} is before end_date {end_date}. Ending crawl.")
            break

        # 다음 페이지로 이동 (점진적으로 과거 탐색)
        page_number -= 1  # 미래로 이동
        logging.info(f"Moving to the next page: {page_number}")
        if len(urls) > 20:
            save_json_to_s3(car_id=car_id, urls=urls, s3=s3, bucket=bucket)
            urls.clear()
        time.sleep(random.randint(*SLEEP_SECONDS))  # 요청 사이에 짧은 대기
    save_json_to_s3(car_id=car_id, urls=urls, s3=s3, bucket=bucket)
    return urls

def lambda_handler(event, context):
    """
    AWS Lambda 핸들러 함수.
    주어진 키워드와 날짜에 대해 크롤링된 게시글 정보를 AWS S3에 업로드합니다.

    :param event: Lambda 실행 시 입력된 데이터 (car_id, keywords, date, bucket, months 포함됨).
        - car_id: 크롤링 대상 그룹 ID
        - keywords: 검색 키워드 리스트
        - date: 크롤링 기준 날짜 (형식: %Y-%m-%d)
        - bucket: AWS S3 버킷 이름
        - months: (선택적) 크롤링 기간 (기본값: 1개월)
    :param context: Lambda 실행 환경 정보.
    :return: 크롤링 상태를 담은 JSON 응답.
    """
    start_time = time.time()  # 작업 시작 시간 기록

    # Event에서 필수 파라미터와 선택적 파라미터 추출
    car_id = event.get("car_id")
    keywords = event.get("keywords", [])
    date = event.get("date")
    bucket = event.get("bucket")
    months = event.get("months", 1)  # 기본값: 1개월

    # 필수 파라미터 검증
    if not car_id or not keywords or not date or not bucket:
        logging.error("Missing required event parameters.")
        return {
            "statusCode": 400,
            "body": {
                "success": False,
                "message": "Required parameters are missing: car_id, keywords, date, bucket",
            },
        }

    try:
        # AWS S3 리소스 생성
        s3 = boto3.resource("s3")
        # 각 키워드에 대해 크롤링 시작
        logging.info(f"[CRAWLER] Started crawling for keyword: {keywords}, date: {date}, car_id: {car_id}")
        # main_crawler 호출: URL 크롤링
        leftover_urls = main_crawler(keywords=keywords, date=date, months=months, s3=s3, car_id=car_id, bucket=bucket)
        save_json_to_s3(car_id=car_id, urls=leftover_urls, s3=s3, bucket=bucket)
    except Exception as e:
        # 예외 처리: 에러 상세 로그 저장
        logging.error(f"Error occurred during Lambda execution: {e}")
        return {
            "statusCode": 500,
            "body": {
                "success": False,
                "error": str(e),
                "end_time": datetime.now().isoformat(),
                "duration": time.time() - start_time,
                "car_id": car_id,
                "date": date,
            },
        }

    # 작업 완료: 성공 JSON 응답 반환
    return {
        "statusCode": 200,
        "body": {
            "success": True,
            "end_time": datetime.now().isoformat(),
            "duration": time.time() - start_time,
            "car_id": car_id,
            "date": date,
            "keywords": keywords,
        },
    }
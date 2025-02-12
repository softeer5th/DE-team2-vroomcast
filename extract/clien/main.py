import random
import requests
import urllib.parse
from bs4 import BeautifulSoup
from datetime import datetime
import time
import logging
import boto3

# 상수 정의
BASE_URL = "https://www.clien.net"
SEARCH_URL = "/service/search?q={}&sort=recency&p={}&boardCd=&isBoard=false"
BOARD_FILTER = "cm_car"
SLEEP_SECONDS = (1, 3)
# 설정: 람다 기본 로깅
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def fetch_html(url: str) -> str:
    """URL로부터 HTML 콘텐츠를 가져옵니다."""
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    logging.info(f"{response.status_code} from {url}")
    if response.status_code != 200:
        logging.error(f"Failed to fetch URL: {url} - Status Code: {response.status_code}")
        return ""
    return response.text


def parse_rows(rows, target_date: datetime.date) -> dict:
    """HTML row 데이터를 파싱하고 필터링하여 URL 딕셔너리를 반환합니다."""
    urls = {}
    for row in rows:
        content_time = row.select_one('span.timestamp')
        content_url = row.select_one('a.subject_fixed')
        content_url_postfix = content_url['href'].split('?')[0]
        content_id = content_url_postfix.split('/')[-1]
        content_board = content_url_postfix.split('/')[-2]
        content_date = datetime.strptime(content_time.text, "%Y-%m-%d %H:%M:%S").date()

        # 게시판 필터와 날짜 기준으로 필터링
        if content_board != BOARD_FILTER or content_date != target_date:
            continue
        full_url = BASE_URL + content_url_postfix
        urls[content_id] = full_url
    return urls


def main_crawler(keyword: str, date: str) -> dict:
    """
    지정된 키워드와 날짜 기준으로 게시글 URL을 크롤링합니다.
    :param keyword: 검색 키워드
    :param date: 검색 대상 날짜 (형식: "%Y-%m-%d")
    :return: 게시글 URL 딕셔너리
    """
    encoded_keyword = urllib.parse.quote(keyword)
    target_date = datetime.strptime(date, "%Y-%m-%d").date()
    page_number = 0
    urls = {}

    while True:
        # URL 생성 및 요청
        search_url = BASE_URL + SEARCH_URL.format(encoded_keyword, page_number)
        logging.info(f"Fetching URL: {search_url}")
        html_content = fetch_html(search_url)
        if not html_content:
            break  # HTTP 요청 실패 시 종료

        # 응답 파싱 및 필터링
        soup = BeautifulSoup(html_content, "html.parser")
        rows = soup.select("div.list_item.symph_row.jirum")
        logging.info(f"Parsing {len(rows)} rows.")
        urls.update(parse_rows(rows, target_date))

        # 가장 마지막 행의 날짜 확인
        if not rows or datetime.strptime(
                rows[-1].select_one('span.timestamp').text,
                "%Y-%m-%d %H:%M:%S"
        ).date() < target_date:
            break

        page_number += 1
        time.sleep(random.randint(*SLEEP_SECONDS))  # 요청 간 대기 시간

    logging.info(f"Extracted {len(urls)} URLs.")
    return urls

def lambda_handler(event, context):
    """
    AWS Lambda 핸들러 함수.
    키워드를 기반으로 크롤링된 게시글 HTML 콘텐츠를 AWS S3에 업로드합니다.

    :param event: Lambda 호출 시 입력된 데이터 (car_id, keywords, date, bucket_name 포함).
    :param context: Lambda 실행 환경과 관련된 컨텍스트 정보.
    :return: Lambda 실행 결과를 담은 JSON 응답.
    """
    start_time = time.time()
    car_id = event["car_id"]
    keywords = event["keywords"]
    date = event["date"]
    bucket_name = event["bucket_name"]
    try:
        s3 = boto3.resource("s3")
        for keyword in keywords:
            logging.info(f"Search started keywords: {keyword} date: {date} car_id: {car_id}")
            urls = main_crawler(keyword, date)
            for id, url in urls.items():
                html_content = fetch_html(url)
                if not html_content:
                    continue
                s3.Object(bucket_name, f"extracted/{car_id}/{date}/post/clien/{id}.html").put(Body=html_content)
                logging.info(f"put extracted/{car_id}/{date}/post/clien/{id}.html to s3")
                time.sleep(random.randint(1, 3))
    except Exception as e:
        logging.error(e)
        return {
            "statusCode": 500,
            "body": {
                "success":False,
                "end_time":datetime.now().isoformat(),
                "duration":time.time() - start_time,
                "car_id": car_id,
                "date":date,
            }
        }
    return {
        "statusCode": 200,
        "body": {
            "success":True,
            "end_time":datetime.now().isoformat(),
            "duration":time.time() - start_time,
            "car_id": car_id,
            "date":date
        }
    }
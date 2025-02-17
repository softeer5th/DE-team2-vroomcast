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
SEARCH_URL = "/service/search?q={}&sort=recency&{}boardCd=cm_car&isBoard=true"
BOARD_FILTER = "cm_car"
SLEEP_SECONDS = (1, 3)
TRIAL_LIMIT = 10

logging.basicConfig(level=logging.INFO)

def fetch_html(url: str) -> str:
    """URL로부터 HTML 콘텐츠를 가져옵니다."""
    i = 0
    while i < TRIAL_LIMIT:
        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:135.0) Gecko/20100101 Firefox/135.0"}, allow_redirects=True)
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


def parse_rows(rows, start_datetime, end_datetime) -> dict:
    """HTML row 데이터를 파싱하고 필터링하여 URL 딕셔너리를 반환합니다."""
    urls = {}
    for row in rows:
        content_time = row.select_one('span.timestamp')
        content_url = row.select_one('a.subject_fixed')
        content_url_postfix = content_url['href'].split('?')[0]
        content_id = content_url_postfix.split('/')[-1]
        content_datetime = datetime.strptime(content_time.text, "%Y-%m-%d %H:%M:%S")

        # 게시판 필터와 날짜 기준으로 필터링
        if start_datetime > content_datetime or end_datetime < content_datetime:
            continue
        full_url = BASE_URL + content_url_postfix
        urls[content_id] = full_url
    return urls


def main_crawler(keyword:str, start_datetime:str, end_datetime:str) -> dict:
    """
    지정된 키워드와 날짜 기준으로 게시글 URL을 크롤링합니다.
    :param keyword: 검색 키워드
    :param date: 검색 대상 날짜 (형식: "%Y-%m-%d")
    :return: 게시글 URL 딕셔너리
    """
    encoded_keyword = urllib.parse.quote(keyword)
    start_datetime = datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M:%S")
    end_datetime = datetime.strptime(end_datetime, "%Y-%m-%dT%H:%M:%S")
    page_number = 0
    urls = {}

    while page_number < 50:
        # URL 생성 및 요청
        search_url = BASE_URL + SEARCH_URL.format(encoded_keyword, f"p={page_number}&"if page_number else "")
        logging.info(f"Fetching URL: {search_url}")
        html_content = fetch_html(search_url)
        if not html_content:
            logging.error(f"failed to fetch {search_url}")
            continue
        # 응답 파싱 및 필터링
        soup = BeautifulSoup(html_content, "html.parser")
        rows = soup.select("div.list_item.symph_row.jirum")
        logging.info(f"Parsing {len(rows)} rows.")
        urls.update(parse_rows(rows, start_datetime, end_datetime))

        # 가장 마지막 행의 날짜 확인
        if not rows or datetime.strptime(
                rows[-1].select_one('span.timestamp').text,
                "%Y-%m-%d %H:%M:%S"
        ) < start_datetime:
            break

        page_number += 1
        time.sleep(random.randint(*SLEEP_SECONDS))  # 요청 간 대기 시간

    logging.info(f"Extracted {len(urls)} URLs.")
    return urls

def save_to_s3(s3, bucket, object_key, data):
    """Save a JSON object to S3."""
    s3.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=json.dumps(data, indent=4, ensure_ascii=False),
        ContentType='application/json'
    )

def lambda_handler(event, context):
    """
    AWS Lambda 핸들러 함수.
    키워드를 기반으로 크롤링된 게시글 json 콘텐츠를 AWS S3에 업로드합니다.

    :param event: Lambda 호출 시 입력된 데이터 (car_id, keywords, date, bucket 포함).
    :param context: Lambda 실행 환경과 관련된 컨텍스트 정보.
    :return: Lambda 실행 결과를 담은 JSON 응답.
    """
    start_time = time.time()
    car_id = event["car_id"]
    keywords = event["keywords"]
    date = event["date"]
    batch_num = event["batch"]
    start_datetime = event["start_datetime"]
    end_datetime = event["end_datetime"]
    bucket = event["bucket"]
    try:
        s3 = boto3.resource("s3")
        for keyword in keywords:
            logging.info(f"Search started keywords: {keyword} date: {date} car_id: {car_id}")
            urls = main_crawler(keyword, start_datetime, end_datetime)
            for id, url in urls.items():
                html_content = fetch_html(url)
                if not html_content:
                    logging.error(f"failed to fetch {url}")
                    continue
                dump_data = get_post_dict(html_content, id, url)
                json_body = json.dumps(
                    dump_data,
                    ensure_ascii=False,
                    indent=4
                )
                s3.Object(bucket, f"extracted/{car_id}/{date}/{batch_num}/raw/clien/{id}.json").put(Body=json_body)
                logging.info(f"put extracted/{car_id}/{date}/{batch_num}/raw/clien/{id}.json to s3")
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
                "batch": batch_num,
                "start_datetime": start_datetime,
                "end_datetime": end_datetime,
            }
        }
    return {
        "statusCode": 200,
        "body": {
            "success":True,
            "end_time":datetime.now().isoformat(),
            "duration":time.time() - start_time,
            "car_id": car_id,
            "date":date,
            "batch":batch_num,
            "start_datetime":start_datetime,
            "end_datetime":end_datetime,
        }
    }
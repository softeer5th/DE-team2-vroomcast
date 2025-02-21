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

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def fetch_html(url: str) -> str:
    """
    Retrieve HTML content from a URL with retry mechanism.
    
    Attempts to fetch HTML content from the specified URL up to TRIAL_LIMIT times.
    Each attempt sends an HTTP GET request using a standard browser User-Agent.
    If a non-200 status is received or an exception occurs, the function logs the event,
    waits for a randomized delay within SLEEP_SECONDS, and retries the request.
    Returns the HTML content on a successful response, or an empty string if all attempts fail.
    
    Args:
        url (str): The URL to retrieve HTML content from.
    
    Returns:
        str: The HTML content if successfully fetched; otherwise, an empty string.
    
    Example:
        >>> html = fetch_html("https://example.com")
        >>> if html:
        ...     print("HTML content retrieved successfully.")
        ... else:
        ...     print("Failed to retrieve HTML content.")
    """
    i = 0
    while i < TRIAL_LIMIT:
        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:135.0) Gecko/20100101 Firefox/135.0"}, allow_redirects=True)
            logger.info(f"{response.status_code} from {url}")
            if response.status_code != 200:
                i += 1
                continue
            return response.text
        except Exception as e:
            i += 1
            logger.warning(f"{e} from {url}")
            print(f"{e} from {url}")
            time.sleep(random.randint(*SLEEP_SECONDS))
            print("Retrying...")
    return ""


def parse_rows(rows, start_datetime, end_datetime) -> dict:
    """
    Parses HTML row elements to extract and filter post URLs by date range.
    
    Args:
        rows (list): List of BeautifulSoup Tag objects representing HTML rows.
        start_datetime (datetime.datetime): Lower datetime bound; posts earlier than this are ignored.
        end_datetime (datetime.datetime): Upper datetime bound; posts later than this are ignored.
    
    Returns:
        dict: Dictionary mapping post IDs (str) to fully qualified post URLs (str).
    
    The full URL is constructed by concatenating the module's BASE_URL with the URL postfix
    extracted from each row.
    """
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
    지정된 키워드와 날짜 범위 내의 게시글 URL을 크롤링합니다.
    
    매개변수:
        keyword (str): 검색할 키워드.
        start_datetime (str): 검색 시작 날짜 및 시간을 나타내며, 형식은 "%Y-%m-%dT%H:%M:%S"입니다.
        end_datetime (str): 검색 종료 날짜 및 시간을 나타내며, 형식은 "%Y-%m-%dT%H:%M:%S"입니다.
    
    반환값:
        dict: 게시글 ID를 키로, 해당 게시글의 URL을 값으로 갖는 딕셔너리.
    
    동작:
        1. 키워드를 URL 인코딩하고, 시작 및 종료 날짜/시간을 datetime 객체로 변환합니다.
        2. 최대 50페이지에 대해 생성된 검색 URL로 HTML 콘텐츠를 가져옵니다.
        3. HTML 콘텐츠가 존재하면 BeautifulSoup로 파싱한 후, 게시글 리스트에서 지정된 날짜 범위 내의 URL을 추출합니다.
        4. 게시글 목록이 없거나, 마지막 게시글의 날짜가 시작 날짜보다 이전인 경우 크롤링을 중단합니다.
        5. 각 요청 사이에는 랜덤한 대기 시간을 두어 서버 부하를 완화합니다.
        6. 최종적으로 추출된 게시글 URL 딕셔너리를 반환합니다.
    """
    encoded_keyword = urllib.parse.quote(keyword)
    start_datetime = datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M:%S")
    end_datetime = datetime.strptime(end_datetime, "%Y-%m-%dT%H:%M:%S")
    page_number = 0
    urls = {}

    while page_number < 50:
        # URL 생성 및 요청
        search_url = BASE_URL + SEARCH_URL.format(encoded_keyword, f"p={page_number}&"if page_number else "")
        html_content = fetch_html(search_url)
        if not html_content:
            logger.error(f"failed to fetch {search_url}")
            continue
        # 응답 파싱 및 필터링
        soup = BeautifulSoup(html_content, "html.parser")
        rows = soup.select("div.list_item.symph_row.jirum")
        logger.info(f"Parsing {len(rows)} rows.")
        urls.update(parse_rows(rows, start_datetime, end_datetime))

        # 가장 마지막 행의 날짜 확인
        if not rows or datetime.strptime(
                rows[-1].select_one('span.timestamp').text,
                "%Y-%m-%d %H:%M:%S"
        ) < start_datetime:
            break

        page_number += 1
        time.sleep(random.randint(*SLEEP_SECONDS))  # 요청 간 대기 시간

    logger.info(f"Extracted {len(urls)} URLs.")
    return urls

def save_to_s3(s3, bucket, object_key, data):
    """
    Uploads a JSON-formatted object to an S3 bucket.
    
    Serializes the provided data with indented JSON formatting and uploads it to the
    specified S3 bucket under the given object key. The uploaded object is marked
    with the content type "application/json".
    
    Args:
        s3: An S3 service client with a put_object method (e.g., a boto3 S3 client).
        bucket (str): The name of the target S3 bucket.
        object_key (str): The key (or path) under which the object is stored.
        data (dict): A JSON-serializable object to be saved.
    
    Raises:
        Any exception from s3.put_object will propagate.
    """
    s3.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=json.dumps(data, indent=4, ensure_ascii=False),
        ContentType='application/json'
    )

def lambda_handler(event, context):
    """
    Entry point for AWS Lambda to crawl posts and upload JSON files to AWS S3.
    
    Extracts parameters from the event to perform web scraping based on provided keywords 
    and date range. For each keyword, crawls the corresponding posts, retrieves HTML content, 
    converts it into JSON, and uploads the file to a designated S3 bucket. Execution metrics 
    such as duration and counts of attempted and successfully processed posts are included in 
    the returned response.
    
    Args:
        event (dict): Input event with the following keys:
            - "car_id" (str): Identifier for the car.
            - "keywords" (list): List of keywords for the crawl.
            - "date" (str): Date string associated with the crawl.
            - "batch" (str/int): Identifier for the current batch of crawls.
            - "start_datetime" (str): Start datetime filter for posts.
            - "end_datetime" (str): End datetime filter for posts.
            - "bucket" (str): AWS S3 bucket name for storing extracted data.
        context (object): AWS Lambda context object providing runtime and execution details.
    
    Returns:
        dict: A response dictionary containing:
            - "statusCode" (int): HTTP-like status code (200 for success, 500 for error).
            - "body" (dict): Contains details of the operation, including:
                - "success" (bool): True if the crawl and upload were successful.
                - "end_time" (str): ISO 8601 timestamp indicating when the process completed.
                - "duration" (float): Execution duration in seconds.
                - "car_id" (str): Car identifier from the event.
                - "date" (str): Date string from the event.
                - "batch" (str/int): Batch identifier.
                - "start_datetime" (str): Start datetime filter.
                - "end_datetime" (str): End datetime filter.
                - "attempted_posts_count" (int): Total number of posts attempted.
                - "extracted_posts_count" (int): Count of successfully processed posts.
                - "error" (str, optional): Error message if an exception was encountered.
    """
    start_time = time.time()
    car_id = event["car_id"]
    keywords = event["keywords"]
    date = event["date"]
    batch_num = event["batch"]
    start_datetime = event["start_datetime"]
    end_datetime = event["end_datetime"]
    bucket = event["bucket"]
    total_count = 0
    success_count = 0
    try:
        s3 = boto3.resource("s3")
        for keyword in keywords:
            logger.info(f"Search started keywords: {keyword} date: {date} car_id: {car_id}")
            urls = main_crawler(keyword, start_datetime, end_datetime)
            for id, url in urls.items():
                total_count += 1
                html_content = fetch_html(url)
                if not html_content:
                    logger.error(f"failed to fetch {url}")
                    continue
                success_count += 1
                dump_data = get_post_dict(html_content, id, url)
                json_body = json.dumps(
                    dump_data,
                    ensure_ascii=False,
                    indent=4
                )
                s3.Object(bucket, f"extracted/{car_id}/{date}/{batch_num}/raw/clien/{id}.json").put(Body=json_body)
                logger.info(f"put extracted s3://{bucket}/extracted/{car_id}/{date}/{batch_num}/raw/clien/{id}.json to s3")
                time.sleep(random.randint(1, 3))
    except Exception as e:
        logger.error(e)
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
                "attempted_posts_count": total_count,
                "extracted_posts_count": success_count,
                "error": str(e)
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
            "attempted_posts_count": total_count,
            "extracted_posts_count": success_count,
        }
    }
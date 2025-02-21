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
SEARCH_URL = "/service/board/cm_car?&od=T31&category=0{}"
SLEEP_SECONDS = (1, 3)
TRIAL_LIMIT = 10

def fetch_html(url: str) -> str:
    """
    Retrieves HTML content from a URL with retry logic.
    
    Attempts to fetch the HTML content using an HTTP GET request with a custom
    User-Agent header. If the response status code is not 200 or an exception occurs,
    the request is retried up to TRIAL_LIMIT times. On exceptions, the function waits
    for a random duration specified by SLEEP_SECONDS before retrying.
    
    Args:
        url (str): The URL from which to retrieve HTML content.
    
    Returns:
        str: The HTML content if the request is successful; otherwise, an empty string.
    """
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
    """
    Parses a list of HTML rows to extract post details and returns a dictionary mapping post IDs to data tuples.
    
    Each HTML row is expected to contain:
      - A <span> with class "timestamp" holding the post's date and time in the "%Y-%m-%d %H:%M:%S" format.
      - An <a> tag with class "list_subject" whose 'href' attribute contains the post URL.
      - A <span> with class "subject_fixed" containing a 'title' attribute for the post title.
    
    The function extracts the URL postfix from the 'href', splits it to obtain the post ID, and constructs a full URL by concatenating it with the global BASE_URL. It also parses the timestamp into a datetime.date object.
    
    Args:
        rows (Iterable): A collection of HTML elements (e.g., BeautifulSoup objects) representing individual post rows.
    
    Returns:
        dict: A dictionary where each key is a post ID (str) and each value is a tuple containing:
            - full URL (str): The complete URL to the post.
            - publication date (datetime.date): The post date parsed from the timestamp.
            - post title (str): The title of the post.
            
    Example:
        >>> from bs4 import BeautifulSoup
        >>> html = '''
        ... <div>
        ...     <span class="timestamp">2023-10-01 12:34:56</span>
        ...     <a class="list_subject" href="/posts/12345?ref=home"></a>
        ...     <span class="subject_fixed" title="Example Post"></span>
        ... </div>
        ... '''
        >>> row = BeautifulSoup(html, 'html.parser').div
        >>> parse_rows([row])
        {'12345': ('http://clien.url/posts/12345', datetime.date(2023, 10, 1), 'Example Post')}
    """
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
    """
    Extracts the latest and oldest post dates from the specified forum page.
    
    Fetches the HTML content from a URL constructed using the base and search URL formats,
    parses the page to locate post entries, and extracts dates from the first and last posts.
    Dates are derived from the <span> element with class "timestamp" and converted to date objects.
    Returns (None, None) if the HTML content cannot be fetched or if no post entries are found.
    
    Args:
        page_number (int): The page number to fetch and parse.
    
    Returns:
        tuple: A pair where the first element is the latest post date and the second element
        is the oldest post date (both as datetime.date objects), or (None, None) on failure.
    """
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
    목표 날짜가 포함된 페이지 번호를 검색합니다.
    
    주어진 YYYY-MM-DD 형식의 날짜를 포함하는 페이지 번호를, 현재 페이지의 최신/오래된 게시일 범위를 기준으로 탐색합니다.
    탐색 방향이 잘못되면 span 값을 줄이고 반대 방향으로 이동하며, 유효하지 않은 페이지를 만나면 탐색을 종료하여 None을 반환합니다.
    
    Args:
        date (str): 검색할 목표 날짜 (YYYY-MM-DD).
        span (int, optional): 페이지 탐색 시 이동할 단위. 기본값은 100.
    
    Returns:
        int or None: 목표 날짜가 포함된 페이지 번호. 유효하지 않은 페이지의 경우 None 반환.
    
    Raises:
        ValueError: date 인자가 YYYY-MM-DD 형식이 아닐 경우.
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
    """
    Uploads scraped post data as JSON files to an S3 bucket.
    
    Iterates over a dictionary of post identifiers and associated data, fetching the HTML
    content for each post and converting it into JSON format. The JSON data is then uploaded
    to the specified S3 bucket using a structured object key. If fetching the HTML fails,
    the function logs an error and skips the post. A randomized delay is applied after each
    upload to enforce rate limiting.
    
    Args:
        car_id: Unique identifier for the car, used in the S3 object key path.
        urls: Dictionary mapping content IDs to tuples containing:
              - post_url (str): The URL of the post.
              - post_date: The date of the post, convertible to a string.
        s3: S3 resource object (e.g., a boto3 resource) used to interact with the S3 service.
        bucket: Name of the S3 bucket where JSON files will be stored.
    
    Returns:
        None.
    """
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
    Crawls posts from the forum starting at a given date for a specified duration and filters them by keywords.
    
    Iteratively retrieves pages beginning from the target date's page number and extracts post URLs, dates, and titles.
    Posts are filtered to include only those whose title contains at least one of the specified keywords and whose post date
    is within the period starting from the target date up to the end date (computed by adding the specified number of months).
    The collected posts are periodically saved to an AWS S3 bucket as JSON files when more than 20 entries are gathered.
    The crawl terminates when the oldest post on a page exceeds the computed end date, or if no further posts can be retrieved.
    
    Args:
        keywords (List[str]): List of keywords to filter posts.
        date (str): Target start date for crawling in the format "%Y-%m-%d".
        s3: AWS S3 resource instance used for uploading JSON data.
        car_id: Identifier corresponding to the car category.
        bucket: Name of the S3 bucket where the JSON files are stored.
        months (int, optional): Duration in months for the crawl. Defaults to 1.
    
    Returns:
        dict: A mapping of content IDs to tuples containing the post URL and post date.
              Format: {content_id: (post_url, post_date)}.
    
    Example:
        >>> result = main_crawler(['keyword1', 'keyword2'], "2025-01-01", s3_resource, "car123", "my-bucket", months=2)
        >>> print(result)
        {'123456': ('https://clien_url', datetime.date(2025, 1, 5)), ...}
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
        search_url = BASE_URL + SEARCH_URL.format(f"&po={page_number}" if page_number else "")
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
    
    Lambda 이벤트로부터 크롤링에 필요한 파라미터를 추출하여 Clien 포럼의 게시글 데이터를 크롤링한 후,
    AWS S3에 업로드합니다. 필수 이벤트 키는 car_id, keywords, date, bucket이며, months는 선택적(기본값: 1)입니다.
    
    Parameters:
        event (dict): Lambda 호출 시 전달되는 이벤트 데이터.
            필수 키:
                - car_id (str): 크롤링 대상 그룹의 ID.
                - keywords (list of str): 검색 키워드 목록.
                - date (str): 크롤링 기준 날짜 (형식: YYYY-MM-DD).
                - bucket (str): 결과 데이터를 저장할 AWS S3 버킷 이름.
            선택적 키:
                - months (int): 크롤링 기간(월 단위, 기본값: 1).
        context (LambdaContext): Lambda 실행 환경 정보.
    
    Returns:
        dict: 크롤링 실행 결과를 포함하는 JSON 응답.
              성공 시 'statusCode' 200와 크롤링 결과 및 작업 소요 시간이 포함되며,
              필수 파라미터 누락 시 'statusCode' 400, 예외 발생 시 'statusCode' 500을 반환합니다.
    
    Example:
        >>> event = {
        ...     "car_id": "123",
        ...     "keywords": ["engine", "transmission"],
        ...     "date": "2025-01-01",
        ...     "bucket": "my-s3-bucket",
        ...     "months": 2
        ... }
        >>> context = {}  # 실제 LambdaContext 객체가 전달됩니다.
        >>> response = lambda_handler(event, context)
        >>> print(response)
        {'statusCode': 200, 'body': {'success': True, ...}}
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

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Sample event and context for testing
    sample_event = {
        "car_id": "santa_fe",
        "keywords": ["싼타페", "산타페", "santafe"],
        "date": "2023-11-21",
        "bucket": "hmg-5th-crawling-test",
        "months": 1
    }
    sample_context = {}  # Empty context object for testing
    # Call lambda_handler with the sample event and context
    response = lambda_handler(sample_event, sample_context)
    # Print the response
    print(response)
    
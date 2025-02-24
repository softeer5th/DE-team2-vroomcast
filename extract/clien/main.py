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
BOARD_URL = "/service/board/cm_car?&od=T31&category=0"
SEARCH_URL = "/service/search?q={}&sort=recency&{}boardCd=cm_car&isBoard=true"
BOARD_FILTER = "cm_car"
SLEEP_SECONDS = (1, 3)
TRIAL_LIMIT = 3
user_agent_list = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246",
    "Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
]


s3 = boto3.resource("s3")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def fetch_html(url: str) -> str:
    """
    HTML content를 fetch한다. 만약에 TRIAL_LIMIT 만큼 시도해도 성공하지 못한 경우에는 빈 문자열을 반환한다.

    Parameters:
        url: str
            HTML을 얻기 위한 URL

    Returns:
        str
            HTML content를 문자열로 반환.
            만약에 실패한 경우에는 빈 문자열을 반환.
    """
    i = 0
    while i < TRIAL_LIMIT:
        try:
            response = requests.get(url, headers={
                "User-Agent": user_agent_list[i % len(user_agent_list)]
            }, allow_redirects=True)
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


def parse_rows(rows: list, start_datetime: datetime, end_datetime: datetime) \
        -> dict[str, str]:
    """
    포스트 목록 html list를 받아서, 개별 url, title, 작성시간을 수집한다.

    Arguments:
        rows (list): 파싱이 필요한 html row list.
        start_datetime (datetime): 해당 시간보다 크거나 같은 시간의 포스트 정보만 추출한다.
        end_datetime (datetime): 해당 시간보다 작거나 같은 시간의 포스트 정보만 추출한다.

    Returns:
        dict: content_id를 key로 하고 value에 full_url이 존재.
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


def get_list_of_post_url(start_datetime: datetime, end_datetime: datetime, page_number_list: list[int], keyword: str) -> tuple[
    dict, list[int]]:
    """
    주어진 페이지 넘버 리스트와 검색어를 통해서, 시작시간과 종료시간 사이의 포스트 url들을 추출한다.

    Parameters:
        start_datetime (datetime): 포스트 작성시간을 필터링할 시작시간
        end_datetime (datetime): 포스트 작성시간을 필터링할 종료시간
        page_number_list (list[int]): 확인할 페이지 넘버 리스트.
        keyword (str): 검색어.

    Returns:
        tuple[dict, list[int]]
            튜플의 첫번째 원소로 url 목록을 가진 dict(key는 content_id, value는 full_url)를 반환하고,
            목록 불러오기에 실패한 페이지 리스트를 두번째 원소로 넣어준다.
    """
    urls = {}
    failed_page_number = []
    for page_number in page_number_list:
        # URL 생성 및 요청
        search_url = BASE_URL + SEARCH_URL.format(urllib.parse.quote(keyword), f"p={page_number}&" if page_number > 1 else "")
        html_content = fetch_html(search_url)
        if not html_content:
            logger.error(f"failed to fetch {search_url}")
            failed_page_number.append(page_number)
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
        time.sleep(random.randint(*SLEEP_SECONDS))  # 요청 간 대기 시간
    return urls, failed_page_number


def load_each_post_with_keyword(urls: dict[int, str], data_for_load_s3: dict) -> tuple[
    int, int]:
    """
    주어진 url들을 돌면서 save_post_to_s3()를 불러온다.

    Parameters:
	    urls (dict[int, str]): 키가 식별자(int)이며, 값은 URL (str)과 게시물 제목 (str)을 포함한 튜플인 딕셔너리.
	    data_for_load_s3 (dict): 게시물을 S3 저장 서비스에 저장하는 데 필요한 데이터를 포함하는 딕셔너리.

    Returns:
        tuple[int, int]: 총 포스트의 개수, S3에 성공적으로 저장된 포스트의 개수
    """
    total = 0
    success = 0
    for id, url in urls.items():
        total += 1
        if save_post_to_s3(id, url, data_for_load_s3):
            success += 1
            time.sleep(random.randint(*SLEEP_SECONDS))
    return total, success


def save_post_to_s3(id: int, url: str, data_for_load_s3: dict) -> bool:
    """
    게시물의 데이터를 S3 버킷에 저장한다.
    주어진 URL에서 HTML 콘텐츠를 가져와 구조화된 딕셔너리로 처리하고,
    데이터를 JSON 형식으로 직렬화한 후,
    제공된 메타데이터를 기반으로 특정 경로에 S3 버킷에 업로드한다.

    Args:
        id (int): 게시물의 고유 식별자.
        url (str): 게시물의 HTML 콘텐츠를 가져올 URL.
        data_for_load_s3 (dict): S3 업로드를 위한 메타데이터를 포함하는 딕셔너리:
            - 'car_id' (str): 진행 중인 작업에서 차량 식별자.
            - 'date' (str): 작업 날짜, S3 경로에 포함.
            - 'batch_num' (str): 작업의 배치 번호, S3 경로에 포함.
            - 'bucket' (str): 데이터가 업로드될 S3 버킷의 이름.


    Returns:
       bool: 데이터가 S3에 성공적으로 업로드되면 True, 그렇지 않으면 False.
    """
    html_content = fetch_html(url)
    if not html_content:
        logger.error(f"failed to fetch {url}")
        return False
    dump_data = get_post_dict(html_content, id, url)
    json_body = json.dumps(
        dump_data,
        ensure_ascii=False,
        indent=4
    )
    load_path = f"extracted/{data_for_load_s3['car_id']}/{data_for_load_s3['date']}/{data_for_load_s3['batch_num']}/raw/clien/{id}.json"
    try:
        s3.Object(data_for_load_s3['bucket'], load_path).put(Body=json_body)
        logger.info(f"put extracted s3://{load_path} to s3")
    except Exception as e:
        logger.error(f"failed to put extracted s3://{load_path} to s3")
        logger.error(e)
        return False
    return True


def main_crawler(keywords: list[str], start_datetime: str, end_datetime: str, data_for_load_s3: dict) -> tuple[
    int, int]:

    """
    게시물 URL을 키워드 및 지정된 시간 범위에 따라 추출하는 메인 크롤링 프로세스를 실행하며, 관련 데이터를 지정된 저장소에 로드한다.

    Args:
        keywords (list[str]): 게시물을 필터링할 키워드 목록.
        start_datetime (str): ISO 8601 형식("YYYY-MM-DDTHH:MM:SS")의 시작 날짜 및 시간.
        end_datetime (str): ISO 8601 형식("YYYY-MM-DDTHH:MM:SS")의 종료 날짜 및 시간.
        data_for_load_s3 (dict): 결과를 S3 버킷에 로드하는 데 필요한 구성 또는 데이터.

    Returns:
        tuple[int, int]: 처리된 총 URL 수와 성공적으로 추출된 URL 수를 포함하는 튜플.
    """

    start_datetime = datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M:%S")
    end_datetime = datetime.strptime(end_datetime, "%Y-%m-%dT%H:%M:%S")
    total_count = 0
    success_count = 0
    for keyword in keywords:
        urls, failed_page_list = get_list_of_post_url(start_datetime, end_datetime, list(range(0,50)), keyword)
        logger.info(f"Found {len(urls)} URLs. Failed page URLs: {failed_page_list}")
        if failed_page_list:
            logger.info(f"Try failed page URLs.")
            retried_urls, retried_failed_page_list = get_list_of_post_url(start_datetime, end_datetime, failed_page_list, keyword)
            urls.update(retried_urls)
        logger.info(f"Extracted {len(urls)} URLs.")
        total, success = load_each_post_with_keyword(urls, data_for_load_s3)
        total_count += total
        success_count += success
        logger.info(f"Extracted {total} URLs out of {success} URLs on keyword {keyword}")
    return total_count, success_count


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
    total_count = 0
    success_count = 0
    try:
        total_count, success_count = main_crawler(
            keywords=keywords,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            data_for_load_s3 = {
                "bucket":bucket,
                "car_id":car_id,
                "date":date,
                "batch_num":batch_num,
            }
        )
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
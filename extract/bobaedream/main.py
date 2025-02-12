from datetime import datetime, timedelta
import os
import json
from urllib.parse import urljoin
import boto3
from botocore.exceptions import ClientError
import requests
from bs4 import BeautifulSoup
import logging

def _get_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")

def _fetch_search_results(
    collection: str, keyword: str, page: int, start_date: str
) -> str:
    url = "https://www.bobaedream.co.kr/search"

    params = {
        "colle": collection,
        "searchField": "ALL",
        "page": page,
        "sort": "DATE",
        "startDate": start_date,
        "keyword": keyword,
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://www.bobaedream.co.kr",
        "Referer": "https://www.bobaedream.co.kr/search",
    }

    response = requests.post(url, data=params, headers=headers)
    response.raise_for_status()
    response.encoding = "utf-8"

    return response.text

def _parse_post_urls_per_page(soup: BeautifulSoup, date: str) -> list[tuple[str, str]]:
    posts = soup.select(".search_Community ul li")
    if not posts:
        return []
    post_urls = []
    for post in posts:
        post_title_elem = post.select_one("dt a")
        post_date_raw = post.select_one(".path span:last-child").text
        post_date = "20" + ''.join(post_date_raw.split()).replace(".", "-")
        if post_date > date:
            continue
        post_url = urljoin(
            "https://www.bobaedream.co.kr", post_title_elem.get("href", "")
        )
        post_id = int(post_url.split("No=")[1].split("&")[0])
        post_urls.append((post_id, post_url))
    return post_urls

def get_post_urls(keyword: str, date: str) -> list[tuple[str, str]]:
    post_urls = []
    for page in range(1, 10000):
        html = _fetch_search_results("community", keyword, page, date)
        soup = _get_soup(html)
        post_urls_per_page = _parse_post_urls_per_page(soup, date)
        if len(post_urls_per_page) == 0:
            break
        post_urls.extend(post_urls_per_page)
    return post_urls

def _fetch_post(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:135.0) Gecko/20100101 Firefox/135.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
        "Referer": "https://www.bobaedream.co.kr/search"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    response.encoding = "utf-8"
    return response.text

def _save_post_json(post_data: dict, post_id: str, car_id: str, date: str):
    os.makedirs(f"extracted/{car_id}/{date}/raw/bobaedream", exist_ok=True)
    with open(f"extracted/{car_id}/{date}/raw/bobaedream/{post_id}.json", "w", encoding="utf-8") as f:
        json.dump(post_data, f, ensure_ascii=False, indent=2)

def extract_at_local(car_id: str, keyword: str, date: str):
    logging.info(f"Extracting: {keyword}, {date}")
    post_urls = get_post_urls(keyword, date)
    logging.info(f"Total {len(post_urls)} posts found.")
    for post_id, post_url in post_urls:
        post_html = _fetch_post(post_url)
        post_data = {
            "url": post_url,
            "html": post_html
        }
        _save_post_json(post_data, post_id, car_id, date)
        logging.info(f"Extracted: {post_url}")

def _save_to_s3(content: dict, bucket: str, key: str):
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(content, ensure_ascii=False).encode('utf-8'),
            ContentType='application/json',
            ContentEncoding='utf-8'
        )
    except ClientError as e:
        logging.error(f"Error saving to S3: {e}")
        raise

def extract(car_id: str, keyword: str, date: str, bucket: str):
    logging.info(f"Extracting: {keyword}, {date}")
    post_urls = get_post_urls(keyword, date)
    logging.info(f"Total {len(post_urls)} posts found.")
    
    for post_id, post_url in post_urls:
        try:
            post_html = _fetch_post(post_url)
            
            post_data = {
                "url": post_url,
                "html": post_html
            }
            
            s3_key = f"{car_id}/{date}/raw/bobaedream/{post_id}.json"
            
            _save_to_s3(post_data, bucket, s3_key)
            
            logging.info(f"Saved to S3: {s3_key}")
            
        except Exception as e:
            logging.error(f"Error processing post {post_url}: {e}")
            continue

def lambda_handler(event, context):
    start_time = datetime.now()
    
    try:
        logging.basicConfig(level=logging.INFO)

        bucket = event.get("bucket")
        if not bucket:
            raise ValueError("Bucket name is required")
            
        car_id = event.get("car_id")
        keywords = event.get("keywords")
        date = event.get("date")
        
        if not all([car_id, keywords, date]):
            raise ValueError("car_id, keywords, and date are required")
        
        for keyword in keywords:
            extract(car_id, keyword, date, bucket)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        return {
            'statusCode': 200,
            'body': {
                'success': True,
                'end_time': end_time.isoformat(),
                'duration': str(duration),
                'car_id': car_id,
                'date': date
            }
        }
    except Exception as e:
        logging.error(f"Error in lambda_handler: {e}")
        end_time = datetime.now()
        duration = end_time - start_time
        
        return {
            'statusCode': 500,
            'body': {
                'success': False,
                'end_time': end_time.isoformat(),
                'duration': str(duration),
                'car_id': car_id if car_id else None,
                'date': date
            }
        }

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    car_id = "santafe"
    keywords = ["싼타페", "산타페"]
    date = "2025-02-10"
    
    for keyword in keywords:
        extract_at_local(car_id, keyword, date)